#!/usr/bin/env python3
import datetime
import json
import logging
import os
import re
from json import JSONDecodeError
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote

import requests
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

try:
    import psycopg2
    from psycopg2.extras import Json
except ImportError:
    psycopg2 = None
    Json = None


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class ObjectVersion:
    def __init__(
        self,
        path: str,
        valid_from: int,
        valid_to: int,
        created_at: int,
        uuid: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ):
        self.path = path
        self.uuid = uuid
        self.valid_from = int(valid_from)
        self.valid_to = int(valid_to)
        self.created_at = int(created_at)
        self.metadata = metadata or {}
        self.valid_from_as_dt = datetime.datetime.fromtimestamp(self.valid_from / 1000)
        self.created_at_as_dt = datetime.datetime.fromtimestamp(self.created_at / 1000)

    def __repr__(self):
        run_number = self.metadata.get("Run") or self.metadata.get("RunNumber")
        return (
            f"ObjectVersion(path={self.path!r}, uuid={self.uuid!r}, "
            f"created_at={self.created_at_as_dt}, valid_from={self.valid_from_as_dt}, "
            f"run={run_number})"
        )


class Ccdb:
    def __init__(self, url: str, timeout: int = 60):
        self.url = url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def get_objects_list(
        self,
        added_since: Optional[int] = None,
        path: str = "",
        no_wildcard: bool = False,
    ) -> List[str]:
        url = f"{self.url}/latest/{path}"
        if path:
            url += "/"
        url += "" if no_wildcard else ".*"

        headers = {"Accept": "application/json"}
        if added_since is not None:
            headers["If-Not-Before"] = str(added_since)

        logger.info("Listing recent objects from %s", url)
        r = self.session.get(url, headers=headers, timeout=self.timeout)
        r.raise_for_status()

        try:
            payload = r.json()
        except JSONDecodeError as err:
            logger.error("JSON decode error in get_objects_list: %s", err)
            raise

        return [item["path"] for item in payload.get("objects", [])]

    def get_versions_list(
        self,
        object_path: str,
        from_ts: str = "",
        to_ts: str = "",
        run: int = -1,
        metadata: str = "",
    ) -> List[ObjectVersion]:
        url = f"{self.url}/browse/{quote(object_path, safe='/')}"
        if run != -1:
            url += f"/RunNumber={run}"
        if metadata:
            url += metadata

        headers = {"Accept": "application/json", "Connection": "close"}
        if from_ts != "":
            headers["If-Not-Before"] = str(from_ts)
        if to_ts != "":
            headers["If-Not-After"] = str(to_ts)

        logger.info("Listing versions for %s", object_path)
        r = self.session.get(url, headers=headers, timeout=self.timeout)
        r.raise_for_status()

        try:
            payload = r.json()
        except ValueError as err:
            raise RuntimeError(f"Error reading JSON for object {object_path}: {err}") from err

        versions = [
            ObjectVersion(
                path=obj["path"],
                uuid=obj.get("id"),
                valid_from=obj["validFrom"],
                valid_to=obj["validUntil"],
                created_at=obj["Created"],
                metadata=obj,
            )
            for obj in payload.get("objects", [])
        ]
        versions.sort(key=lambda v: v.created_at)
        return versions

    def load_existing_etags(self, conn) -> set:
        if conn is None:
            return set()
        with conn.cursor() as cur:
            cur.execute("SELECT etag FROM qcdb_objects;")
            return {row[0] for row in cur.fetchall()}

    def download_version(self, version: ObjectVersion) -> requests.Response:
        etag = version.metadata.get("ETag")
        if etag:
            etag = str(etag).strip('"')
            url = f"{self.url}/download/{quote(etag)}"
        elif version.uuid:
            raise RuntimeError(
                f"No ETag found for version {version.path} ({version.uuid}); "
                "download endpoint expects ETag."
            )
        else:
            raise RuntimeError(f"Cannot download {version.path}: missing both ETag and uuid")

        r = self.session.get(url, stream=True, timeout=self.timeout)
        r.raise_for_status()
        return r


def save_response_to_file(resp: requests.Response, outdir: str, fallback_name: str = "download.bin") -> str:
    os.makedirs(outdir, exist_ok=True)
    cd = resp.headers.get("Content-Disposition", "")
    m = re.search(r'filename="([^"]+)"', cd)
    filename = m.group(1) if m else fallback_name
    dst = os.path.join(outdir, filename)

    with open(dst, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    return dst


def save_json_to_file_flat(data, outdir: str, ccdb_path: str):
    fpath = (Path(outdir) / ccdb_path).with_suffix(".json")
    fpath.parent.mkdir(parents=True, exist_ok=True)
    with fpath.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def get_pg_conn(use_postgres: bool, pg_conn_str: Optional[str]):
    if not use_postgres or psycopg2 is None:
        return None
    return psycopg2.connect(pg_conn_str)


def init_db(conn):
    if conn is None:
        return

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS qcdb_objects (
                id SERIAL PRIMARY KEY,
                qc_path TEXT NOT NULL,
                file_name TEXT,
                etag TEXT UNIQUE NOT NULL,
                created_at BIGINT,
                downloaded_at TIMESTAMP DEFAULT NOW(),
                metadata_json JSONB NOT NULL
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS qcdb_sync_runs (
                id SERIAL PRIMARY KEY,
                qc_prefix TEXT NOT NULL,
                since_ms BIGINT,
                started_at TIMESTAMP NOT NULL DEFAULT NOW(),
                finished_at TIMESTAMP NOT NULL DEFAULT NOW(),
                downloaded INTEGER NOT NULL DEFAULT 0,
                skipped_existing INTEGER NOT NULL DEFAULT 0,
                failed INTEGER NOT NULL DEFAULT 0,
                details_json JSONB NOT NULL DEFAULT '{}'::jsonb
            );
        """)
    conn.commit()


def save_batch_to_postgres(conn, rows):
    if conn is None or not rows:
        return

    with conn.cursor() as cur:
        for qc_path, obj in rows:
            etag = str(obj.get("ETag", "")).strip('"')
            if not etag:
                continue
            cur.execute("""
                INSERT INTO qcdb_objects (qc_path, file_name, etag, created_at, metadata_json)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (etag) DO NOTHING;
            """, (
                qc_path,
                obj.get("fileName"),
                etag,
                obj.get("Created") or obj.get("created"),
                Json(obj),
            ))
    conn.commit()


def save_sync_run(conn, qc_prefix, since_ms, started_at, finished_at, downloaded, skipped_existing, failed, details):
    if conn is None:
        return

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO qcdb_sync_runs (
                qc_prefix, since_ms, started_at, finished_at,
                downloaded, skipped_existing, failed, details_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            qc_prefix,
            since_ms,
            started_at,
            finished_at,
            downloaded,
            skipped_existing,
            failed,
            Json(details),
        ))
    conn.commit()


def ms_since_hours_ago(hours: int) -> int:
    dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=hours)
    local_dt = dt.astimezone()
    logger.info(
        "Fetching data for the last %d hour(s), from %s (UTC) or %s (Geneva Local Time)",
        hours,
        dt.strftime("%Y-%m-%d %H:%M:%S %Z"),
        local_dt.strftime("%Y-%m-%d %H:%M:%S %Z"),
    )
    return int(dt.timestamp() * 1000)


def download_objects(
    ccdb: Ccdb,
    qc_prefix: str,
    out_dir: str,
    since_ms: Optional[int] = None,
    conn=None,
    limit_objects: Optional[int] = None,
    limit_versions: Optional[int] = None,
):
    logger.info("")
    logger.info("Processing prefix: %s", qc_prefix)

    started_at = datetime.datetime.now()

    object_paths = ccdb.get_objects_list(
        added_since=since_ms,
        path=qc_prefix,
        no_wildcard=True,
    )
    if limit_objects:
        object_paths = object_paths[:limit_objects]

    existing_etags = ccdb.load_existing_etags(conn)

    total_downloaded = 0
    total_skipped = 0
    total_failed = 0
    details = {"downloaded": [], "skipped": [], "failed": []}

    for object_path in object_paths:
        try:
            versions = ccdb.get_versions_list(
                object_path=object_path,
                from_ts=str(since_ms) if since_ms is not None else "",
            )

            logger.info("Found %d versions under %s", len(versions), object_path)

            if limit_versions:
                versions = versions[:limit_versions]
            if not versions:
                continue

            local_metadata = []
            batch = []

            for version in tqdm(versions, desc=object_path, leave=False):
                etag = str(version.metadata.get("ETag", "")).strip('"')
                file_name = version.metadata.get("fileName")

                try:
                    if etag and etag in existing_etags:
                        total_skipped += 1
                        details["skipped"].append({
                            "qc_path": object_path,
                            "etag": etag,
                            "file_name": file_name,
                        })
                        continue

                    resp = ccdb.download_version(version)
                    fallback_name = file_name or f"{version.uuid or 'version'}_{version.valid_from}.bin"

                    local_metadata.append(version.metadata)
                    batch.append((object_path, version.metadata))

                    total_downloaded += 1
                    if etag:
                        existing_etags.add(etag)

                    details["downloaded"].append({
                        "qc_path": object_path,
                        "etag": etag,
                        "file_name": file_name,
                    })

                except Exception as err:
                    total_failed += 1
                    logger.error("Failed downloading version %s: %s", version, err)
                    details["failed"].append({
                        "qc_path": object_path,
                        "etag": etag,
                        "file_name": file_name,
                        "error": str(err),
                    })

            if local_metadata:
                save_json_to_file_flat(local_metadata, out_dir, object_path)
                save_batch_to_postgres(conn, batch)

        except Exception as err:
            total_failed += 1
            logger.error("Failed processing object path %s: %s", object_path, err)
            details["failed"].append({
                "qc_path": object_path,
                "error": str(err),
            })

    finished_at = datetime.datetime.now()

    save_sync_run(
        conn=conn,
        qc_prefix=qc_prefix,
        since_ms=since_ms,
        started_at=started_at,
        finished_at=finished_at,
        downloaded=total_downloaded,
        skipped_existing=total_skipped,
        failed=total_failed,
        details=details,
    )

    logger.info(
        "Finished: downloaded=%d skipped_existing=%d failed=%d",
        total_downloaded,
        total_skipped,
        total_failed,
    )


if __name__ == "__main__":
    with open("config.json") as f:
        config = json.load(f)

    load_dotenv()
    TIMEOUT = int(os.getenv("TIMEOUT", "60"))
    USE_POSTGRES = os.getenv("USE_POSTGRES", "true").lower() == "true"
    PG_CONN_STR = os.getenv("PG_CONN_STR")
    LIMIT = int(os.getenv("LIMIT", "10"))
    OUT_DIR = str(os.getenv("OUT_DIR"))

    BASE = "http://ali-qcdb-gpn.cern.ch:8083"
    QC_PREFIXES = config.get("qc_prefixes", [])
    FULL_BACKUP = bool(config.get("full_backup", False))
    HOURS_BACK = int(config.get("hours_back", 24))
    LIMIT_OBJECTS = config.get("limit_objects")
    LIMIT_VERSIONS = config.get("limit_versions")

    since_ms = None if FULL_BACKUP else ms_since_hours_ago(HOURS_BACK)

    ccdb = Ccdb(BASE, timeout=TIMEOUT)

    conn = get_pg_conn(USE_POSTGRES, PG_CONN_STR)
    if conn:
        init_db(conn)

    try:
        for qc_prefix in QC_PREFIXES:
            download_objects(
                ccdb=ccdb,
                qc_prefix=qc_prefix,
                out_dir=OUT_DIR,
                since_ms=since_ms,
                conn=conn,
                limit_objects=LIMIT_OBJECTS,
                limit_versions=LIMIT_VERSIONS,
            )
    finally:
        if conn:
            conn.close()
            
