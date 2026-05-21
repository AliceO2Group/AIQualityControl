import datetime
import os
import re
from typing import Dict, List, Optional
from urllib.parse import quote

import requests


class ObjectVersion:
    def __init__(self, path: str, valid_from: int, valid_to: int, created_at: int,
                 uuid: Optional[str] = None, metadata: Optional[Dict] = None):
        self.path = path
        self.uuid = uuid
        self.valid_from = int(valid_from)
        self.valid_to = int(valid_to)
        self.created_at = int(created_at)
        self.metadata = metadata or {}
        self.valid_from_as_dt = datetime.datetime.fromtimestamp(self.valid_from / 1000)
        self.created_at_as_dt = datetime.datetime.fromtimestamp(self.created_at / 1000)

    def __repr__(self):
        run = self.metadata.get("Run") or self.metadata.get("RunNumber")
        return (f"ObjectVersion(path={self.path!r}, created_at={self.created_at_as_dt}, "
                f"valid_from={self.valid_from_as_dt}, run={run})")


class Ccdb:
    def __init__(self, url: str, timeout: int = 60):
        self.url = url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def get_versions_list(self, object_path: str, from_ts: str = "", to_ts: str = "") -> List[ObjectVersion]:
        url = f"{self.url}/browse/{quote(object_path, safe='/')}"
        headers = {"Accept": "application/json", "Connection": "close"}
        if from_ts:
            headers["If-Not-Before"] = str(from_ts)
        if to_ts:
            headers["If-Not-After"] = str(to_ts)

        r = self.session.get(url, headers=headers, timeout=self.timeout)
        r.raise_for_status()

        versions = [
            ObjectVersion(
                path=obj["path"],
                uuid=obj.get("id"),
                valid_from=obj["validFrom"],
                valid_to=obj["validUntil"],
                created_at=obj["Created"],
                metadata=obj,
                
            )
            for obj in r.json().get("objects", [])
        ]
        versions.sort(key=lambda v: v.created_at)
        return versions

    def download_version(self, version: ObjectVersion) -> requests.Response:
        etag = version.metadata.get("ETag")
        if not etag:
            raise RuntimeError(f"Cannot download {version.path}: missing ETag")
        url = f"{self.url}/download/{quote(str(etag).strip(chr(34)))}"
        r = self.session.get(url, stream=True, timeout=self.timeout)
        r.raise_for_status()
        return r


def save_response_to_file(resp: requests.Response, outdir: str, fallback_name: str = "download.bin") -> str:
    os.makedirs(outdir, exist_ok=True)
    m = re.search(r'filename="([^"]+)"', resp.headers.get("Content-Disposition", ""))
    filename = m.group(1) if m else fallback_name
    dst = os.path.join(outdir, filename)
    with open(dst, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    return dst
