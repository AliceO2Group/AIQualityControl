#!/usr/bin/env python3
import os
import json
import time
import requests
from pathlib import Path
from collections import defaultdict
from dotenv import load_dotenv
from permissions.bkkp_api_personal_token import PERSONAL_TOKEN as TOKEN

load_dotenv()

OUT_DIR = os.getenv("OUT_DIR", "./bkkp_data")
TIMEOUT = int(os.getenv("TIMEOUT", "60"))

USE_POSTGRES = os.getenv("USE_POSTGRES", "true").lower() == "true"
PG_CONN_STR = os.getenv("PG_CONN_STR")

LIMIT = int(os.getenv("LIMIT", "10"))  # 0 = no limit

# false = normal full backup using lhcFills
# true  = incremental sync using runs?filter[updatedAt][from]=...
SYNC_MODE = os.getenv("SYNC_MODE", "false").lower() == "true"

try:
    import psycopg2
    from psycopg2.extras import Json
except ImportError:
    psycopg2 = None
    Json = None


BASE_DIR = Path(__file__).resolve().parent
CA_BUNDLE = BASE_DIR / "permissions" / "ali-bookkeeping.cern.ch.pem"

LHC_FILLS_URL = f"https://ali-bookkeeping.cern.ch/api/lhcFills?token={TOKEN}"


def get_headers():
    headers = {}
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    return headers


def now_ms():
    return int(time.time() * 1000)


def get_pg_conn():
    if not USE_POSTGRES:
        print("Postgres disabled via USE_POSTGRES=false")
        return None

    if psycopg2 is None:
        raise ImportError("psycopg2 is not installed but USE_POSTGRES=true")

    if not PG_CONN_STR:
        raise ValueError("PG_CONN_STR is not set")

    return psycopg2.connect(PG_CONN_STR)


def init_db(conn):
    if conn is None:
        return

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bookkeeping_lhc_fills (
                fill_number               BIGINT PRIMARY KEY,
                stable_beams_start        BIGINT,
                stable_beams_end          BIGINT,
                stable_beams_duration     BIGINT,
                beam_type                 TEXT,
                filling_scheme_name       TEXT,
                colliding_bunches_count   INTEGER,
                delivered_luminosity      NUMERIC,
                statistics_json           JSONB,
                metadata_json             JSONB NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS bookkeeping_runs (
                run_number                BIGINT PRIMARY KEY,
                id                        BIGINT,
                fill_number               BIGINT NOT NULL REFERENCES bookkeeping_lhc_fills(fill_number),
                time_o2_start             BIGINT,
                time_o2_end               BIGINT,
                time_trg_start            BIGINT,
                time_trg_end              BIGINT,
                start_time                BIGINT,
                end_time                  BIGINT,
                qc_time_start             BIGINT,
                qc_time_end               BIGINT,
                run_duration              BIGINT,
                environment_id            TEXT,
                updated_at                BIGINT,
                run_type                  INTEGER,
                definition                TEXT,
                calibration_status        TEXT,
                run_quality               TEXT,
                n_detectors               INTEGER,
                n_flps                    INTEGER,
                n_epns                    INTEGER,
                lhc_beam_energy           NUMERIC,
                lhc_beam_mode             TEXT,
                lhc_beta_star             NUMERIC,
                pdp_beam_type             TEXT,
                pdp_workflow_parameters   TEXT,
                trigger_value             TEXT,
                start_of_data_transfer    BIGINT,
                end_of_data_transfer      BIGINT,
                ctf_file_count            INTEGER,
                ctf_file_size             NUMERIC,
                tf_file_count             INTEGER,
                tf_file_size              NUMERIC,
                other_file_count          INTEGER,
                other_file_size           NUMERIC,
                cross_section             NUMERIC,
                trigger_efficiency        NUMERIC,
                trigger_acceptance        NUMERIC,
                eor_reasons_json          JSONB,
                detectors_qualities_json  JSONB,
                tags_json                 JSONB,
                qc_flags_json             JSONB,
                metadata_json             JSONB NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS bookkeeping_run_logs (
                log_id           BIGINT PRIMARY KEY,
                run_number       BIGINT NOT NULL REFERENCES bookkeeping_runs(run_number) ON DELETE CASCADE,
                title            TEXT,
                text             TEXT,
                author_name      TEXT,
                created_at       BIGINT,
                origin           TEXT,
                subtype          TEXT,
                root_log_id      BIGINT,
                parent_log_id    BIGINT,
                tags_json        JSONB,
                payload_json     JSONB NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_updates (
                sync_id                    BIGSERIAL PRIMARY KEY,
                sync_mode                  TEXT NOT NULL,
                started_at_ms              BIGINT NOT NULL,
                finished_at_ms             BIGINT,
                success                    BOOLEAN NOT NULL DEFAULT FALSE,
                source_updated_at_from     BIGINT,
                max_run_updated_at_seen    BIGINT,
                fills_seen                 INTEGER NOT NULL DEFAULT 0,
                runs_seen                  INTEGER NOT NULL DEFAULT 0,
                logs_seen                  INTEGER NOT NULL DEFAULT 0,
                fills_upserted             INTEGER NOT NULL DEFAULT 0,
                runs_inserted              INTEGER NOT NULL DEFAULT 0,
                runs_updated               INTEGER NOT NULL DEFAULT 0,
                logs_inserted              INTEGER NOT NULL DEFAULT 0,
                logs_updated               INTEGER NOT NULL DEFAULT 0,
                local_files_written        INTEGER NOT NULL DEFAULT 0,
                local_bytes_written        BIGINT NOT NULL DEFAULT 0,
                stats_json                 JSONB,
                error_text                 TEXT
            );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_lhc_fills_beam_type
            ON bookkeeping_lhc_fills (beam_type);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_lhc_fills_metadata_gin
            ON bookkeeping_lhc_fills USING GIN (metadata_json);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_runs_fill_number
            ON bookkeeping_runs (fill_number);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_runs_lhc_beam_mode
            ON bookkeeping_runs (lhc_beam_mode);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_runs_pdp_beam_type
            ON bookkeeping_runs (pdp_beam_type);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_runs_run_quality
            ON bookkeeping_runs (run_quality);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_runs_updated_at
            ON bookkeeping_runs (updated_at);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_runs_metadata_gin
            ON bookkeeping_runs USING GIN (metadata_json);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_runs_tags_gin
            ON bookkeeping_runs USING GIN (tags_json);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_runs_qc_flags_gin
            ON bookkeeping_runs USING GIN (qc_flags_json);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_run_logs_run_number
            ON bookkeeping_run_logs (run_number);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_run_logs_payload_gin
            ON bookkeeping_run_logs USING GIN (payload_json);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_sync_updates_success_started
            ON sync_updates (success, started_at_ms DESC);
        """)

    conn.commit()


def create_sync_update(conn, sync_mode, source_updated_at_from):
    if conn is None:
        return None

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO sync_updates (
                sync_mode,
                started_at_ms,
                source_updated_at_from,
                success
            )
            VALUES (%s, %s, %s, %s)
            RETURNING sync_id;
        """, (sync_mode, now_ms(), source_updated_at_from, False))
        sync_id = cur.fetchone()[0]

    conn.commit()
    return sync_id


def finalize_sync_update(
    conn,
    sync_id,
    success,
    stats=None,
    error_text=None,
):
    if conn is None or sync_id is None:
        return

    stats = stats or {}

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE sync_updates
            SET
                finished_at_ms = %s,
                success = %s,
                max_run_updated_at_seen = %s,
                fills_seen = %s,
                runs_seen = %s,
                logs_seen = %s,
                fills_upserted = %s,
                runs_inserted = %s,
                runs_updated = %s,
                logs_inserted = %s,
                logs_updated = %s,
                local_files_written = %s,
                local_bytes_written = %s,
                stats_json = %s,
                error_text = %s
            WHERE sync_id = %s;
        """, (
            now_ms(),
            success,
            stats.get("max_run_updated_at_seen"),
            stats.get("fills_seen", 0),
            stats.get("runs_seen", 0),
            stats.get("logs_seen", 0),
            stats.get("fills_upserted", 0),
            stats.get("runs_inserted", 0),
            stats.get("runs_updated", 0),
            stats.get("logs_inserted", 0),
            stats.get("logs_updated", 0),
            stats.get("local_files_written", 0),
            stats.get("local_bytes_written", 0),
            Json(stats),
            error_text,
            sync_id,
        ))

    conn.commit()


def get_last_successful_sync_updated_at(conn):
    if conn is None:
        return None

    with conn.cursor() as cur:
        cur.execute("""
            SELECT max_run_updated_at_seen
            FROM sync_updates
            WHERE success = TRUE
              AND max_run_updated_at_seen IS NOT NULL
            ORDER BY sync_id DESC
            LIMIT 1;
        """)
        row = cur.fetchone()

    if not row:
        return None
    return row[0]



def extract_fill_row(fill_obj):
    return (
        fill_obj.get("fillNumber"),
        fill_obj.get("stableBeamsStart"),
        fill_obj.get("stableBeamsEnd"),
        fill_obj.get("stableBeamsDuration"),
        fill_obj.get("beamType"),
        fill_obj.get("fillingSchemeName"),
        fill_obj.get("collidingBunchesCount"),
        fill_obj.get("deliveredLuminosity"),
        Json(fill_obj.get("statistics")),
        Json(fill_obj),
    )


def extract_run_row(run, parent_fill_number=None):
    fill_number = run.get("fillNumber") or parent_fill_number

    return (
        run.get("runNumber"),
        run.get("id"),
        fill_number,
        run.get("timeO2Start"),
        run.get("timeO2End"),
        run.get("timeTrgStart"),
        run.get("timeTrgEnd"),
        run.get("startTime"),
        run.get("endTime"),
        run.get("qcTimeStart"),
        run.get("qcTimeEnd"),
        run.get("runDuration"),
        run.get("environmentId"),
        run.get("updatedAt"),
        run.get("runType"),
        run.get("definition"),
        run.get("calibrationStatus"),
        run.get("runQuality"),
        run.get("nDetectors"),
        run.get("nFlps"),
        run.get("nEpns"),
        run.get("lhcBeamEnergy"),
        run.get("lhcBeamMode"),
        run.get("lhcBetaStar"),
        run.get("pdpBeamType"),
        run.get("pdpWorkflowParameters"),
        run.get("triggerValue"),
        run.get("startOfDataTransfer"),
        run.get("endOfDataTransfer"),
        run.get("ctfFileCount"),
        safe_numeric(run.get("ctfFileSize")),
        run.get("tfFileCount"),
        safe_numeric(run.get("tfFileSize")),
        run.get("otherFileCount"),
        safe_numeric(run.get("otherFileSize")),
        run.get("crossSection"),
        run.get("triggerEfficiency"),
        run.get("triggerAcceptance"),
        Json(run.get("eorReasons", [])),
        Json(run.get("detectorsQualities", [])),
        Json(run.get("tags", [])),
        Json(run.get("qcFlags", {})),
        Json(run),
    )


def extract_log_row(log, run_number):
    author = log.get("author") or {}
    return (
        log.get("id"),
        run_number,
        log.get("title"),
        log.get("text"),
        author.get("name"),
        log.get("createdAt"),
        log.get("origin"),
        log.get("subtype"),
        log.get("rootLogId"),
        log.get("parentLogId"),
        Json(log.get("tags", [])),
        Json(log),
    )


def safe_numeric(value):
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None


def save_json_local(filename, payload):
    Path(OUT_DIR).mkdir(parents=True, exist_ok=True)
    out_path = Path(OUT_DIR) / filename

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    size_bytes = out_path.stat().st_size
    print(f"Saved local JSON to {out_path}")
    return {
        "path": str(out_path),
        "bytes": size_bytes,
    }


# Insertion

def ensure_fill_exists_for_runs(conn, runs):
    """
    Minimal helper for sync mode:
    if a run references a fill_number not yet present in bookkeeping_lhc_fills,
    create a placeholder fill row so the FK on bookkeeping_runs(fill_number) does not fail.
    """
    if conn is None or not runs:
        return 0

    fill_numbers = sorted({
        run.get("fillNumber")
        for run in runs
        if run.get("fillNumber") is not None
    })

    if not fill_numbers:
        return 0

    upserted = 0
    with conn.cursor() as cur:
        for fill_number in fill_numbers:
            cur.execute("""
                INSERT INTO bookkeeping_lhc_fills (
                    fill_number,
                    stable_beams_start,
                    stable_beams_end,
                    stable_beams_duration,
                    beam_type,
                    filling_scheme_name,
                    colliding_bunches_count,
                    delivered_luminosity,
                    statistics_json,
                    metadata_json
                )
                VALUES (%s, NULL, NULL, NULL, NULL, NULL, NULL, NULL, %s, %s)
                ON CONFLICT (fill_number) DO NOTHING;
            """, (
                fill_number,
                Json(None),
                Json({"fillNumber": fill_number, "placeholder": True}),
            ))
            upserted += 1

    conn.commit()
    return upserted


def save_fills_batch(conn, fills):
    if conn is None or not fills:
        return 0

    count = 0
    with conn.cursor() as cur:
        for fill_obj in fills:
            cur.execute("""
                INSERT INTO bookkeeping_lhc_fills (
                    fill_number,
                    stable_beams_start,
                    stable_beams_end,
                    stable_beams_duration,
                    beam_type,
                    filling_scheme_name,
                    colliding_bunches_count,
                    delivered_luminosity,
                    statistics_json,
                    metadata_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (fill_number) DO UPDATE SET
                    stable_beams_start = EXCLUDED.stable_beams_start,
                    stable_beams_end = EXCLUDED.stable_beams_end,
                    stable_beams_duration = EXCLUDED.stable_beams_duration,
                    beam_type = EXCLUDED.beam_type,
                    filling_scheme_name = EXCLUDED.filling_scheme_name,
                    colliding_bunches_count = EXCLUDED.colliding_bunches_count,
                    delivered_luminosity = EXCLUDED.delivered_luminosity,
                    statistics_json = EXCLUDED.statistics_json,
                    metadata_json = EXCLUDED.metadata_json;
            """, extract_fill_row(fill_obj))
            count += 1

    conn.commit()
    print(f"Saved {count} fills to Postgres")
    return count


def save_runs_batch(conn, fills=None, runs=None):
    if conn is None:
        return {"inserted": 0, "updated": 0, "total": 0}

    flat_runs = []
    if runs is not None:
        flat_runs = runs
    elif fills is not None:
        for fill_obj in fills:
            fill_number = fill_obj.get("fillNumber")
            nested_runs = fill_obj.get("runs", []) or []
            for run in nested_runs:
                if run.get("fillNumber") is None and fill_number is not None:
                    run["fillNumber"] = fill_number
                flat_runs.append(run)

    inserted = 0
    updated = 0

    with conn.cursor() as cur:
        for run in flat_runs:
            run_number = run.get("runNumber")
            fill_number = run.get("fillNumber")
            if not run_number or fill_number is None:
                continue

            cur.execute("""
                SELECT 1
                FROM bookkeeping_runs
                WHERE run_number = %s;
            """, (run_number,))
            exists = cur.fetchone() is not None

            cur.execute("""
                INSERT INTO bookkeeping_runs (
                    run_number,
                    id,
                    fill_number,
                    time_o2_start,
                    time_o2_end,
                    time_trg_start,
                    time_trg_end,
                    start_time,
                    end_time,
                    qc_time_start,
                    qc_time_end,
                    run_duration,
                    environment_id,
                    updated_at,
                    run_type,
                    definition,
                    calibration_status,
                    run_quality,
                    n_detectors,
                    n_flps,
                    n_epns,
                    lhc_beam_energy,
                    lhc_beam_mode,
                    lhc_beta_star,
                    pdp_beam_type,
                    pdp_workflow_parameters,
                    trigger_value,
                    start_of_data_transfer,
                    end_of_data_transfer,
                    ctf_file_count,
                    ctf_file_size,
                    tf_file_count,
                    tf_file_size,
                    other_file_count,
                    other_file_size,
                    cross_section,
                    trigger_efficiency,
                    trigger_acceptance,
                    eor_reasons_json,
                    detectors_qualities_json,
                    tags_json,
                    qc_flags_json,
                    metadata_json
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (run_number) DO UPDATE SET
                    id = EXCLUDED.id,
                    fill_number = EXCLUDED.fill_number,
                    time_o2_start = EXCLUDED.time_o2_start,
                    time_o2_end = EXCLUDED.time_o2_end,
                    time_trg_start = EXCLUDED.time_trg_start,
                    time_trg_end = EXCLUDED.time_trg_end,
                    start_time = EXCLUDED.start_time,
                    end_time = EXCLUDED.end_time,
                    qc_time_start = EXCLUDED.qc_time_start,
                    qc_time_end = EXCLUDED.qc_time_end,
                    run_duration = EXCLUDED.run_duration,
                    environment_id = EXCLUDED.environment_id,
                    updated_at = EXCLUDED.updated_at,
                    run_type = EXCLUDED.run_type,
                    definition = EXCLUDED.definition,
                    calibration_status = EXCLUDED.calibration_status,
                    run_quality = EXCLUDED.run_quality,
                    n_detectors = EXCLUDED.n_detectors,
                    n_flps = EXCLUDED.n_flps,
                    n_epns = EXCLUDED.n_epns,
                    lhc_beam_energy = EXCLUDED.lhc_beam_energy,
                    lhc_beam_mode = EXCLUDED.lhc_beam_mode,
                    lhc_beta_star = EXCLUDED.lhc_beta_star,
                    pdp_beam_type = EXCLUDED.pdp_beam_type,
                    pdp_workflow_parameters = EXCLUDED.pdp_workflow_parameters,
                    trigger_value = EXCLUDED.trigger_value,
                    start_of_data_transfer = EXCLUDED.start_of_data_transfer,
                    end_of_data_transfer = EXCLUDED.end_of_data_transfer,
                    ctf_file_count = EXCLUDED.ctf_file_count,
                    ctf_file_size = EXCLUDED.ctf_file_size,
                    tf_file_count = EXCLUDED.tf_file_count,
                    tf_file_size = EXCLUDED.tf_file_size,
                    other_file_count = EXCLUDED.other_file_count,
                    other_file_size = EXCLUDED.other_file_size,
                    cross_section = EXCLUDED.cross_section,
                    trigger_efficiency = EXCLUDED.trigger_efficiency,
                    trigger_acceptance = EXCLUDED.trigger_acceptance,
                    eor_reasons_json = EXCLUDED.eor_reasons_json,
                    detectors_qualities_json = EXCLUDED.detectors_qualities_json,
                    tags_json = EXCLUDED.tags_json,
                    qc_flags_json = EXCLUDED.qc_flags_json,
                    metadata_json = EXCLUDED.metadata_json;
            """, extract_run_row(run, parent_fill_number=fill_number))

            if exists:
                updated += 1
            else:
                inserted += 1

    conn.commit()
    total = inserted + updated
    print(f"Saved {total} runs to Postgres ({inserted} inserted, {updated} updated)")
    return {"inserted": inserted, "updated": updated, "total": total}


def save_logs_batch(conn, all_logs):
    if conn is None or not all_logs:
        return {"inserted": 0, "updated": 0, "total": 0}

    inserted = 0
    updated = 0

    with conn.cursor() as cur:
        for run_number, logs in all_logs.items():
            for log in logs:
                log_id = log.get("id")
                if not log_id:
                    continue

                cur.execute("""
                    SELECT 1
                    FROM bookkeeping_run_logs
                    WHERE log_id = %s;
                """, (log_id,))
                exists = cur.fetchone() is not None

                cur.execute("""
                    INSERT INTO bookkeeping_run_logs (
                        log_id,
                        run_number,
                        title,
                        text,
                        author_name,
                        created_at,
                        origin,
                        subtype,
                        root_log_id,
                        parent_log_id,
                        tags_json,
                        payload_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (log_id) DO UPDATE SET
                        run_number = EXCLUDED.run_number,
                        title = EXCLUDED.title,
                        text = EXCLUDED.text,
                        author_name = EXCLUDED.author_name,
                        created_at = EXCLUDED.created_at,
                        origin = EXCLUDED.origin,
                        subtype = EXCLUDED.subtype,
                        root_log_id = EXCLUDED.root_log_id,
                        parent_log_id = EXCLUDED.parent_log_id,
                        tags_json = EXCLUDED.tags_json,
                        payload_json = EXCLUDED.payload_json;
                """, extract_log_row(log, run_number))

                if exists:
                    updated += 1
                else:
                    inserted += 1

    conn.commit()
    total = inserted + updated
    print(f"Saved {total} logs to Postgres ({inserted} inserted, {updated} updated)")
    return {"inserted": inserted, "updated": updated, "total": total}



def fetch_lhc_fills():
    print("Fetching LHC fills from API...")

    response = requests.get(
        LHC_FILLS_URL,
        headers=get_headers(),
        verify=str(CA_BUNDLE),
        timeout=TIMEOUT,
    )
    response.raise_for_status()

    payload = response.json()
    fills = payload.get("data", [])

    if LIMIT > 0:
        fills = fills[:LIMIT]

    print(f"Fetched {len(fills)} fills")
    return fills


def fetch_runs_updated_since(updated_at_from):
    url = f"https://ali-bookkeeping.cern.ch/api/runs?filter[updatedAt][from]={updated_at_from}&token={TOKEN}"

    print(f"Fetching updated runs from API since updatedAt={updated_at_from}...")
    response = requests.get(
        url,
        headers=get_headers(),
        verify=str(CA_BUNDLE),
        timeout=TIMEOUT,
    )
    response.raise_for_status()

    payload = response.json()
    runs = payload.get("data", [])

    if LIMIT > 0:
        runs = runs[:LIMIT]

    print(f"Fetched {len(runs)} updated runs")
    return runs


def fetch_run_logs(run_number):
    url = f"https://ali-bookkeeping.cern.ch/api/runs/{run_number}/logs?token={TOKEN}"

    response = requests.get(
        url,
        headers=get_headers(),
        verify=str(CA_BUNDLE),
        timeout=TIMEOUT,
    )
    response.raise_for_status()

    payload = response.json()
    return payload.get("data", [])


# ---------------- transform helpers ----------------

def group_runs_as_fill_like_objects(runs):

    grouped = defaultdict(list)

    for run in runs:
        fill_number = run.get("fillNumber")
        if fill_number is None:
            continue
        grouped[fill_number].append(run)

    fill_like_objects = []
    for fill_number, grouped_runs in grouped.items():
        fill_like_objects.append({
            "fillNumber": fill_number,
            "runs": grouped_runs,
        })

    return fill_like_objects



def main():
    conn = None
    sync_id = None
    sync_stats = {
        "fills_seen": 0,
        "runs_seen": 0,
        "logs_seen": 0,
        "fills_upserted": 0,
        "runs_inserted": 0,
        "runs_updated": 0,
        "logs_inserted": 0,
        "logs_updated": 0,
        "local_files_written": 0,
        "local_bytes_written": 0,
        "max_run_updated_at_seen": None,
    }

    try:
        conn = get_pg_conn()
        if conn:
            init_db(conn)

        source_updated_at_from = None

        if SYNC_MODE:
            source_updated_at_from = get_last_successful_sync_updated_at(conn)
            if source_updated_at_from is None:
                print("No previous successful sync found, falling back to full fetch.")
        sync_mode_label = "sync" if (SYNC_MODE and source_updated_at_from is not None) else "full"

        if conn:
            sync_id = create_sync_update(conn, sync_mode_label, source_updated_at_from)

        all_logs = {}

        # -------- sync mode --------
        if SYNC_MODE and source_updated_at_from is not None:
            runs = fetch_runs_updated_since(source_updated_at_from)
            sync_stats["runs_seen"] = len(runs)

            if runs:
                sync_stats["max_run_updated_at_seen"] = max(
                    (run.get("updatedAt") for run in runs if run.get("updatedAt") is not None),
                    default=source_updated_at_from
                )
            else:
                sync_stats["max_run_updated_at_seen"] = source_updated_at_from

            for run in runs:
                run_number = run.get("runNumber")
                if not run_number:
                    continue

                logs = fetch_run_logs(run_number)
                all_logs[run_number] = logs
                run["logs"] = logs
                sync_stats["logs_seen"] += len(logs)

            fill_like_objects = group_runs_as_fill_like_objects(runs)
            sync_stats["fills_seen"] = len(fill_like_objects)

            local_info = save_json_local("runs_sync.json", runs)
            sync_stats["local_files_written"] += 1
            sync_stats["local_bytes_written"] += local_info["bytes"]

            # make sure referenced fills exist before saving runs
            fills_upserted = ensure_fill_exists_for_runs(conn, runs)
            sync_stats["fills_upserted"] += fills_upserted

            run_result = save_runs_batch(conn, runs=runs)
            sync_stats["runs_inserted"] += run_result["inserted"]
            sync_stats["runs_updated"] += run_result["updated"]

            log_result = save_logs_batch(conn, all_logs)
            sync_stats["logs_inserted"] += log_result["inserted"]
            sync_stats["logs_updated"] += log_result["updated"]

            print(f"Sync mode complete: {len(runs)} runs processed")

        # -------- full mode --------
        else:
            fills = fetch_lhc_fills()
            sync_stats["fills_seen"] = len(fills)

            total_runs = 0
            max_updated_at = None

            for fill_obj in fills:
                runs = fill_obj.get("runs", []) or []
                total_runs += len(runs)

                for run in runs:
                    updated_at = run.get("updatedAt")
                    if updated_at is not None:
                        if max_updated_at is None or updated_at > max_updated_at:
                            max_updated_at = updated_at

                    run_number = run.get("runNumber")
                    if not run_number:
                        continue

                    logs = fetch_run_logs(run_number)
                    all_logs[run_number] = logs
                    run["logs"] = logs
                    sync_stats["logs_seen"] += len(logs)

            sync_stats["runs_seen"] = total_runs
            sync_stats["max_run_updated_at_seen"] = max_updated_at

            print(f"Total nested runs found: {total_runs}")

            local_info = save_json_local("lhc_fills.json", fills)
            sync_stats["local_files_written"] += 1
            sync_stats["local_bytes_written"] += local_info["bytes"]

            fills_saved = save_fills_batch(conn, fills)
            sync_stats["fills_upserted"] += fills_saved

            run_result = save_runs_batch(conn, fills=fills)
            sync_stats["runs_inserted"] += run_result["inserted"]
            sync_stats["runs_updated"] += run_result["updated"]

            log_result = save_logs_batch(conn, all_logs)
            sync_stats["logs_inserted"] += log_result["inserted"]
            sync_stats["logs_updated"] += log_result["updated"]

        finalize_sync_update(
            conn,
            sync_id,
            success=True,
            stats=sync_stats,
            error_text=None,
        )

    except Exception as e:
        finalize_sync_update(
            conn,
            sync_id,
            success=False,
            stats=sync_stats,
            error_text=str(e),
        )
        raise

    finally:
        if conn is not None:
            conn.close()
            print("Closed Postgres connection")


if __name__ == "__main__":
    main()