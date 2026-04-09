#!/usr/bin/env python3
import os
import json
import requests
from pathlib import Path
from dotenv import load_dotenv
from permissions.bkkp_api_personal_token import PERSONAL_TOKEN as TOKEN

load_dotenv()

OUT_DIR = os.getenv("OUT_DIR", "./bkkp_data")
TIMEOUT = int(os.getenv("TIMEOUT", "60"))

USE_POSTGRES = os.getenv("USE_POSTGRES", "true").lower() == "true"
PG_CONN_STR = os.getenv("PG_CONN_STR")

LIMIT = int(os.getenv("LIMIT", "1"))  # 0 = no limit

try:
    import psycopg2
    from psycopg2.extras import Json
except ImportError as e: 
    print(e)
    
    
BASE_DIR = Path(__file__).resolve().parent
CA_BUNDLE = BASE_DIR / "permissions" / "ali-bookkeeping.cern.ch.pem"
LHC_FILLS_URL = f"https://ali-bookkeeping.cern.ch/api/lhcFills" # API endpoint for fills 
RUN_LOGS_URL = f"https://ali-bookkeeping.cern.ch/api/runs/" # API endpoint for runs 


# Database connection layer 
def get_pg_conn():
    if not USE_POSTGRES:
        print("Postgres disabled via USE_POSTGRES=false")
        return None

    if psycopg2 is None:
        raise ImportError("psycopg2 is not installed but USE_POSTGRES=true")

    if not PG_CONN_STR:
        raise ValueError("PG_CONN_STR is not set")

    return psycopg2.connect(PG_CONN_STR)


### LOAD of ETL ### 

# HTTP Requests to API Endpoints 
def get_headers():
    headers = {}
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    return headers

# GET HTTP request to 'lhcFills' endpoint. Response is converted to json only if it doesn't return a 4xx or 5xx status. 
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

# fetch fills, extract nested runs, for each run fetch logs seperately 
def fetch_run_logs(run_number):
    url = f"{RUN_LOGS_URL}{run_number}/logs"

    response = requests.get(
        url,
        headers=get_headers(),
        verify=str(CA_BUNDLE),
        timeout=TIMEOUT,
    )
    response.raise_for_status()

    payload = response.json()
    return payload.get("data", [])


# EXTRACT of ETL 

# Flatten a nested API object into SQL columns.

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


# ---------------- local save ----------------
# Even if DB insert fails, still have a local copy of the fetched data.

def save_json_local(filename, payload):
    Path(OUT_DIR).mkdir(parents=True, exist_ok=True)
    out_path = Path(OUT_DIR) / filename

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Saved local JSON to {out_path}")


# LOAD of ETL 
# Inserts fills into PostgreSQL.

def save_fills_batch(conn, fills):
    if conn is None or not fills:
        return

    # Upsert
    # If row doesn't exist, insert it. If row already exists, update it 
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

    conn.commit()
    print(f"Saved {len(fills)} fills to Postgres")


def save_runs_batch(conn, fills):
    if conn is None:
        return

    inserted = 0
    with conn.cursor() as cur:
        for fill_obj in fills:
            fill_number = fill_obj.get("fillNumber")
            runs = fill_obj.get("runs", []) or []

            for run in runs:
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
                inserted += 1

    conn.commit()
    print(f"Saved {inserted} runs to Postgres")


def save_logs_batch(conn, all_logs):
    if conn is None or not all_logs:
        return

    inserted = 0
    with conn.cursor() as cur:
        for run_number, logs in all_logs.items():
            for log in logs:
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
                inserted += 1

    conn.commit()
    print(f"Saved {inserted} logs to Postgres")




# ---------------- main ----------------

def main():
    conn = None
    try:
        conn = get_pg_conn()

        fills = fetch_lhc_fills()

        all_logs = {}
        total_runs = 0

        for fill_obj in fills:
            runs = fill_obj.get("runs", []) or []
            total_runs += len(runs)

            for run in runs:
                run_number = run.get("runNumber")
                if not run_number:
                    continue

                logs = fetch_run_logs(run_number)
                all_logs[run_number] = logs
                run["logs"] = logs

        print(f"Total nested runs found: {total_runs}")

        save_json_local("lhc_fills.json", fills)
        save_fills_batch(conn, fills)
        save_runs_batch(conn, fills)
        save_logs_batch(conn, all_logs)

    finally:
        if conn is not None:
            conn.close()
            print("Closed Postgres connection")


if __name__ == "__main__":
    main()