import datetime
import os
from pathlib import Path

import requests

try:
    import ROOT
except Exception as exc:
    raise SystemExit(f"PyROOT import failed: {exc}")

COARSE_SEVERITY = {"Bad": 2, "Medium": 1, "Good": 0}


def combine_predictions(predictions: list) -> dict:
    """Return the worst-case prediction across all pads (Bad > Medium > Good)."""
    return max(predictions, key=lambda p: COARSE_SEVERITY[p["coarse_name"]])


def write_prediction_root_file(prediction: dict, out_path: str) -> str:
    """Write a ROOT file with a TNamed ccdb_object holding the prediction string."""
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    label = f"{prediction['coarse_name']} ({prediction['fine_name']})"
    ROOT.gROOT.SetBatch(True)
    f = ROOT.TFile(out_path, "RECREATE")
    ROOT.TNamed("ccdb_object", label).Write()
    f.Close()
    print(f"  Written: {out_path}  [{label}]")
    return out_path


def upload_to_qcdb(ccdb_url: str, publish_path: str, root_file: str, valid_from_ms: int, valid_until_ms: int, filename: str = None):
    """Upload ROOT file to QCDB via HTTP POST multipart."""
    url = f"{ccdb_url.rstrip('/')}/{publish_path.strip('/')}/{valid_from_ms}/{valid_until_ms}"
    upload_name = filename or os.path.basename(root_file)
    with open(root_file, "rb") as f:
        resp = requests.post(url, files={"send": (upload_name, f, "application/root")}, timeout=30)#, verify="/Users/zetasourpi/.globus/usercert.pem")
    resp.raise_for_status()
    print(f"  Uploaded to QCDB: {url}  (HTTP {resp.status_code})")
    return resp


def publish_predictions(predictions: list, cfg: dict, source_filename: str = None) -> str:
    """Aggregate pad predictions, write ROOT file, upload to QCDB. Returns prediction string."""
    combined = combine_predictions(predictions)
    root_file = write_prediction_root_file(combined, cfg.get("publish_root_file", "ML_prediction.root"))
    now_ms = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
    publish_url = cfg.get("ccdb_url_test") #or cfg["ccdb_url"]
    upload_to_qcdb(publish_url, cfg["qc_publish_path"], root_file, now_ms, now_ms + cfg.get("valid_duration_ms", 3_600_000), filename=source_filename)
    return f"{combined['coarse_name']} ({combined['fine_name']})"
