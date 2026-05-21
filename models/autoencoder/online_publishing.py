import datetime
import os
from pathlib import Path

import numpy as np
import requests

try:
    import ROOT
except Exception as exc:
    raise SystemExit(f"PyROOT import failed: {exc}")

COARSE_SEVERITY = {"Bad": 2, "Medium": 1, "Good": 0}
_PAD_TITLES = {0: "ML Anomaly Map (A-side)", 1: "ML Anomaly Map (C-side)"}


def combine_predictions(predictions: list) -> dict:
    """Return the worst-case prediction across all pads (Bad > Medium > Good)."""
    return max(predictions, key=lambda p: COARSE_SEVERITY[p["coarse_name"]])


ROOT.gROOT.SetBatch(True)


def _make_th2f(arr2d: np.ndarray, name: str, title: str) -> "ROOT.TH2F":
    """Fill a TH2F from a 2-D numpy array (y-axis flipped to match image convention)."""
    ny, nx = arr2d.shape
    h2 = ROOT.TH2F(name, f"{title};x;y", nx, 0, nx, ny, 0, ny)
    h2.SetDirectory(0)
    h2.SetStats(0)
    content = np.zeros((ny + 2, nx + 2), dtype=np.float64)
    content[1:ny + 1, 1:nx + 1] = arr2d[::-1, :]
    h2.SetContent(content.ravel())
    return h2


def write_prediction_root_file(
    prediction: dict,
    out_path: str,
    loss_maps: dict = None,
    pad_predictions: dict = None,
) -> str:
    """Write a ROOT file with a single TCanvas (ccdb_object).

    Layout: top strip with aggregated label, then one sub-pad per pad side by side.
    loss_maps:      {pad_index: 2-D np.ndarray}
    pad_predictions: {pad_index: prediction_dict}  — for per-pad sub-titles
    """
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    label = prediction["coarse_name"]
    n_pads = len(loss_maps) if loss_maps else 1

    # Close all lingering canvases and restore global ROOT state from scratch
    for c in list(ROOT.gROOT.GetListOfCanvases()):
        c.Close()
    ROOT.gROOT.SetBatch(True)
    ROOT.gStyle.SetPalette(ROOT.kDarkBodyRadiator)

    canvas = ROOT.TCanvas("ccdb_object", f"ML Prediction: {label}", 620 * n_pads, 700)

    # --- top title strip ---
    title_pad = ROOT.TPad("title_pad", "", 0.0, 0.88, 1.0, 1.0)
    title_pad.SetFillColor(0)
    title_pad.SetBorderSize(0)
    title_pad.Draw()
    title_pad.cd()
    pave = ROOT.TPaveText(0.01, 0.05, 0.99, 0.95, "brNDC")
    pave.SetFillColor(0)
    pave.SetBorderSize(1)
    pave.SetTextFont(62)
    pave.SetTextSize(0.40)
    pave.AddText(f"Aggregated ML Quality: {label}")
    pave.Draw()
    canvas.cd()

    kept = []  # hold references so Python GC doesn't collect before Update()
    if loss_maps:
        pad_width = 1.0 / n_pads
        for slot, (pad_idx, lm) in enumerate(sorted(loss_maps.items())):
            x1, x2 = slot * pad_width, (slot + 1) * pad_width
            map_pad = ROOT.TPad(f"map_pad_{pad_idx}", "", x1, 0.0, x2, 0.88)
            map_pad.SetRightMargin(0.16)
            map_pad.SetLeftMargin(0.10)
            map_pad.Draw()
            map_pad.cd()

            pred = (pad_predictions or {}).get(pad_idx, {})
            coarse = pred.get("coarse_name", "")
            base   = _PAD_TITLES.get(pad_idx, f"ML Anomaly Map (Pad {pad_idx})")
            suffix = f" [{coarse}]" if coarse else ""
            h2 = _make_th2f(lm, name=f"loss_map_pad{pad_idx}", title=base + suffix)
            h2.Draw("colz")
            kept += [map_pad, h2]
            canvas.cd()

    kept += [title_pad, pave]
    canvas.Update()

    f = ROOT.TFile(out_path, "RECREATE")
    canvas.Write()
    f.Close()
    canvas.Close()  # explicit cleanup before next iteration
    print(f"  Written: {out_path}  [Aggregated ML Quality: {label}]  [{n_pads} map(s)]")
    return out_path


def upload_to_qcdb(ccdb_url: str, publish_path: str, root_file: str, valid_from_ms: int, valid_until_ms: int, filename: str = None):
    """Upload ROOT file to QCDB via HTTP POST multipart."""
    url = f"{ccdb_url.rstrip('/')}/{publish_path.strip('/')}/{valid_from_ms}/{valid_until_ms}"
    upload_name = filename or os.path.basename(root_file)
    with open(root_file, "rb") as f:
        resp = requests.post(url, files={"blob": (upload_name, f, "application/root")}, timeout=30)#, verify="/Users/zetasourpi/.globus/usercert.pem")
    resp.raise_for_status()
    print(f"  Uploaded to QCDB: {url}  (HTTP {resp.status_code})")
    return resp


def publish_predictions(
    predictions: list,
    cfg: dict,
    source_filename: str = None,
    loss_maps: dict = None,
) -> str:
    """Aggregate pad predictions, write ROOT file, upload to QCDB. Returns prediction string."""
    combined = combine_predictions(predictions)
    pad_predictions = {p["pad_index"]: p for p in predictions}
    root_file = write_prediction_root_file(
        combined,
        cfg.get("publish_root_file", "ML_prediction.root"),
        loss_maps=loss_maps,
        pad_predictions=pad_predictions,
    )
    now_ms = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
    publish_url = cfg.get("ccdb_url_test") #or cfg["ccdb_url"]
    upload_to_qcdb(publish_url, cfg["qc_publish_path"], root_file, now_ms, now_ms + cfg.get("valid_duration_ms", 3_600_000), filename=source_filename)
    return f"{combined['coarse_name']} ({combined['fine_name']})"
