import os 
import random
from typing import Dict, Any
import numpy as np
import torch
from pathlib import Path
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple
import yaml
import mlflow
import subprocess
import torch
import numpy as np
from PIL import Image as PILImage
try:
    import ROOT
except Exception as exc:
    raise SystemExit("PyROOT not available: " + str(exc))

import tempfile


def seed_everything(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)


def flatten_dict(d: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """Flatten nested dicts for MLflow params."""
    out = {}
    for k, v in d.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            out.update(flatten_dict(v, key))
        else:
            # MLflow params are best as str/int/float/bool
            out[key] = v
    return out




def _git(cmd: list[str]) -> str:
    return subprocess.check_output(["git", *cmd], text=True).strip()

def log_git_to_mlflow(log_diff: bool = False) -> None:
    try:
        commit = _git(["rev-parse", "HEAD"])
        branch = _git(["rev-parse", "--abbrev-ref", "HEAD"])
        is_dirty = subprocess.call(["git", "diff", "--quiet"]) != 0 or \
                   subprocess.call(["git", "diff", "--cached", "--quiet"]) != 0

        # searchable/filterable in the MLflow UI
        mlflow.set_tags({
            "git.commit": commit,
            "git.branch": branch,
            "git.dirty": str(is_dirty).lower(),
        })

        # Optional: remote URL
        try:
            remote = _git(["remote", "get-url", "origin"])
            mlflow.set_tag("git.remote.origin", remote)
        except Exception:
            pass

        if log_diff and is_dirty:
            diff = subprocess.check_output(["git", "diff"], text=True)
            mlflow.log_text(diff, artifact_file="git_diff.patch")

    except Exception:
        # If not in a git repo or git not installed, just skip quietly
        mlflow.set_tag("git.info", "unavailable")




def load_yaml(path: str | Path) -> dict:
    path = Path(path).expanduser().resolve()

    if not path.exists():
        raise FileNotFoundError(f"YAML config not found: {path}")

    text = path.read_text()
    if not text.strip():
        raise ValueError(f"YAML config is empty: {path}")

    cfg = yaml.safe_load(text)

    if cfg is None:
        raise ValueError(f"YAML parsed to None (empty/invalid): {path}")
    if not isinstance(cfg, dict):
        raise TypeError(f"Top-level YAML must be a dict, got {type(cfg)} in {path}")

    print(f"Loaded .yaml file: {path}")
    return cfg

def resolve_repo_root(repo_root: str | Path | None = None) -> Path:
    return Path(repo_root).resolve() if repo_root is not None else Path.cwd().resolve()


def root_to_pil_image(root_path: str, pad_index: int = 0, grey_scale: bool = True,
                      W: int = 330, H: int = 330):
    """Render one pad from a ROOT ccdb_object canvas and return a PIL Image in memory."""
    ROOT.gROOT.SetBatch(True)
    ROOT.gErrorIgnoreLevel = ROOT.kWarning

    f = ROOT.TFile.Open(root_path, "READ")
    canvas = f.Get("ccdb_object")
    if not canvas:
        f.Close()
        raise RuntimeError(f"ccdb_object not found in {root_path}")

    src_pad = canvas.GetListOfPrimitives().At(pad_index)
    if src_pad is None:
        f.Close()
        raise IndexError(f"pad_index={pad_index} out of range in {root_path}")

    # Build an exact-size canvas with a margin-free pad
    c = ROOT.TCanvas(f"c_rpi_{pad_index}", "", W, H)
    c.SetCanvasSize(W, H)
    c.SetFillColor(0)
    c.SetBorderMode(0)

    p = ROOT.TPad("p_rpi", "", 0, 0, 1, 1)
    p.SetFillColor(0)
    p.SetBorderMode(0)
    p.SetBorderSize(0)
    p.SetLeftMargin(0.0)
    p.SetRightMargin(0.0)
    p.SetTopMargin(0.0)
    p.SetBottomMargin(0.0)
    p.SetTicks(0, 0)
    p.SetGrid(0, 0)
    ROOT.gStyle.SetPadTickX(0)
    ROOT.gStyle.SetPadTickY(0)
    p.Draw()
    p.cd()

    ROOT.gStyle.SetOptTitle(0)
    ROOT.gStyle.SetOptStat(0)

    # Find and draw the main histogram/graph object
    main_obj = None
    for prim in src_pad.GetListOfPrimitives():
        cn = prim.ClassName()
        if cn.startswith("TH") or cn.startswith("TGraph") or cn.startswith("TProfile"):
            main_obj = prim
            break

    if main_obj:
        clone = main_obj.Clone()
        if clone.InheritsFrom("TH1") or clone.InheritsFrom("TProfile"):
            clone.SetTitle("")
            try:
                clone.SetStats(0)
            except Exception:
                pass
            for get_ax in (clone.GetXaxis, clone.GetYaxis):
                ax = get_ax()
                ax.SetLabelSize(0.0)
                ax.SetTitleSize(0.0)
                ax.SetTickLength(0.0)
                ax.SetNdivisions(0, True)
                ax.SetAxisColor(0)
                ax.SetLabelColor(0)
                ax.SetTitleColor(0)
        clone.Draw(main_obj.GetDrawOption() or "COL")
    else:
        src_pad.Draw()

    p.Update()
    c.Update()

    # Capture rendered pixels via TImage -> temp file -> PIL
    img = ROOT.TImage.Create()
    img.FromPad(p)
    if grey_scale:
        img.Gray()

    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        img.WriteImage(tmp_path)
        pil_img = PILImage.open(tmp_path).copy()
    finally:
        os.remove(tmp_path)

    c.Close()
    f.Close()
    
    return pil_img






import os 
import json 
import pandas as pd 
import os 
import math, os, sys, re
import time
from tqdm import tqdm
try:
    import ROOT 
except Exception as e:
    raise SystemExit("PyROOT import failed. Make sure your kernel uses the env with ROOT installed.\n" + str(e))


import pandas as pd
import plotly.graph_objects as go

from array import array
import numpy as np
from tqdm.auto import tqdm 
import shutil 
import logging
import tempfile

logger = logging.getLogger(__name__)

def load_json_file_into_df(filepath): 
    with open(filepath, "r") as f: 
        data = json.load(f)
    # Note: pd.DataFrame loads the list of dicts directly into columns, while pd.json_normalize also flattens nested JSON fields into separate columns.
    return pd.json_normalize(data) 


def config_logger(output_file="output.log"): 
    logger = logging.getLogger("tpc_qc")
    logger.propagate = False

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    # log to file
    file_handler = logging.FileHandler(output_file, mode="w")
    file_handler.setLevel(logging.INFO)

    # log to terminal (stdout)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(fmt)
    stream_handler.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    logger.info("Logger initialized")

    return logger


def load_quality_summ_from_root_objects(filepath_of_root_objects):
    
    list_root_obj_names_wprefix = os.listdir(filepath_of_root_objects)
    logger.info(f'Root objects to process and extract the quality summary from: {len(list_root_obj_names_wprefix)}')

    quality_dict = {}

    for root_obj in list_root_obj_names_wprefix: 
        ROOT_FILE = os.path.join(filepath_of_root_objects,root_obj)
        root_object_name_wtprefix = os.path.basename(ROOT_FILE)

        f = ROOT.TFile.Open(ROOT_FILE, "READ")
        if not f or f.IsZombie():
            raise SystemExit(f"Failed to open {ROOT_FILE}")

        canvas = f.Get("ccdb_object") 
        if not canvas:
            raise SystemExit("Canvas not found.")

        tcanvas_prim_list = canvas.GetListOfPrimitives()
        try: 
            obj = tcanvas_prim_list[0] # it will always have one object in the quality summaries case 

            lines = obj.GetListOfLines()  # this is a TList of TLatex
            for line in lines:
                # for TLatex, text is in the "title"
                text = line.GetTitle()
                match_obj = re.search(r"\{([^}]*)\}", text)
                if not match_obj:
                    continue
                match_text = match_obj.group(1)
                    
                # split in key-value pairs to save in a json file 
                if ":" in match_text:
                    key, value = [s.strip() for s in match_text.split(":",1)]

                    quality_dict.setdefault(root_object_name_wtprefix, {})
                    quality_dict[root_object_name_wtprefix][key] = value
        except Exception as e: 
            #logger.error(f"Extracting quality summary from root object: {root_obj}, with error: {e}")
            continue
    return quality_dict
    

def build_bkkp_run_api_url(
    runs_limit,
    detector,
    run_definition,
    run_qualities,
    personal_token,
    tag_value=None,
    tag_operation=None
):

    url = (
        f"https://ali-bookkeeping.cern.ch/api/runs?"
        f"page[limit]={runs_limit}"
        f"&filter[detectors][values]={detector}"
        f"&filter[detectors][operator]=and"
        f"&filter[definitions]={run_definition}"
        f"&filter[runQualities]={run_qualities}"
        f"&token={personal_token}"
    )

    # Add tag filters only if provided
    if tag_value is not None and tag_operation is not None:
        url += (
            f"&filter[tags][values]={tag_value}"
            f"&filter[tags][operation]={tag_operation}"
        )

    return url


def has_good_detector_quality(run: dict, detector_name: str) -> bool:
    for dq in run.get("detectorsQualities", []):
        if dq.get("name") == detector_name and dq.get("quality") == "good":
            return True
    return False


def has_bad_detector_quality(run: dict, detector_name: str) -> bool:
    for dq in run.get("detectorsQualities", []):
        if dq.get("name") == detector_name and dq.get("quality") == "bad":
            return True
    return False


def is_in_stable_beams(run: dict) -> bool:
    return run.get("lhcBeamMode") == "STABLE BEAMS"


def has_beam_type(run: dict, beam_type: str) -> bool:   # 'PP' or 'PbPb'
    return run.get("pdpBeamType") == beam_type


# Convert ROOT to IMAGES 

def make_canvas_exact(name: str, w: int, h: int) -> ROOT.TCanvas:
    """
    ROOT's TCanvas(w,h) is not always the final pixel buffer size.
    This forces the internal canvas size to w×h.
    """
    c = ROOT.TCanvas(name, "", w, h)
    c.SetFillColor(0)
    c.SetBorderMode(0)
    c.SetBorderSize(0)

    c.SetCanvasSize(w, h)

    try:
        # Make sure the window is the requested size 
        dw = w - c.GetWw()
        dh = h - c.GetWh()
        c.SetWindowSize(w + dw, h + dh)
    except Exception:
        pass

    return c


def strip_axes_and_ticks(h):
    """
    Remove titles, labels, tick marks, and divisions.
    Works for TH1/TH2/TProfile-like objects.
    """
    if not h:
        return

    h.SetTitle("")
    try:
        h.SetStats(0)
    except Exception:
        pass

    # Axes (TH1/TH2/TProfile)
    for ax_getter in (h.GetXaxis, h.GetYaxis, getattr(h, "GetZaxis", None)):
        if ax_getter is None:
            continue
        ax = ax_getter()
        if not ax:
            continue

        ax.SetTitle("")
        ax.SetLabelSize(0.0)
        ax.SetTitleSize(0.0)
        ax.SetTickLength(0.0)     # <-- kills ruler ticks
        ax.SetNdivisions(0, True) # <-- kills tick subdivisions
        ax.SetAxisColor(0)
        ax.SetLabelColor(0)
        ax.SetTitleColor(0)


def pad_no_ticks(p: ROOT.TPad):
    # Pad-level ticks off (ROOT can draw ticks from pad settings)
    p.SetTicks(0, 0)
    p.SetGrid(0, 0)

    ROOT.gStyle.SetPadTickX(0)
    ROOT.gStyle.SetPadTickY(0)


def export_pad_png_1to1(p: ROOT.TPad, out_png: str, grey_scale: bool):
    p.Update()
    p.GetCanvas().Update()

    img = ROOT.TImage.Create()
    img.FromPad(p) #    Export exactly the rendered pad pixels (avoid SaveAs driver scaling).
    if grey_scale: 
        img.Gray()
    # print("Captured image size:",
    #   img.GetWidth(), "x", img.GetHeight())
    img.WriteImage(out_png)


def convert_root_files_to_img(ROOT_FILES_PATH, img_folder_of_root_obj, grey_scale=True, W=330, H=330):

    ROOT.gROOT.SetBatch(True)
    ROOT.gErrorIgnoreLevel = ROOT.kWarning

    os.makedirs(img_folder_of_root_obj, exist_ok=True)
    root_filenames = [fn for fn in os.listdir(ROOT_FILES_PATH) if fn.endswith(".root")]

    for root_filename in tqdm(root_filenames, total=len(root_filenames),
                              desc="Convert root objects to images."):

        try:
            fpath = os.path.join(ROOT_FILES_PATH, root_filename)
            f = ROOT.TFile.Open(fpath, "READ")

            canvas = f.Get("ccdb_object")
            if not canvas:
                raise RuntimeError("ccdb_object not found in file.")

            tcanvas_prim_list = canvas.GetListOfPrimitives()

            for i, src_pad in enumerate(tcanvas_prim_list):
                if i < 2: # Because we know that the 3rd and 4rth pad are the histograms

                    c = make_canvas_exact(f"c_{root_filename[:-5]}_{i}", W, H)

                    # Fill the whole canvas with a pad (no margins/borders)
                    p = ROOT.TPad("p_tmp", "", 0, 0, 1, 1)
                    p.SetFillColor(0)
                    p.SetBorderMode(0)
                    p.SetBorderSize(0)
                    p.SetLeftMargin(0.0)
                    p.SetRightMargin(0.0)
                    p.SetTopMargin(0.0)
                    p.SetBottomMargin(0.0)
                    pad_no_ticks(p)

                    p.Draw()
                    p.cd()

                    ROOT.gStyle.SetOptTitle(0)
                    ROOT.gStyle.SetOptStat(0)

                    prims = src_pad.GetListOfPrimitives()

                    main_obj = None
                    overlay_objs = []

                    for prim in prims:
                        cname_prim = prim.ClassName()

                        if (main_obj is None and
                            (cname_prim.startswith("TH") or
                            cname_prim.startswith("TGraph") or
                            cname_prim.startswith("TProfile"))):
                            main_obj = prim

                        elif cname_prim in [
                            "TLine", "TPolyLine", "TPolyMarker", "TBox",
                            "TEllipse", "TArc", "TCutG"
                        ]:
                            overlay_objs.append(prim)

                    if main_obj:
                        main_clone = main_obj.Clone()
                        if main_clone.InheritsFrom("TH1") or main_clone.InheritsFrom("TProfile"):
                            strip_axes_and_ticks(main_clone)

                        draw_opt = main_obj.GetDrawOption() or "COL"
                        main_clone.Draw(draw_opt)

                        for obj in overlay_objs:
                            obj_clone = obj.Clone()
                            obj_clone.Draw(obj.GetDrawOption())
                    else:
                        # fallback: draw whole pad content
                        src_pad.Draw()

                    out_name = os.path.join(img_folder_of_root_obj, f"{root_filename[:-5]}_{i}.png")
                    export_pad_png_1to1(p, out_name, grey_scale)

                    c.Close()

                f.Close()

        except Exception as e:
            print(f"Error on file {root_filename}: {e}")
            continue


def root_to_pil_image(root_path: str, pad_index: int = 0, grey_scale: bool = True, W: int = 330, H: int = 330):
    """Render one pad from a ROOT file and return a PIL Image entirely in memory."""
    import numpy as np
    from PIL import Image as PILImage

    f = ROOT.TFile.Open(root_path, "READ")
    canvas = f.Get("ccdb_object")
    if not canvas:
        f.Close()
        raise RuntimeError(f"ccdb_object not found in {root_path}")

    tcanvas_prim_list = canvas.GetListOfPrimitives()
    src_pad = tcanvas_prim_list.At(pad_index)
    if src_pad is None:
        f.Close()
        raise IndexError(f"pad_index={pad_index} out of range in {root_path}")

    c = make_canvas_exact(f"c_tmp_{pad_index}", W, H)
    p = ROOT.TPad("p_tmp", "", 0, 0, 1, 1)
    p.SetFillColor(0)
    p.SetBorderMode(0)
    p.SetBorderSize(0)
    p.SetLeftMargin(0.0)
    p.SetRightMargin(0.0)
    p.SetTopMargin(0.0)
    p.SetBottomMargin(0.0)
    pad_no_ticks(p)
    p.Draw()
    p.cd()

    ROOT.gStyle.SetOptTitle(0)
    ROOT.gStyle.SetOptStat(0)

    main_obj = None
    for prim in src_pad.GetListOfPrimitives():
        cname = prim.ClassName()
        if cname.startswith("TH") or cname.startswith("TGraph") or cname.startswith("TProfile"):
            main_obj = prim
            break

    if main_obj:
        main_clone = main_obj.Clone()
        if main_clone.InheritsFrom("TH1") or main_clone.InheritsFrom("TProfile"):
            strip_axes_and_ticks(main_clone)
        main_clone.Draw(main_obj.GetDrawOption() or "COL")
    else:
        src_pad.Draw()

    p.Update()
    c.Update()

    img = ROOT.TImage.Create()
    img.FromPad(p)
    if grey_scale:
        img.Gray()

    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        img.WriteImage(tmp_path)
        pil_img = PILImage.open(tmp_path).copy()
    finally:
        os.remove(tmp_path)

    c.Close()
    f.Close()
    return pil_img


# Convert ROOT to TENSORS 
import os 
from tqdm import tqdm
import ROOT 
import numpy as np
from typing import Any, List, Tuple


def th2_to_numpy(th2:Any) -> np.ndarray:
    
    nx = th2.GetNbinsX()
    ny = th2.GetNbinsY()
    a = np.zeros((ny, nx), dtype=np.float32)  # rows=y, cols=x
    for ix in range(1, nx+1):
        for iy in range(1, ny+1):
            a[iy-1, ix-1] = th2.GetBinContent(ix, iy)
    return a

def convert_root_files_to_tensors(
        ROOT_FILES_PATH: str,
        dest_folder: str
    ) -> List[Tuple[str, str, np.ndarray]]:
    
    """
    Extract TH2 histograms from ROOT files and convert them to NumPy arrays.

    Returns
    -------
    List of tuples:
        ("TH2", histogram_name, numpy_array)
    """
        
    root_filenames = [
        f for f in os.listdir(ROOT_FILES_PATH)
        if f.endswith(".root")
        and os.path.isfile(os.path.join(ROOT_FILES_PATH, f))
    ]    
    
    os.makedirs(dest_folder, exist_ok=True)

    for root_filename in tqdm(iterable=root_filenames, total=len(root_filenames)): 
        f = ROOT.TFile.Open(os.path.join(ROOT_FILES_PATH,root_filename), "READ")
        if not f or f.IsZombie(): # IsZombie checks if ROOT failed internally
            raise SystemExit(f"Failed to open {os.path.join(ROOT_FILES_PATH,root_filename)}")
        
        canvas = f.Get("ccdb_object") 
        if not canvas:
            raise SystemExit("Histogram not found. Pick a name printed in the previous cell.")
        
        tcanvas_prim_list = canvas.GetListOfPrimitives() #  { @0x16dcf0f78, @0x16dcf0f78, @0x16dcf0f78, @0x16dcf0f78 }
        
        tensors = []        

        for i , pad in enumerate(tcanvas_prim_list):
            if i<2: 
                prims = pad.GetListOfPrimitives()

                for obj in prims: # obj has TFrame, TH2, TLine and anything relevant to draw the pad which the histogram we are seeing
                
                    cls = obj.ClassName()

                    if obj.InheritsFrom("TH2"):
                        tensor = th2_to_numpy(obj)
                        tensors.append(tensor)

        if tensors:
            data = np.stack(tensors)  # (N, H, W)

            np.savez_compressed(
                os.path.join(dest_folder, root_filename.replace(".root", ".npz")),
                data=data
            )


def save_response_to_file(resp, outdir: str, fallback_name: str = "download.bin") -> str:
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
