
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

logger = logging.getLogger(__name__)

def load_json_file_into_df(filepath): 
    with open(filepath, "r") as f: 
        data = json.load(f)
    # Note: pd.DataFrame loads the list of dicts directly into columns, while pd.json_normalize also flattens nested JSON fields into separate columns.
    return pd.json_normalize(data) 


def config_logger(output_file="output.log"): 
    logger = logging.getLogger("tpc_qc")
    logger.propagate = False

    # 👇 If handlers already exist, don't add more
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


def convert_root_files_to_img(ROOT_FILES_PATH, img_folder_of_root_obj): 

    ROOT.gROOT.SetBatch(True)
    ROOT.gErrorIgnoreLevel = ROOT.kWarning   # hides "Info in <TCanvas::Print>" lines
    root_filenames = os.listdir(ROOT_FILES_PATH)
    os.makedirs(img_folder_of_root_obj, exist_ok=True)

    def strip_axes_and_titles(obj):
        if hasattr(obj, "SetTitle"):
            obj.SetTitle("")

        for axis_name in ["Xaxis", "Yaxis", "Zaxis"]:
            get_axis = getattr(obj, f"Get{axis_name}", None)
            if not get_axis:
                continue
            ax = get_axis()
            if not ax:
                continue
            ax.SetTitle("")       # no axis title
            ax.SetTitleSize(0.0)  # hide axis title
            ax.SetLabelSize(0.0)  # hide numbers
            ax.SetTickLength(0.0) # hide ticks if you want them gone as well

    
    for root_filename in tqdm(root_filenames, total=len(root_filenames), desc="Convert root objects to images."):

        try: 
            fpath = os.path.join(ROOT_FILES_PATH, root_filename)
            f = ROOT.TFile.Open(fpath, "READ")
            if not f or f.IsZombie():
                raise SystemExit(f"Failed to open {fpath}")

            h = f.Get("ccdb_object")
            if not h:
                raise SystemExit("Histogram not found. Pick a name printed in the previous cell.")

            tcanvas_prim_list = h.GetListOfPrimitives()

            for i, pad in enumerate(tcanvas_prim_list):

                # Create a new clean canvas
                cname = f"c_{root_filename[:-5]}_{i}"
                c = ROOT.TCanvas(cname, "", 2000, 2000)

                # Remove all margins so only the plot is visible
                c.SetLeftMargin(0.0)
                c.SetRightMargin(0.0)
                c.SetTopMargin(0.0)
                c.SetBottomMargin(0.0)

                # Turn off global titles / stats
                ROOT.gStyle.SetOptTitle(0)
                ROOT.gStyle.SetOptStat(0)

                prims = pad.GetListOfPrimitives()

                main_obj = None          # main TH2 / TH1 / TGraph, etc.
                overlay_objs = []        # lines, polygons, etc. that we keep

                for prim in prims:
                    cname_prim = prim.ClassName()

                    # choose first TH* or TGraph* as main object
                    # Identify the main drawable object
                    
                    if (main_obj is None and
                        (cname_prim.startswith("TH") or
                        cname_prim.startswith("TGraph") or
                        cname_prim.startswith("TProfile"))):

                        main_obj = prim

                        # if prim.InheritsFrom("TH2"):
                        #     for ix in range(1, prim.GetNbinsX()+1):
                        #         for iy in range(1, prim.GetNbinsY()+1):
                        #             val = prim.GetBinContent(ix, iy)
                        #             if val != 0:
                        #                 x = prim.GetXaxis().GetBinCenter(ix)
                        #                 y = prim.GetYaxis().GetBinCenter(iy)
                                        # Uncomment to print bin contents
                                        #print(f"  Bin({ix},{iy}) at (x={x:.2f}, y={y:.2f}) = {val}")
                        

                    # keep various "inside" graphics (lines, boxes, polygons...)
                    elif cname_prim in [
                        "TLine", "TPolyLine", "TPolyMarker", "TBox",
                        "TEllipse", "TArc", "TCutG"
                    ]:
                        overlay_objs.append(prim)

                    # implicitly SKIP palette axis, text labels, legends, etc.
                    # (TPaletteAxis, TLatex, TText, TLegend, ...)

                if not main_obj:
                    # Fall back: draw the whole pad if we didn't find a main object
                    pad.Draw()
                else:
                    # Clone so we don't modify the original object in the file
                    main_clone = main_obj.Clone()
                    strip_axes_and_titles(main_clone)

                    draw_opt = main_obj.GetDrawOption()
                    if not draw_opt:
                        draw_opt = "COL"

                    main_clone.Draw(draw_opt)

                    # Draw overlays on top
                    for obj in overlay_objs:
                        obj_clone = obj.Clone()
                        obj_clone.Draw(obj.GetDrawOption())
                        
                c.Update()

                out_name = os.path.join(
                    img_folder_of_root_obj, f"{root_filename[:-5]}_{i}.png"
                )
                c.SaveAs(out_name)

            f.Close()
        except Exception as e: 
            logger.error(f"Error on file: {root_filename} --> \n{e}")
            continue
        
        

def convert_root_files_to_img(ROOT_FILES_PATH, img_folder_of_root_obj): 

    ROOT.gROOT.SetBatch(True)
    ROOT.gErrorIgnoreLevel = ROOT.kWarning   # hides "Info in <TCanvas::Print>" lines
    root_filenames = os.listdir(ROOT_FILES_PATH)
    os.makedirs(img_folder_of_root_obj, exist_ok=True)

    def strip_axes_and_titles(obj):
        if hasattr(obj, "SetTitle"):
            obj.SetTitle("")

        for axis_name in ["Xaxis", "Yaxis", "Zaxis"]:
            get_axis = getattr(obj, f"Get{axis_name}", None)
            if not get_axis:
                continue
            ax = get_axis()
            if not ax:
                continue
            ax.SetTitle("")       # no axis title
            ax.SetTitleSize(0.0)  # hide axis title
            ax.SetLabelSize(0.0)  # hide numbers
            ax.SetTickLength(0.0) # hide ticks if you want them gone as well

    
    for root_filename in tqdm(root_filenames, total=len(root_filenames), desc="Convert root objects to images."):

        try: 
            fpath = os.path.join(ROOT_FILES_PATH, root_filename)
            f = ROOT.TFile.Open(fpath, "READ")
            if not f or f.IsZombie():
                raise SystemExit(f"Failed to open {fpath}")

            h = f.Get("ccdb_object")
            if not h:
                raise SystemExit("Histogram not found. Pick a name printed in the previous cell.")

            tcanvas_prim_list = h.GetListOfPrimitives()

            for i, pad in enumerate(tcanvas_prim_list):

                # Create a new clean canvas
                cname = f"c_{root_filename[:-5]}_{i}"
                c = ROOT.TCanvas(cname, "", 2000, 2000)

                # Remove all margins so only the plot is visible
                c.SetLeftMargin(0.0)
                c.SetRightMargin(0.0)
                c.SetTopMargin(0.0)
                c.SetBottomMargin(0.0)

                # Turn off global titles / stats
                ROOT.gStyle.SetOptTitle(0)
                ROOT.gStyle.SetOptStat(0)

                prims = pad.GetListOfPrimitives()

                main_obj = None          # main TH2 / TH1 / TGraph, etc.
                overlay_objs = []        # lines, polygons, etc. that we keep

                for prim in prims:
                    cname_prim = prim.ClassName()

                    # choose first TH* or TGraph* as main object
                    # Identify the main drawable object
                    
                    if (main_obj is None and
                        (cname_prim.startswith("TH") or
                        cname_prim.startswith("TGraph") or
                        cname_prim.startswith("TProfile"))):

                        main_obj = prim

                        # if prim.InheritsFrom("TH2"):
                        #     for ix in range(1, prim.GetNbinsX()+1):
                        #         for iy in range(1, prim.GetNbinsY()+1):
                        #             val = prim.GetBinContent(ix, iy)
                        #             if val != 0:
                        #                 x = prim.GetXaxis().GetBinCenter(ix)
                        #                 y = prim.GetYaxis().GetBinCenter(iy)
                        #                 # Uncomment to print bin contents
                        #                 print(f"  Bin({ix},{iy}) at (x={x:.2f}, y={y:.2f}) = {val}")
                                                        
                    # keep various "inside" graphics (lines, boxes, polygons...)
                    elif cname_prim in [
                        "TLine", "TPolyLine", "TPolyMarker", "TBox",
                        "TEllipse", "TArc", "TCutG"
                    ]:
                        overlay_objs.append(prim)

                    # implicitly SKIP palette axis, text labels, legends, etc.
                    # (TPaletteAxis, TLatex, TText, TLegend, ...)

                if not main_obj:
                    # Fall back: draw the whole pad if we didn't find a main object
                    pad.Draw()
                else:
                    # Clone so we don't modify the original object in the file
                    main_clone = main_obj.Clone()
                    strip_axes_and_titles(main_clone)

                
                    # ------------ NORMALIZATION BLOCK ------------
                    if main_clone.InheritsFrom("TH1"):
                        # max-normalization: value / max
                        max_val = main_clone.GetMaximum()
                        if max_val > 0:
                            main_clone.Scale(1.0 / max_val)
                            main_clone.SetMinimum(0.0)
                            main_clone.SetMaximum(1.0)
                            
                    draw_opt = main_obj.GetDrawOption()
                    if not draw_opt:
                        draw_opt = "COL"

                    main_clone.Draw(draw_opt)

                    # Draw overlays on top
                    for obj in overlay_objs:
                        obj_clone = obj.Clone()
                        obj_clone.Draw(obj.GetDrawOption())
                        
                c.Update()

                out_name = os.path.join(
                    img_folder_of_root_obj, f"{root_filename[:-5]}_{i}.png"
                )
                c.SaveAs(out_name)

            f.Close()
        except Exception as e: 
            logger.error(f"Error on file: {root_filename} --> \n{e}")
            continue
        

def convert_root_files_to_img_(ROOT_FILES_PATH, img_folder_of_root_obj): 
    
    root_filenames = os.listdir(ROOT_FILES_PATH)
    os.makedirs(img_folder_of_root_obj, exist_ok=True)

    for root_filename in tqdm(iterable=root_filenames, total=len(root_filenames)): 
        f = ROOT.TFile.Open(os.path.join(ROOT_FILES_PATH,root_filename), "READ")
        if not f or f.IsZombie():
            raise SystemExit(f"Failed to open {os.path.join(ROOT_FILES_PATH,root_filename)}")
        
        h = f.Get("ccdb_object") 
        if not h:
            raise SystemExit("Histogram not found. Pick a name printed in the previous cell.")

        tcanvas_prim_list = h.GetListOfPrimitives()
            
        for i, pad in enumerate(tcanvas_prim_list):
            
            if hasattr(pad, "SetTitle"):
                pad.SetTitle("")
            if pad.InheritsFrom("TPaletteAxis"):
                pad.Remove(pad)
            
            # Save the pad in an image with 2000x2000 pixels 
            c = ROOT.TCanvas(f"c_{root_filename[:-5]}_{i}", "", 2000, 2000)
            c.SetWindowSize(2000,2000)
            pad.Draw()
            pad.SetPad(0, 0, 0.9, 0.9)  # stretch pad to fill the whole canvas for better resolution
            c.Update()
            c.SaveAs(str(os.path.join(img_folder_of_root_obj, f"{root_filename[:-5]}_{i}.png")))


def plot_qs_cluster_per_run_number(ref_times, cl_times, run_number):

    fig = go.Figure()

    # Q_sum trace
    fig.add_trace(
        go.Scatter(
            x=pd.to_datetime(ref_times, unit="ms"),
            y=['Qsum' for _ in range(len(ref_times))],
            mode="markers",
            name=f"Q_sum (N={len(ref_times)})",
        )
    )

    # Cluster trace
    fig.add_trace(
        go.Scatter(
            x=pd.to_datetime(cl_times, unit="ms"),
            y=["Cluster" for _ in range(len(cl_times))],
            mode="markers",
            name=f"Cluster (N={len(cl_times)}))",
        )
    )

    fig.update_layout(
        title=f"Creation times for run number: {run_number}",
        xaxis_title="createTime",
        yaxis=dict(title=""),
        height=400,
    )

    fig.show()