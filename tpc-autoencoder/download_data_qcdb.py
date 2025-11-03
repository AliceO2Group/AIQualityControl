#!/usr/bin/env python3
import os, sys, re, json, time, pathlib, requests
from urllib.parse import quote
import pathlib
from pathlib import Path

# ------------ CONFIG ------------
BASE      = "http://ali-qcdb-gpn.cern.ch:8083"     # CCDB/QCDB endpoint
QC_PREFIX = "qc/TPC/MO/Q_O_physics/QualitySummary" # subtree to crawl
OUT_DIR   = "./downloads"                          # where to save files


TIMEOUT   = 60

sess = requests.Session()
HEADERS_JSON = {"Accept": "application/json"}
HEADERS_BIN  = {}  # CCDB will set content headers on the response

qcdb = {"BASE_DIR": BASE, "QC_PREFIX": QC_PREFIX, "OUT_DIR": OUT_DIR
      , "TIMEOUT": TIMEOUT, "DATA": {}}


def download_file(path, obj):
        url = f"{BASE}/download/{quote(os.path.join(path, obj['name']))}"
        r = sess.get(url, headers=HEADERS_BIN, timeout=TIMEOUT)
        r.raise_for_status()
        outpath = os.path.join(OUT_DIR, path, obj['name'])
        with open(outpath, 'wb') as f:
            f.write(r.content)
        print(f"Downloaded {outpath} ({len(r.content)} bytes)")
        
        
def save_response_to_file(resp, outdir, fallback_name="download.bin"):
    os.makedirs(outdir, exist_ok=True)
    # filename from Content-Disposition if possible
    cd = resp.headers.get("Content-Disposition", "")
    m = re.search(r'filename="([^"]+)"', cd)
    filename = m.group(1) if m else fallback_name
    dst = os.path.join(outdir, filename)
    with open(dst, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024*1024):
            if chunk:
                f.write(chunk)
    return dst

def save_json_to_file_flat(data, outdir, ccdb_path):
    fpath = (Path(outdir) / ccdb_path).with_suffix(".json")
    fpath.parent.mkdir(parents=True, exist_ok=True)
    with fpath.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def user_interaction(path, objects):
    user_action_input = input(f"Do you want to download from {path} - (y/n) or 'q' to quit: ")
    action = None # default action is to skip download
    
    if user_action_input.lower() in ['q', 'quit', 'exit']:
        print("Exiting.")
        sys.exit(0)
    elif user_action_input.lower() in ['n', 'no']:
        print(f"Skipping download of files.")
    elif user_action_input.lower() in ['y', 'yes']:
        action = True
    else:
        print("Invalid input. Skipping download.")

    user_set_limit = input(f"Do you want to set a limit to the number of objects to download? (total objects {len(objects)}) (y/n): ")

    if user_set_limit.lower() in ['y', 'yes']:
        limit = input("Enter the limit: ")
        try:
            limit = int(limit)
            if limit <= 0:
                print("Invalid limit. No limit will be applied.")
                limit = None
                
        except ValueError:
            print("Invalid input. No limit will be applied.")
            limit = None
    elif user_set_limit.lower() in ['n', 'no']:
        limit = None

    return action, limit if 'limit' in locals() else None


def browse(path):
    """Return (subdirs, objects) for QCDB /browse/<path>."""
    url = f"{BASE}/browse/{quote(path)}"
    r = sess.get(url, headers=HEADERS_JSON, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if data['subfolders']:
        for subfolder in data['subfolders']:
            print("DIR ", os.path.join(path, subfolder))
            browse(subfolder)
    else: # update the metadata in the qcdb dict and download files
        qcdb['DATA'].setdefault(path, [])
        action ,limit = user_interaction(path, data['objects'])
        
        if action is None:
            print(f"Skipping download of files in {path}.")
            return # skip download, go to next folder
        
        if limit is not None and limit < len(data['objects']):
            print(f"Limiting download to {limit} objects.")
            data['objects'] = data['objects'][:limit]

        for i, obj in enumerate(data['objects']):
            
            print(f"OBJ {i+1}/{len(data['objects'])} in {path}: {obj['fileName']} (ETag: {obj['ETag']})")
            print("FILE", os.path.join(path, obj['fileName']))
            
            # Update metadata    
            qcdb['DATA'][path].append(obj)
            # Download the file
            download_base_url = f"{BASE}/download/"
            etag = qcdb['DATA'][path][i]['ETag'].strip('"')
            url = os.path.join(download_base_url, etag)
            download = sess.get(url, headers=HEADERS_BIN, timeout=TIMEOUT)
            download.raise_for_status()
            
            outfile = save_response_to_file(download, os.path.join(OUT_DIR, path), fallback_name=qcdb['DATA'][path][i]['fileName'])
            print("Saved:", outfile)
            
        # Save metadata to a JSON file
        save_json_to_file_flat(qcdb['DATA'][path], OUT_DIR, path)
    return qcdb['DATA']

browse(QC_PREFIX) 

