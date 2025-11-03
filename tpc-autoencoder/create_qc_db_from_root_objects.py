import os, re, json, pickle
import ROOT
from collections import OrderedDict
import traceback
from tqdm import tqdm

# ---------- TLatex cleanup ----------
def strip_latex(s: str) -> str:
    if not s:
        return s
    s = re.sub(r'#splitline\{([^}]*)\}\{([^}]*)\}', r'\1 \2', s)
    s = re.sub(r'#(?:color|font)\[[^\]]*\]\{([^}]*)\}', r'\1', s)  # unwrap #color/#font
    s = re.sub(r'^#(?:color|font)\[[^\]]*\]\{', '', s)             # handle missing trailing '}'
    s = re.sub(r'#(?:it|bf)\{([^}]*)\}', r'\1', s)
    s = s.replace('{', ' ').replace('}', ' ')
    s = re.sub(r'\s+', ' ', s).strip()
    s = re.sub(r'\s*:\s*', ': ', s)
    return s

# ---------- canvas finder ----------
def _read_obj(obj):
    return obj.ReadObj() if isinstance(obj, ROOT.TKey) else obj

def find_canvas(rootobj):
    obj = _read_obj(rootobj)
    if isinstance(obj, ROOT.TPad):
        return obj
    if isinstance(obj, (ROOT.TDirectory, ROOT.TDirectoryFile)):
        keys = obj.GetListOfKeys() or []
        for k in keys:
            c = find_canvas(k)
            if c:
                return c
        return None
    if hasattr(obj, "GetListOfPrimitives"):
        return obj
    return None

# ---------- text harvesting ----------
def _y_of(o):
    if hasattr(o, "GetY2"): return float(o.GetY2())
    if hasattr(o, "GetY"):  return float(o.GetY())
    return 0.0

def iter_texts(pad):
    out = []
    lst = pad.GetListOfPrimitives()
    if not lst:
        return []
    for prim in lst:
        if isinstance(prim, ROOT.TPad):
            out.extend(iter_texts(prim)); continue
        if isinstance(prim, ROOT.TLatex) or isinstance(prim, ROOT.TText):
            txt = prim.GetTitle() or getattr(prim, "GetText", lambda:"")()
            if txt: out.append((_y_of(prim), txt)); continue
        if isinstance(prim, ROOT.TPaveText):
            y = _y_of(prim)
            lines = prim.GetListOfLines()
            if lines:
                for ln in lines:
                    out.append((y, ln.GetTitle()))
            continue
        if isinstance(prim, ROOT.TPaveLabel):
            out.append((_y_of(prim), prim.GetLabel())); continue
        if isinstance(prim, ROOT.TLegend):
            y = _y_of(prim)
            entries = prim.GetListOfPrimitives() or prim.GetListOfEntries()
            if entries:
                for e in entries:
                    if hasattr(e, "GetLabel"):
                        out.append((y, e.GetLabel()))
            continue
        if hasattr(prim, "GetListOfPrimitives") and prim.GetListOfPrimitives():
            for sub in prim.GetListOfPrimitives():
                if isinstance(sub, ROOT.TLatex) or isinstance(sub, ROOT.TText):
                    out.append((_y_of(sub), sub.GetTitle()))
    out.sort(key=lambda t: -t[0])  # top→bottom
    cleaned = []
    for _, s in out:
        if not s: continue
        s = strip_latex(s.replace("\n"," "))
        if s:
            cleaned.append(s)
    return cleaned

# ---------- parse "key: value" lines only ----------
def to_keyval_dict(text_lines, accept_alt=False):
    sep_pattern = r":" if not accept_alt else r":|=|\s-\s"
    d = OrderedDict()
    for s in text_lines:
        s = re.sub(r"\s+", " ", s).strip()
        if not s or re.fullmatch(r"[-–—\s]+", s):
            continue
        if not re.search(sep_pattern, s):
            continue
        k, v = re.split(sep_pattern, s, maxsplit=1)
        k, v = k.strip().strip(' "\''), v.strip().strip(' "\'')

        # remove stray trailing braces moved from latex on either side
        k = k.replace('}', '').strip()
        v = v.replace('}', '').strip()

        if k:
            # If the same key shows multiple times, prefer a non-"quality missing!" value
            prev = d.get(k)
            if prev is None or (prev.lower().startswith("quality missing") and not v.lower().startswith("quality missing")):
                d[k] = v
    return d

# ---------- extract from ROOT file ----------
def extract_quality_from_root(root_path, key_name=None):
    f = ROOT.TFile.Open(root_path)
    if not f or f.IsZombie():
        raise RuntimeError(f"Cannot open: {root_path}")
    canvas = None
    if key_name:
        key = f.GetKey(key_name)
        if key:
            canvas = key.ReadObj()
    if canvas is None:
        canvas = find_canvas(f)
    if not canvas or not isinstance(canvas, ROOT.TPad):
        raise RuntimeError("No TCanvas/TPad found.")
    lst = canvas.GetListOfPrimitives()
    if not lst or lst.GetSize() == 0:
        raise RuntimeError("Canvas has no primitives (empty).")
    lines = iter_texts(canvas)
    kv = to_keyval_dict(lines, accept_alt=True)
    return kv, getattr(canvas, "GetName", lambda: None)()

# ---------- JSON metadata helpers ----------
def load_metadata_list(json_path):
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)

def select_metadata_for_file(meta_list, root_path):
    """
    Try to select the metadata item that corresponds to the ROOT you opened.
    Heuristics:
      1) fileName matches basename (when present)
      2) prefer ObjectType == 'TCanvas'
      3) pick latest by lastModified/createTime
    """
    base = os.path.basename(root_path)
    candidates = []
    for item in meta_list:
        score = 0
        if str(item.get("ObjectType","")).lower() == "tcanvas":
            score += 2
        if str(item.get("fileName","")) == base:
            score += 3
        lm = int(item.get("lastModified") or item.get("Last-Modified") or 0)
        ct = int(item.get("createTime") or item.get("Created") or 0)
        candidates.append((score, max(lm, ct), item))
    if not candidates:
        return {}
    # sort by score then time
    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return candidates[0][2]

# ---------- DB upsert ----------
def save_pickle(obj, out_path):
    with open(out_path, "wb") as f:
        pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)

def load_pickle_safe(in_path):
    try:
        with open(in_path, "rb") as f:
            return pickle.load(f)
    except Exception:
        return {}

def upsert_run(db_path, id: str, record: dict):
    db = load_pickle_safe(db_path)
    db[str(id)] = record
    save_pickle(db, db_path)
    return db

# ---------- main entry ----------
def process(root_file, json_meta_file, *, key_name="ccdb_object", db_pickle="quality_db.pkl", extra_meta=None):
    # 1) extract clean quality pairs
    kv, canvas_name = extract_quality_from_root(root_file, key_name=key_name)

    # 2) load & select metadata
    meta_list = load_metadata_list(json_meta_file)
    chosen = select_metadata_for_file(meta_list, root_file)
    if extra_meta:
        chosen.update(extra_meta)

    # 3) id (string)
    id = str(chosen.get("id") or chosen.get("id") or "unknown_id")

    # 4) build final record
    record = {
        "quality": kv,          # clean key→value pairs
        "metadata": chosen,     # full JSON object for that entry
    }

    # 5) upsert into DB
    upsert_run(db_pickle, id, record)

    # 6) return for immediate use
    return id, record

# ----------------- example usage -----------------
if __name__ == "__main__":
    ROOT.gROOT.SetBatch(True)  # headless-safe
    
    qc_object_path   = "downloads/qc/TPC/MO/Q_O_physics/"
    root_file_dir    = os.path.join(qc_object_path, "QualitySummary")
    abs_path_json_meta = os.path.abspath(os.path.join(qc_object_path, "QualitySummary.json"))
    failures_log     = "failed_to_enter_db.txt"

    # Only *.root files
    all_entries = sorted(os.listdir(os.path.abspath(root_file_dir)))
    root_files = [e for e in all_entries if e.lower().endswith(".root")]

    failed = []
    ok = 0

    with tqdm(root_files, desc="Processing ROOT canvases", unit="file") as pbar:
        for root_file in pbar:
            abs_path_root_file = os.path.abspath(os.path.join(root_file_dir, root_file))
            try:
                run, rec = process(abs_path_root_file, abs_path_json_meta,
                                   key_name="ccdb_object", db_pickle="quality_db.pkl")
                ok += 1
                # show run in the bar
                pbar.set_postfix({"ok": ok, "failed": len(failed), "run": str(run)})
            except Exception:
                failed.append(abs_path_root_file)
                with open(failures_log, "a", encoding="utf-8") as log:
                    log.write(f"{abs_path_root_file}\n{traceback.format_exc()}\n---\n")
                pbar.set_postfix({"ok": ok, "failed": len(failed)})

    # Final summary
    print(f"\nDone. Success: {ok}  Failed: {len(failed)}")
    if failed:
        print(f"See details in: {failures_log}")