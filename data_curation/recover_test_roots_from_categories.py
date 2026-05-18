#!/usr/bin/env python3
import argparse
import json
import re
import shutil
from pathlib import Path
from urllib.parse import quote

import requests


DEFAULT_QCDB_ENDPOINT = "http://ali-qcdb-gpn.cern.ch:8083"
DEFAULT_QCDB_PATH = "qc/TPC/MO/Clusters/c_Sides_N_Clusters"


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Recover ROOT objects referenced by categorized PNGs. "
            "The script strips the trailing hist suffix like _0/_1 from PNG names, "
            "maps them to TObject_*.root files, copies any local matches from the "
            "test folder, and downloads missing ones from QCDB."
        )
    )
    parser.add_argument(
        "--categories-dir",
        default="data/tpc/test/rgb_images/categories",
        help="Directory containing category subfolders with PNG files.",
    )
    parser.add_argument(
        "--local-root-dir",
        default="data/tpc/test",
        help="Directory that may already contain matching local ROOT files.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/tpc/test/recovered_roots",
        help="Folder where recovered ROOT files will be written.",
    )
    parser.add_argument(
        "--qcdb-endpoint",
        default=DEFAULT_QCDB_ENDPOINT,
        help="QCDB endpoint used for missing files.",
    )
    parser.add_argument(
        "--qcdb-path",
        default=DEFAULT_QCDB_PATH,
        help="QCDB object path to browse for ROOT metadata.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="HTTP timeout in seconds for QCDB calls.",
    )
    parser.add_argument(
        "--manifest-name",
        default="recovery_manifest.json",
        help="Name of the manifest JSON saved inside the output directory.",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Only copy existing local ROOT files and report what is still missing.",
    )
    return parser.parse_args()


def normalize_png_name_to_root_name(png_path: Path) -> str:
    stem = re.sub(r"_[01]$", "", png_path.stem)
    return f"{stem}.root"


def collect_requested_roots(categories_dir: Path):
    requested = {}
    for png_path in sorted(categories_dir.rglob("*.png")):
        if png_path.name.startswith("."):
            continue
        root_name = normalize_png_name_to_root_name(png_path)
        category = png_path.parent.name
        requested.setdefault(root_name, {"categories": set(), "pngs": []})
        requested[root_name]["categories"].add(category)
        requested[root_name]["pngs"].append(str(png_path))
    return requested


def browse_qcdb_metadata(session: requests.Session, endpoint: str, qcdb_path: str, timeout: int):
    url = f"{endpoint.rstrip('/')}/browse/{quote(qcdb_path)}"
    response = session.get(url, headers={"Accept": "application/json"}, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    return payload.get("objects", [])


def build_qcdb_index(objects):
    index = {}
    for obj in objects:
        file_name = obj.get("fileName")
        if not file_name:
            continue
        index[file_name] = obj
    return index


def copy_local_root(src: Path, dst: Path):
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def download_root(session: requests.Session, endpoint: str, obj: dict, dst: Path, timeout: int):
    etag = str(obj.get("ETag", "")).strip('"')
    if not etag:
        raise RuntimeError(f"Missing ETag for {obj.get('fileName')}")

    url = f"{endpoint.rstrip('/')}/download/{quote(etag)}"
    response = session.get(url, stream=True, timeout=timeout)
    response.raise_for_status()

    dst.parent.mkdir(parents=True, exist_ok=True)
    with dst.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                handle.write(chunk)


def make_manifest_serializable(requested, recovered, missing, qcdb_misses):
    manifest = {
        "requested_root_files": len(requested),
        "recovered_count": len(recovered),
        "missing_count": len(missing),
        "qcdb_metadata_misses": sorted(qcdb_misses),
        "files": {},
    }

    for root_name, info in sorted(requested.items()):
        manifest["files"][root_name] = {
            "categories": sorted(info["categories"]),
            "pngs": info["pngs"],
            "status": "recovered" if root_name in recovered else "missing",
        }

    return manifest


def main():
    args = parse_args()

    categories_dir = Path(args.categories_dir)
    local_root_dir = Path(args.local_root_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not categories_dir.exists():
        raise FileNotFoundError(f"Categories directory does not exist: {categories_dir}")

    requested = collect_requested_roots(categories_dir)
    if not requested:
        raise RuntimeError(f"No PNG files found under {categories_dir}")

    session = requests.Session()

    recovered = set()
    missing = []
    qcdb_misses = set()
    qcdb_index = None

    for root_name in sorted(requested):
        local_src = local_root_dir / root_name
        dst = output_dir / root_name

        if local_src.exists():
            copy_local_root(local_src, dst)
            recovered.add(root_name)
            continue

        if args.skip_download:
            missing.append(root_name)
            continue

        if qcdb_index is None:
            qcdb_index = build_qcdb_index(
                browse_qcdb_metadata(
                    session=session,
                    endpoint=args.qcdb_endpoint,
                    qcdb_path=args.qcdb_path,
                    timeout=args.timeout,
                )
            )

        obj = qcdb_index.get(root_name)
        if obj is None:
            qcdb_misses.add(root_name)
            missing.append(root_name)
            continue

        download_root(
            session=session,
            endpoint=args.qcdb_endpoint,
            obj=obj,
            dst=dst,
            timeout=args.timeout,
        )
        recovered.add(root_name)

    manifest = make_manifest_serializable(requested, recovered, missing, qcdb_misses)
    manifest_path = output_dir / args.manifest_name
    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)

    print(f"Requested ROOT files: {len(requested)}")
    print(f"Recovered ROOT files: {len(recovered)}")
    print(f"Missing ROOT files:   {len(missing)}")
    print(f"Manifest written to:  {manifest_path}")

    if missing:
        print("Still missing:")
        for root_name in missing[:50]:
            print(f"  - {root_name}")
        if len(missing) > 50:
            print(f"  ... and {len(missing) - 50} more")


if __name__ == "__main__":
    main()
