import sys
from pathlib import Path

import torch
from torchvision import transforms

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "data_curation" / "postgres_db"))
from sync_qcdb_checks import Ccdb, save_response_to_file
from utils import root_to_pil_image

TEMP_DIR = Path(__file__).parent / ".temp_online_images"
TEMP_DIR.mkdir(exist_ok=True)


def fetch_latest(ccdb_url: str, qc_path: str) -> str:
    """Download the latest ROOT object for qc_path. Returns local file path."""
    ccdb = Ccdb(ccdb_url)
    versions = ccdb.get_versions_list(qc_path)
    if not versions:
        raise RuntimeError(f"No versions found: {qc_path}")
    latest = max(versions, key=lambda v: v.created_at)
    return save_response_to_file(ccdb.download_version(latest), str(TEMP_DIR), fallback_name="latest.root")


def root_to_images(root_path: str, pad_indices=(0, 1), image_size=(330, 330)) -> list:
    """Render histogram pads from a local ROOT file to PNGs. Returns list of Path objects."""
    paths = []
    for i in pad_indices:
        pil = root_to_pil_image(root_path, pad_index=i, grey_scale=False, W=image_size[1], H=image_size[0])
        out = TEMP_DIR / f"pad_{i}.png"
        pil.save(out)
        paths.append(out)
    return paths


def image_to_tensor(png_path, image_size=(330, 330)) -> torch.Tensor:
    """Load a PNG file and return a [3,H,W] float tensor."""
    from PIL import Image
    tfm = transforms.Compose([transforms.Resize(image_size), transforms.ToTensor()])
    return tfm(Image.open(png_path).convert("RGB"))


def prepare_tensors(ccdb_url: str, qc_path: str, image_size=(330, 330), pad_indices=(0, 1)) -> list:
    """Fetch ROOT file, render histogram pads to images, return [3,H,W] tensors."""
    root_path = fetch_latest(ccdb_url, qc_path)
    tfm = transforms.Compose([transforms.Resize(image_size), transforms.ToTensor()])
    tensors = []
    for i in pad_indices:
        pil = root_to_pil_image(root_path, pad_index=i, grey_scale=False, W=image_size[1], H=image_size[0])
        pil.save(TEMP_DIR / f"pad_{i}.png")   # fixed name — overwritten on every call
        tensors.append(tfm(pil.convert("RGB")))
    return tensors
