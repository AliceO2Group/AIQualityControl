"""Test the online pipeline with a local .root file — no QCDB access needed.

Usage:
    python test_online_pipeline.py path/to/file.root
    python test_online_pipeline.py path/to/folder/     # picks first .root found
"""
import sys
import torch
import matplotlib.pyplot as plt
from pathlib import Path

from online_data_preparation import root_to_images, image_to_tensor, TEMP_DIR
from online_inference_conv_classifier import LABEL_TO_FOLDER, coarse_name
from online_publishing import combine_predictions, write_prediction_root_file
from utils import load_yaml

# --- config ---
config = load_yaml("params.yaml")
cfg = config["online_inference"]
image_size = tuple(cfg.get("image_size", [330, 330]))
pad_indices = tuple(cfg.get("pad_indices", [0, 1]))

# --- resolve root file ---
if len(sys.argv) < 2:
    raise SystemExit("Usage: python test_online_pipeline.py <path/to/file.root or folder>")
path = Path(sys.argv[1])
if path.is_dir():
    files = sorted(path.glob("*.root"))
    if not files:
        raise FileNotFoundError(f"No .root files found in {path}")
    path = files[0]
print(f"Root file: {path}")

# --- ROOT -> temp PNGs -> tensors ---
print("Rendering pads...")
png_paths = root_to_images(str(path), pad_indices=pad_indices, image_size=image_size)
tensors = [image_to_tensor(p, image_size) for p in png_paths]
print(f"  Tensors: {[tuple(t.shape) for t in tensors]}")

# --- load models ---
device = torch.device("mps" if torch.backends.mps.is_available() else "cuda" if torch.cuda.is_available() else "cpu")
print(f"Device: {device}")
ae_model = torch.load(cfg["ae_model_path"], map_location=device, weights_only=False).eval()
for p in ae_model.parameters():
    p.requires_grad = False
classifier = torch.load(cfg["classifier_model_path"], map_location=device, weights_only=False).eval()

# --- inference ---
predictions, loss_maps = [], []
for pad_idx, tensor in zip(pad_indices, tensors):
    img = tensor.unsqueeze(0).to(device)
    with torch.no_grad():
        recon = ae_model(img)
        loss_map = (img - recon) ** 2
        fine_label = int(torch.argmax(classifier(loss_map), dim=1).item())
    predictions.append({"pad_index": pad_idx, "fine_label": fine_label,
                         "fine_name": LABEL_TO_FOLDER[fine_label], "coarse_name": coarse_name(fine_label)})
    loss_maps.append(loss_map.squeeze(0).mean(0).cpu().numpy())
    print(f"  Pad {pad_idx}: {LABEL_TO_FOLDER[fine_label]} → {coarse_name(fine_label)}")

combined = combine_predictions(predictions)
label = f"{combined['coarse_name']} ({combined['fine_name']})"
print(f"\nFinal prediction: {label}")

write_prediction_root_file(combined, cfg.get("publish_root_file", "inference_predictions/online/prediction.root"))

# --- plot ---
n = len(tensors)
fig, axes = plt.subplots(2, n, figsize=(6 * n, 10))
if n == 1:
    axes = axes.reshape(2, 1)
for i, (tensor, pred, lm) in enumerate(zip(tensors, predictions, loss_maps)):
    axes[0, i].imshow(tensor.permute(1, 2, 0).numpy())
    axes[0, i].set_title(f"Pad {pred['pad_index']}: {pred['fine_name']} ({pred['coarse_name']})", fontsize=12)
    axes[0, i].axis("off")
    im = axes[1, i].imshow(lm, cmap="hot")
    axes[1, i].set_title("Loss map")
    axes[1, i].axis("off")
    fig.colorbar(im, ax=axes[1, i], fraction=0.046)

fig.suptitle(f"Prediction: {label}\n{path.name}", fontsize=14)
plt.tight_layout()
out_fig = TEMP_DIR / "test_prediction.png"
plt.savefig(out_fig, dpi=150, bbox_inches="tight")
plt.show()
print(f"Figure saved -> {out_fig}")
