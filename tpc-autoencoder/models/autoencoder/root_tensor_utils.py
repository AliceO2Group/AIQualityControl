#!/usr/bin/env python3
"""Utilities to convert ROOT TH2 images to tensors and back."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import matplotlib.image as mpimg
import matplotlib.pyplot as plt
import numpy as np
import torch

try:
    import ROOT
except Exception as exc:
    raise SystemExit(
        "PyROOT import failed. Make sure your environment has ROOT installed.\n"
        f"{exc}"
    )


ROOT.gROOT.SetBatch(True)


@dataclass
class HistogramTensorInfo:
    name: str
    title: str
    pad_name: str
    tensor: torch.Tensor
    x_bins: int
    y_bins: int
    x_min: float
    x_max: float
    y_min: float
    y_max: float
    value_min: float
    value_max: float
    display_min: float | None
    display_max: float | None


def normalize_tensor_log1p(
    tensor: torch.Tensor,
    max_value: float = 62000.0,
) -> torch.Tensor:
    """Apply log1p normalization to a raw count tensor.

    Values are clamped to [0, max_value] before taking log1p so the output
    is always in [0, 1] — required by the Sigmoid decoder output.
    """
    if max_value <= 0:
        raise ValueError("max_value must be > 0")
    return torch.log1p(tensor.clamp(min=0.0, max=max_value)) / np.log1p(max_value)


def denormalize_tensor_log1p(
    normalized_tensor: torch.Tensor,
    max_value: float = 62000.0,
) -> torch.Tensor:
    """Invert log1p normalization back to raw counts."""
    if max_value <= 0:
        raise ValueError("max_value must be > 0")
    return torch.expm1(normalized_tensor * np.log1p(max_value))


def th2_to_numpy(th2) -> np.ndarray:
    """Convert a ROOT TH2 into a (H, W) NumPy array."""
    nx = th2.GetNbinsX()
    ny = th2.GetNbinsY()
    array = np.zeros((ny, nx), dtype=np.float32)

    for ix in range(1, nx + 1):
        for iy in range(1, ny + 1):
            array[iy - 1, ix - 1] = th2.GetBinContent(ix, iy)

    return array


def th2_to_tensor(th2, add_channel: bool = True) -> torch.Tensor:
    """Convert a ROOT TH2 into a torch tensor."""
    tensor = torch.from_numpy(th2_to_numpy(th2))
    if add_channel:
        tensor = tensor.unsqueeze(0)
    return tensor


def _get_display_bounds(th2) -> tuple[float | None, float | None]:
    min_stored = float(th2.GetMinimumStored())
    max_stored = float(th2.GetMaximumStored())

    has_override = (min_stored != -1111.0) or (max_stored != -1111.0)
    if not has_override:
        return None, None

    return min_stored, max_stored


def _clone_th2(obj):
    clone = obj.Clone(f"{obj.GetName()}_clone")
    clone.SetDirectory(0)
    return clone


def _unique_canvas_name(base_name: str) -> str:
    return f"{base_name}_{uuid4().hex[:8]}"


def list_th2_histograms(root_path: str | Path) -> list:
    """Extract all TH2 histograms from the ccdb_object canvas."""
    root_file = ROOT.TFile.Open(str(Path(root_path).expanduser().resolve()), "READ")
    if not root_file or root_file.IsZombie():
        raise ValueError(f"Failed to open ROOT file: {root_path}")

    canvas = root_file.Get("ccdb_object")
    if not canvas:
        root_file.Close()
        raise ValueError("Object 'ccdb_object' not found in ROOT file.")

    histograms = []
    primitives = canvas.GetListOfPrimitives()

    for pad_index in range(primitives.GetSize()):
        obj = primitives.At(pad_index)

        # Support simple canvases where the TH2 is drawn directly on ccdb_object.
        if obj.InheritsFrom("TH2"):
            histograms.append((canvas.GetName(), _clone_th2(obj)))
            continue

        pad = obj
        if not (pad.InheritsFrom("TPad") or pad.InheritsFrom("TCanvas")):
            continue

        pad_primitives = pad.GetListOfPrimitives()
        if not pad_primitives:
            continue

        for prim_index in range(pad_primitives.GetSize()):
            obj = pad_primitives.At(prim_index)
            if obj.InheritsFrom("TH2"):
                histograms.append((pad.GetName(), _clone_th2(obj)))

    root_file.Close()
    return histograms


def root_to_tensor(
    root_path: str | Path,
    hist_index: int = 0,
    add_channel: bool = True,
) -> torch.Tensor:
    """Return one ROOT TH2 image as a tensor with shape [1, H, W] by default."""
    histograms = list_th2_histograms(root_path)
    if not histograms:
        raise ValueError(f"No TH2 histograms found in ROOT file: {root_path}")
    if hist_index < 0 or hist_index >= len(histograms):
        raise IndexError(
            f"hist_index={hist_index} is out of range. Found {len(histograms)} TH2 histograms."
        )

    _, histogram = histograms[hist_index]
    return th2_to_tensor(histogram, add_channel=add_channel)


def root_to_th2(
    root_path: str | Path,
    hist_index: int = 0,
):
    """Return one TH2 histogram extracted from the ROOT object."""
    histograms = list_th2_histograms(root_path)
    if not histograms:
        raise ValueError(f"No TH2 histograms found in ROOT file: {root_path}")
    if hist_index < 0 or hist_index >= len(histograms):
        raise IndexError(
            f"hist_index={hist_index} is out of range. Found {len(histograms)} TH2 histograms."
        )
    _, histogram = histograms[hist_index]
    return histogram


def root_to_normalized_tensor(
    root_path: str | Path,
    hist_index: int = 0,
    max_value: float = 62000.0,
) -> tuple[torch.Tensor, HistogramTensorInfo]:
    """ROOT object -> TH2 -> tensor -> log1p-normalized tensor."""
    info = extract_histogram_info(root_path, hist_index=hist_index)
    normalized = normalize_tensor_log1p(info.tensor, max_value=max_value)
    return normalized, info


def extract_histogram_info(
    root_path: str | Path,
    hist_index: int = 0,
) -> HistogramTensorInfo:
    """Return tensor plus metadata needed to reconstruct and render it again."""
    histograms = list_th2_histograms(root_path)
    if not histograms:
        raise ValueError(f"No TH2 histograms found in ROOT file: {root_path}")
    if hist_index < 0 or hist_index >= len(histograms):
        raise IndexError(
            f"hist_index={hist_index} is out of range. Found {len(histograms)} TH2 histograms."
        )

    pad_name, histogram = histograms[hist_index]
    x_axis = histogram.GetXaxis()
    y_axis = histogram.GetYaxis()
    display_min, display_max = _get_display_bounds(histogram)

    return HistogramTensorInfo(
        name=histogram.GetName(),
        title=histogram.GetTitle(),
        pad_name=pad_name,
        tensor=th2_to_tensor(histogram, add_channel=True),
        x_bins=x_axis.GetNbins(),
        y_bins=y_axis.GetNbins(),
        x_min=float(x_axis.GetXmin()),
        x_max=float(x_axis.GetXmax()),
        y_min=float(y_axis.GetXmin()),
        y_max=float(y_axis.GetXmax()),
        value_min=float(histogram.GetMinimum()),
        value_max=float(histogram.GetMaximum()),
        display_min=display_min,
        display_max=display_max,
    )


def tensor_to_th2(
    tensor: torch.Tensor | np.ndarray,
    info: HistogramTensorInfo,
    hist_name: str | None = None,
) -> object:
    """Rebuild a ROOT TH2 from a [1, H, W] or [H, W] tensor."""
    if isinstance(tensor, torch.Tensor):
        array = tensor.detach().cpu().numpy()
    else:
        array = np.asarray(tensor)

    if array.ndim == 3:
        if array.shape[0] != 1:
            raise ValueError(f"Expected a single-channel tensor, got shape {array.shape}")
        array = array[0]

    if array.ndim != 2:
        raise ValueError(f"Expected [1,H,W] or [H,W], got shape {array.shape}")

    if array.shape != (info.y_bins, info.x_bins):
        raise ValueError(
            f"Tensor shape {array.shape} does not match ROOT histogram shape "
            f"({info.y_bins}, {info.x_bins})"
        )

    name = hist_name or f"{info.name}_from_tensor"
    hist = ROOT.TH2F(
        name,
        info.title,
        info.x_bins,
        info.x_min,
        info.x_max,
        info.y_bins,
        info.y_min,
        info.y_max,
    )
    hist.SetDirectory(0)

    for ix in range(info.x_bins):
        for iy in range(info.y_bins):
            hist.SetBinContent(ix + 1, iy + 1, float(array[iy, ix]))

    if info.display_min is not None:
        hist.SetMinimum(info.display_min)
    if info.display_max is not None:
        hist.SetMaximum(info.display_max)

    return hist


def normalized_tensor_to_th2(
    normalized_tensor: torch.Tensor | np.ndarray,
    info: HistogramTensorInfo,
    max_value: float = 62000.0,
    hist_name: str | None = None,
):
    """De-normalize a tensor and rebuild the TH2 histogram."""
    if isinstance(normalized_tensor, np.ndarray):
        normalized_tensor = torch.from_numpy(normalized_tensor)

    raw_tensor = denormalize_tensor_log1p(normalized_tensor, max_value=max_value)
    return tensor_to_th2(raw_tensor, info, hist_name=hist_name)


def _apply_pad_style(pad) -> None:
    pad.SetRightMargin(0.15)
    pad.SetLeftMargin(0.11)
    pad.SetBottomMargin(0.11)
    pad.SetTopMargin(0.08)


def th2_to_root_object(
    th2,
    canvas_name: str = "ccdb_object",
    plot: bool = True,
    save_path: str | Path | None = None,
):
    """Wrap a TH2 into a ROOT canvas with the standard kBird palette."""
    canvas = ROOT.TCanvas(_unique_canvas_name(canvas_name), "", 900, 800)
    canvas.SetBatch(not plot)
    ROOT.gStyle.SetOptStat(0)
    ROOT.gStyle.SetPalette(ROOT.kBird)
    _apply_pad_style(canvas)

    # Keep the histogram alive on the Python side so the canvas does not lose it.
    canvas._th2 = th2

    th2.Draw("COLZ")
    canvas.Update()

    if plot:
        canvas.Draw()
        canvas.Update()

    if save_path is not None:
        save_path = Path(save_path)
        save_path.parent.mkdir(parents=True, exist_ok=True)

        if save_path.suffix.lower() == ".root":
            root_file = ROOT.TFile(str(save_path), "RECREATE")
            canvas.Write(canvas_name)
            root_file.Close()
        else:
            canvas.SaveAs(str(save_path))

    return canvas


def normalized_tensor_to_root_object(
    normalized_tensor: torch.Tensor | np.ndarray,
    info: HistogramTensorInfo,
    max_value: float = 62000.0,
    hist_name: str | None = None,
    canvas_name: str = "ccdb_object",
    plot: bool = True,
    save_path: str | Path | None = None,
):
    """De-normalize -> tensor to TH2 -> ROOT canvas."""
    hist = normalized_tensor_to_th2(
        normalized_tensor,
        info,
        max_value=max_value,
        hist_name=hist_name,
    )
    return th2_to_root_object(
        hist,
        canvas_name=canvas_name,
        plot=plot,
        save_path=save_path,
    )


def compute_global_max(
    folder: str | Path,
    hist_indices: tuple[int, ...] = (0, 1),
    limit: int | None = None,
) -> float:
    """Scan ROOT files in *folder* and return the global maximum bin value.

    Opens every file once and calls GetMaximum() on each histogram — fast
    because ROOT caches the max internally.  Use the result as max_value for
    log1p normalization so the Sigmoid decoder output stays in [0, 1].
    """
    folder = Path(folder)
    paths = sorted(
        str(folder / f)
        for f in os.listdir(str(folder))
        if f.lower().endswith(".root") and (folder / f).is_file()
    )
    if limit is not None:
        paths = paths[:limit]

    if not paths:
        raise ValueError(f"No .root files found in {folder}")

    global_max = 0.0
    for path in paths:
        try:
            histograms = list_th2_histograms(path)
            for idx in hist_indices:
                if 0 <= idx < len(histograms):
                    _, hist = histograms[idx]
                    val = float(hist.GetMaximum())
                    if val > global_max:
                        global_max = val
        except Exception:
            continue

    if global_max == 0.0:
        raise ValueError(f"All histograms in {folder} have a maximum of zero.")

    return global_max


def save_root_comparison(
    orig_norm: torch.Tensor,
    recon_norm: torch.Tensor,
    info: HistogramTensorInfo,
    max_value: float,
    out_path: str | Path,
) -> None:
    """Save a 3-panel ROOT canvas: Original | Reconstructed | Residual.

    Panels 1 and 2 use the same z-range and style as the original ROOT object
    (taken from info.display_min/max).  Panel 3 shows the residual with a
    symmetric z-axis around zero, same kBird palette.
    """
    orig_raw = denormalize_tensor_log1p(orig_norm, max_value=max_value)
    recon_raw = denormalize_tensor_log1p(recon_norm, max_value=max_value)

    th2_orig = tensor_to_th2(orig_raw, info, hist_name=f"{info.name}_orig")
    th2_reco = tensor_to_th2(recon_raw, info, hist_name=f"{info.name}_reco")

    th2_diff = th2_orig.Clone(f"{info.name}_diff")
    th2_diff.SetDirectory(0)
    th2_diff.Add(th2_reco, -1.0)

    # Symmetric z-axis for residual derived from actual bin contents
    diff_arr = th2_to_numpy(th2_diff)
    abs_max = float(max(abs(diff_arr.min()), abs(diff_arr.max()))) or 1.0
    th2_diff.SetMinimum(-abs_max)
    th2_diff.SetMaximum(abs_max)

    ROOT.gStyle.SetOptStat(0)
    ROOT.gStyle.SetPalette(ROOT.kBird)
    canvas = ROOT.TCanvas(_unique_canvas_name("cmp"), "", 2700, 800)
    canvas.Divide(3, 1)

    pad1 = canvas.cd(1)
    _apply_pad_style(pad1)
    th2_orig.SetTitle(f"{info.name} — Original")
    th2_orig.Draw("COLZ")
    pad1.Update()

    pad2 = canvas.cd(2)
    _apply_pad_style(pad2)
    th2_reco.SetTitle(f"{info.name} — Reconstructed")
    th2_reco.Draw("COLZ")
    pad2.Update()

    pad3 = canvas.cd(3)
    _apply_pad_style(pad3)
    th2_diff.SetTitle(f"{info.name} — Residual (orig #minus reco)")
    th2_diff.Draw("COLZ")
    pad3.Update()

    canvas.Update()

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    canvas.SaveAs(str(out_path.with_suffix(".png")))
    root_file = ROOT.TFile(str(out_path.with_suffix(".root")), "RECREATE")
    canvas.Write("comparison")
    root_file.Close()
    canvas.Close()


def _render_histogram_png(hist, out_path: str | Path, draw_option: str = "COLZ") -> Path:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    canvas = ROOT.TCanvas(f"c_{hist.GetName()}", "", 900, 800)
    ROOT.gStyle.SetOptStat(0)
    canvas.SetRightMargin(0.15)
    canvas.SetLeftMargin(0.11)
    canvas.SetBottomMargin(0.11)
    canvas.SetTopMargin(0.08)

    hist.Draw(draw_option)
    canvas.Update()
    canvas.SaveAs(str(out_path))
    canvas.Close()
    return out_path


def save_roundtrip_comparison(
    root_path: str | Path,
    out_dir: str | Path,
    hist_index: int = 0,
) -> dict[str, Path]:
    """
    Render both:
    - original ROOT histogram -> image
    - ROOT histogram -> tensor -> ROOT histogram -> image
    """
    out_dir = Path(out_dir)
    info = extract_histogram_info(root_path, hist_index=hist_index)
    histograms = list_th2_histograms(root_path)
    _, original_hist = histograms[hist_index]
    rebuilt_hist = tensor_to_th2(info.tensor, info)

    original_png = _render_histogram_png( # render the object
        original_hist,
        out_dir / f"{info.name}_original.png",
    )
    rebuilt_png = _render_histogram_png(
        rebuilt_hist,
        out_dir / f"{info.name}_roundtrip.png",
    )

    comparison_png = out_dir / f"{info.name}_comparison.png"
    fig, axes = plt.subplots(1, 2, figsize=(12, 5), constrained_layout=True)
    axes[0].imshow(mpimg.imread(original_png))
    axes[0].set_title("Input: ROOT -> image")
    axes[0].axis("off")

    axes[1].imshow(mpimg.imread(rebuilt_png))
    axes[1].set_title("Output: ROOT -> tensor -> image")
    axes[1].axis("off")

    fig.savefig(comparison_png, dpi=150, bbox_inches="tight")
    plt.close(fig)

    return {
        "original": original_png,
        "roundtrip": rebuilt_png,
        "comparison": comparison_png,
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Convert one ROOT TH2 image into a [1,H,W] tensor and render a "
            "ROOT-vs-tensor roundtrip comparison."
        )
    )
    parser.add_argument("root_file", help="Path to the ROOT file.")
    parser.add_argument(
        "--hist-index",
        type=int,
        default=0,
        help="Which TH2 histogram to read from the ROOT file.",
    )
    parser.add_argument(
        "--out-dir",
        default="data_curation/root_tensor_previews",
        help="Directory where preview PNGs will be written.",
    )
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    info = extract_histogram_info(args.root_file, hist_index=args.hist_index)
    print(f"histogram: {info.name}")
    print(f"pad: {info.pad_name}")
    print(f"tensor shape: {tuple(info.tensor.shape)}")
    print(
        f"value range: min={info.value_min:.6g}, max={info.value_max:.6g}, "
        f"display_range=({info.display_min}, {info.display_max})"
    )

    outputs = save_roundtrip_comparison(
        args.root_file,
        args.out_dir,
        hist_index=args.hist_index,
    )
    print(f"saved original image: {outputs['original']}")
    print(f"saved roundtrip image: {outputs['roundtrip']}")
    print(f"saved side-by-side comparison: {outputs['comparison']}")


if __name__ == "__main__":
    main()
