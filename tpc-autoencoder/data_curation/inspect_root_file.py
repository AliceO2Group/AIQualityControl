#!/usr/bin/env python3
"""Inspect a ROOT file and print object contents plus useful drawing options."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

try:
    import ROOT
except Exception as exc:
    raise SystemExit(
        "PyROOT import failed. Make sure your environment has ROOT installed.\n"
        f"{exc}"
    )


COMMON_DRAW_OPTIONS = {
    "TH1": [
        "HIST",
        "E",
        "E1",
        "E SAME",
        "P",
        "L",
        "BAR",
        "TEXT",
    ],
    "TH2": [
        "COL",
        "COLZ",
        "COLZ0",
        "TEXT",
        "TEXT SAME",
        "BOX",
        "BOX SAME",
        "CONT",
        "CONT1",
        "CONT4",
        "LEGO",
        "LEGO2",
        "SURF",
        "SURF1",
        "ARR",
        "SCAT",
    ],
    "TH3": [
        "BOX",
        "ISO",
        "LEGO",
        "GLBOX",
        "GLISO",
    ],
    "TGraph": [
        "AP",
        "AL",
        "APL",
        "P",
        "L",
        "C",
        "AC",
    ],
}


PALETTE_HINTS = [
    "kBird",
    "kRainBow",
    "kViridis",
    "kGreyScale",
    "kDeepSea",
    "kDarkBodyRadiator",
    "kBlueYellow",
    "kCividis",
]


def iter_keys(directory: ROOT.TDirectory) -> Iterable[ROOT.TKey]:
    keys = directory.GetListOfKeys()
    if not keys:
        return []
    return [keys.At(index) for index in range(keys.GetSize())]


def fmt(value: float) -> str:
    return f"{value:.6g}"


def describe_axis(axis: ROOT.TAxis, name: str) -> str:
    if not axis:
        return f"{name}: <missing>"
    return (
        f"{name}: bins={axis.GetNbins()}, min={fmt(axis.GetXmin())}, "
        f"max={fmt(axis.GetXmax())}, title='{axis.GetTitle()}'"
    )


def describe_histogram(obj, indent: str) -> None:
    print(f"{indent}entries={fmt(obj.GetEntries())}")
    print(f"{indent}minimum={fmt(obj.GetMinimum())}, maximum={fmt(obj.GetMaximum())}")

    if obj.InheritsFrom("TH2") or obj.InheritsFrom("TProfile2D"):
        stored_min = obj.GetMinimumStored()
        stored_max = obj.GetMaximumStored()
        print(
            f"{indent}stored z-range override="
            f"({fmt(stored_min)}, {fmt(stored_max)})"
        )

    print(f"{indent}{describe_axis(obj.GetXaxis(), 'x-axis')}")
    print(f"{indent}{describe_axis(obj.GetYaxis(), 'y-axis')}")

    get_zaxis = getattr(obj, "GetZaxis", None)
    if callable(get_zaxis):
        print(f"{indent}{describe_axis(get_zaxis(), 'z-axis')}")


def class_draw_options(obj) -> list[str]:
    if obj.InheritsFrom("TH3"):
        return COMMON_DRAW_OPTIONS["TH3"]
    if obj.InheritsFrom("TH2") or obj.InheritsFrom("TProfile2D"):
        return COMMON_DRAW_OPTIONS["TH2"]
    if obj.InheritsFrom("TH1") or obj.InheritsFrom("TProfile"):
        return COMMON_DRAW_OPTIONS["TH1"]
    if obj.InheritsFrom("TGraph"):
        return COMMON_DRAW_OPTIONS["TGraph"]
    return []


def print_draw_help(obj, indent: str) -> None:
    options = class_draw_options(obj)
    if options:
        print(f"{indent}common draw/paint options: {', '.join(options)}")

    if obj.InheritsFrom("TH2") or obj.InheritsFrom("TProfile2D"):
        print(
            f"{indent}color reading help: use COL/COLZ for heatmaps, "
            "TEXT to print bin values, CONT for contours."
        )
        print(
            f"{indent}palette ideas: {', '.join(PALETTE_HINTS)} "
            "(for example: ROOT.gStyle.SetPalette(ROOT.kViridis))"
        )
        print(
            f"{indent}range control: obj.SetMinimum(min_value), "
            "obj.SetMaximum(max_value)"
        )


def inspect_primitive(obj, path: str, indent: str = "") -> None:
    draw_option = ""
    get_draw_option = getattr(obj, "GetDrawOption", None)
    if callable(get_draw_option):
        draw_option = get_draw_option() or ""

    print(f"{indent}{path}: {obj.ClassName()}")
    if obj.GetName():
        print(f"{indent}name='{obj.GetName()}'")
    if obj.GetTitle():
        print(f"{indent}title='{obj.GetTitle()}'")
    if draw_option:
        print(f"{indent}stored draw option='{draw_option}'")

    if obj.InheritsFrom("TH1") or obj.InheritsFrom("TProfile"):
        describe_histogram(obj, indent + "  ")
        print_draw_help(obj, indent + "  ")

    if obj.InheritsFrom("TCanvas") or obj.InheritsFrom("TPad"):
        primitives = obj.GetListOfPrimitives()
        size = primitives.GetSize() if primitives else 0
        print(f"{indent}contains {size} primitives")
        for index in range(size):
            child = primitives.At(index)
            child_name = child.GetName() or f"primitive_{index}"
            inspect_primitive(
                child,
                f"{path}/{child_name}",
                indent + "  ",
            )


def inspect_directory(directory: ROOT.TDirectory, prefix: str = "") -> None:
    for key in iter_keys(directory):
        obj_name = key.GetName()
        class_name = key.GetClassName()
        obj_path = f"{prefix}/{obj_name}" if prefix else obj_name
        print(f"{obj_path}: {class_name}")

        obj = key.ReadObj()
        if obj.InheritsFrom("TDirectory"):
            inspect_directory(obj, obj_path)
            continue

        inspect_primitive(obj, obj_path, indent="  ")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read a ROOT file, print its contents, and show useful painting "
            "options plus min/max information."
        )
    )
    parser.add_argument("root_file", help="Path to the ROOT file to inspect.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root_path = Path(args.root_file).expanduser().resolve()

    if not root_path.exists():
        raise SystemExit(f"ROOT file does not exist: {root_path}")

    ROOT.gROOT.SetBatch(True)

    root_file = ROOT.TFile.Open(str(root_path), "READ")
    if not root_file or root_file.IsZombie():
        raise SystemExit(f"Failed to open ROOT file: {root_path}")

    print(f"Inspecting: {root_path}")
    print(f"ROOT file title: '{root_file.GetTitle()}'")
    print()

    inspect_directory(root_file)

    print()
    print("General color/min-max tips:")
    print("  - For 2D histograms use 'COLZ' to show the color bar.")
    print("  - Use SetMinimum/SetMaximum to lock the visible color scale.")
    print("  - Use 'TEXT SAME' on top of 'COLZ' when you want both colors and values.")
    print("  - If colors look misleading, try another palette such as kViridis or kCividis.")

    root_file.Close()


if __name__ == "__main__":
    main()
