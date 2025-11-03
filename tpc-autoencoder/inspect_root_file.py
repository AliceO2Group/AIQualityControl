#!/usr/bin/env python3
import sys
import ROOT

def inspect_root_file(path):
    """Inspect and visualize the contents of a ROOT file."""
    f = ROOT.TFile.Open(path)
    if not f or f.IsZombie():
        print(f"❌ Could not open ROOT file: {path}")
        return

    print(f"✅ Opened: {path}")
    print("\n📦 Listing contents:")
    f.ls()

    keys = f.GetListOfKeys()
    if not keys or keys.GetSize() == 0:
        print("⚠️ No objects found in file.")
        return

    # Inspect each object
    for i, key in enumerate(keys):
        obj_name = key.GetName()
        obj_class = key.GetClassName()
        print(f"\n[{i+1}] Object: {obj_name}  |  Type: {obj_class}")

        obj = key.ReadObj()
        
        # Handle trees
        if obj.InheritsFrom("TTree"):
            print(f"  → {obj_name} is a TTree with {obj.GetEntries()} entries")
            print("  Branches:")
            obj.Print()
            visualize_tree(obj)

        # Handle histograms (1D, 2D, etc.)
        elif obj.InheritsFrom("TH1"):
            print(f"  → {obj_name} is a histogram")
            visualize_hist(obj)

        # Directories or others
        elif obj.InheritsFrom("TDirectory"):
            print(f"  → {obj_name} is a subdirectory")
            obj.ls()

        else:
            print(f"  → Unknown object type: {obj_class}")

    f.Close()


def visualize_hist(hist):
    """Draw a ROOT histogram."""
    c = ROOT.TCanvas("c", "Histogram", 800, 600)
    hist.Draw()
    c.Update()
    input("Press Enter to close histogram...")  # keep window open


def visualize_tree(tree):
    """Draw a variable from a ROOT TTree."""
    c = ROOT.TCanvas("c", "TTree Viewer", 800, 600)
    branch_name = tree.GetListOfBranches().At(0).GetName()
    print(f"  → Plotting first branch: {branch_name}")
    tree.Draw(branch_name)
    c.Update()
    input("Press Enter to close tree plot...")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python inspect_root_file.py <file.root>")
        sys.exit(1)

    path = sys.argv[1]
    inspect_root_file(path)
