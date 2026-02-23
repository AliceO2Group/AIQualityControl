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
        
        h = f.Get("ccdb_object") 
        if not h:
            raise SystemExit("Histogram not found. Pick a name printed in the previous cell.")

        tcanvas_prim_list = h.GetListOfPrimitives() #  { @0x16dcf0f78, @0x16dcf0f78, @0x16dcf0f78, @0x16dcf0f78 }
        
        tensors = []        

        for _, pad in enumerate(tcanvas_prim_list):
            
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
            
if __name__ == "__main__": 
    root_files_folder = "/Users/zetasourpi/Desktop/GitRepoQC/AIQualityControl/data-ingestion/good_run_tpc_qual"
    convert_root_files_to_tensors(root_files_folder, os.path.join(root_files_folder, 'tensor'))
    
