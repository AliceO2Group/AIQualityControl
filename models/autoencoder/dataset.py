import os
import sys
from pathlib import Path

import numpy as np
import torch
from torch.utils.data import Dataset
from torchvision import transforms
from tqdm import tqdm
from utils import *
from PIL import Image
from typing import Optional, Tuple, List
from matplotlib.image import imread

ROOT_TENSOR_UTILS_DIR = Path(__file__).resolve().parents[2] / "data_curation"
if str(ROOT_TENSOR_UTILS_DIR) not in sys.path:
    sys.path.append(str(ROOT_TENSOR_UTILS_DIR))

from root_tensor_utils import root_to_normalized_tensor, root_to_tensor
from utils import root_to_pil_image

class LabeledDataset(torch.utils.data.Dataset):
    """
    Wrap a dataset and force a fixed label for all its samples.
    """
    def __init__(self, base_dataset, label, transform=None):
        self.base_dataset = base_dataset
        self.label = label
        self.transform = transform

    def __len__(self):
        return len(self.base_dataset)

    def __getitem__(self, idx):
        x = self.base_dataset[idx]
        if isinstance(x, (tuple, list)):
            x = x[0]
        if self.transform is not None:
            x = self.transform(x)
        return x, self.label


class QcdbImageDataset(Dataset):
    def __init__(self, folder, limit=None, image_size=None, root_pad_index=0, root_grey_scale=False):
        self.root_pad_index = root_pad_index
        self.root_grey_scale = root_grey_scale
        self.image_size = image_size  # (H, W) or int

        if image_size:
            self.transform = transforms.Compose([
                transforms.Resize(image_size),
                transforms.ToTensor()  # (C,H,W) in [0,1]
            ])
        else:
            self.transform = transforms.ToTensor()  # (C,H,W) in [0,1]

        paths = sorted(
            os.path.join(folder, f)
            for f in os.listdir(folder)
            if f.lower().endswith((".png", ".jpg", ".jpeg", ".root"))
        )

        if limit is not None:
            self.paths = paths[:limit]
        else:
            self.paths = paths

    def __len__(self):
        return len(self.paths)

    def __getitem__(self, idx):
        path = self.paths[idx]
        if path.lower().endswith(".root"):
            if self.image_size is not None:
                s = self.image_size
                w, h = (s, s) if isinstance(s, int) else (s[1], s[0])
            else:
                w, h = 330, 330
            img = root_to_pil_image(path, pad_index=self.root_pad_index, grey_scale=self.root_grey_scale, W=w, H=h)
        else:
            img = Image.open(path)
        return self.transform(img.convert("RGB"))


class QcdbNpyTensorDataset(Dataset):
    def __init__(
        self,
        folder: str,
        limit: Optional[int] = None,
        add_channel: bool = True,
        log1p: bool = False,
        normalize: Optional[str] = None,  # "minmax" or "zscore"
    ):
        
        self.add_channel = add_channel
        self.log1p = log1p
        self.normalize = normalize

        self.paths: List[str] = sorted(
            os.path.join(folder, f)
            for f in os.listdir(folder)
            if f.lower().endswith(".npz")
            and os.path.isfile(os.path.join(folder, f))
        )

        if limit is not None:
            self.paths = self.paths[:limit]

    def __len__(self) -> int:
        return len(self.paths)

    def __getitem__(self, idx: int) -> torch.Tensor:
        
        npz = np.load(self.paths[idx])
        x = npz["data"][0] #(H, W) numpy array  
        x = np.log1p(x)/14

        t = torch.from_numpy(x).unsqueeze(0) # (1, H, W) tensor
        
        # scale_t = torch.quantile(t.flatten(), 1).clamp_min(1e-8)  
            
        # t_norm = t / scale_t # (1, H, W)
        # scale_map = torch.log(scale_t).expand_as(t_norm)   # (1, H, W)
        
        # t_in = torch.cat([t_norm,scale_map], dim=0)  # (2, H, W)
        
        return t
        
class QcdbNpyTensorDataset(Dataset):
    def __init__(
        self,
        folder: str,
        limit: Optional[int] = None,
        add_channel: bool = True,
        log1p: bool = False,
        normalize: Optional[str] = None,  # "minmax" or "zscore"
    ):
        
        self.add_channel = add_channel
        self.log1p = log1p
        self.normalize = normalize

        self.paths: List[str] = sorted(
            os.path.join(folder, f)
            for f in os.listdir(folder)
            if f.lower().endswith(".png")
            and os.path.isfile(os.path.join(folder, f))
        )

        if limit is not None:
            self.paths = self.paths[:limit]

    def __len__(self) -> int:
        return len(self.paths)

    def __getitem__(self, idx: int) -> torch.Tensor:
        
        x = np.array(imread(self.paths[idx]))[:,:,0]
        # print(x.max(),x.min())
        #x = npz["data"][0]/65e3 # (H, W) numpy array  
        x = np.log1p(x)/14
        if self.log1p:
            x = np.log1p(x)

        if self.normalize == "minmax":
            mn, mx = float(x.min()), float(x.max())
            x = (x - mn) / (mx - mn + 1e-8)
        elif self.normalize == "zscore":
            mu, sd = float(x.mean()), float(x.std())
            x = (x - mu) / (sd + 1e-8)

        t = torch.from_numpy(x)  # (H, W) now tensor 
        if self.add_channel:
            t = t.unsqueeze(0)   # (1, H, W)

        return t


class QcdbRootTensorDataset(Dataset):
    def __init__(
        self,
        folder: str,
        mask_path: Optional[str] = None,
        limit: Optional[int] = 10,
        hist_indices: Tuple[int, ...] = (0, 1),
        max_value: float = 62000.0,
        augment_rot90: bool = False,
        augment_hflip: bool = False,
        augment_vflip: bool = False,
        return_metadata: bool = True,
        cache_in_memory: bool = True,
    ):
        if isinstance(limit, str):
            normalized_limit = limit.strip().lower()
            if normalized_limit in {"none", "null", "~", ""}:
                limit = None
            else:
                limit = int(limit)

        self.max_value = max_value
        self.return_metadata = return_metadata
        self.augment_rot90 = augment_rot90
        self.augment_hflip = augment_hflip
        self.augment_vflip = augment_vflip
        self.samples: List[Tuple[str, int]] = []
        self.mask = None
        self.mask_shape = None

        if isinstance(mask_path, str):
            normalized_mask_path = mask_path.strip().lower()
            if normalized_mask_path in {"none", "null", "~", ""}:
                mask_path = None

        if mask_path is not None:
            self.mask = root_to_tensor(mask_path, hist_index=0).squeeze(0).to(torch.bool)
            self.mask_shape = tuple(self.mask.shape)

        root_paths = sorted(
            os.path.join(folder, f)
            for f in os.listdir(folder)
            if f.lower().endswith(".root")
            and os.path.isfile(os.path.join(folder, f))
        )

        if limit is not None:
            root_paths = root_paths[:limit]

        for path in root_paths:
            for hist_index in hist_indices:
                self.samples.append((path, hist_index))

        # Pre-load all tensors once so __getitem__ never touches ROOT files during training.
        # Each [1,330,330] float32 tensor is ~436 KB; 4248 samples ≈ 1.8 GB RAM.
        self._cache: Optional[List] = None
        if cache_in_memory:
            print(f"Caching {len(self.samples)} tensors in memory (one-time ROOT read)...")
            self._cache = []
            for path, hist_index in tqdm(self.samples, unit="hist"):
                tensor, metadata = root_to_normalized_tensor(
                    path,
                    hist_index=hist_index,
                    max_value=self.max_value,
                )
                tensor = tensor.to(dtype=torch.float32)
                sample_info = {
                    "path": path,
                    "hist_index": hist_index,
                    "hist_name": metadata.name,
                    "pad_name": metadata.pad_name,
                    "display_min": metadata.display_min,
                    "display_max": metadata.display_max,
                    "max_value": self.max_value,
                }
                self._cache.append((tensor, sample_info))
            print(f"Cache ready: {len(self._cache)} tensors loaded.")

    def __len__(self) -> int:
        return len(self.samples)

    def _augment(self, tensor: torch.Tensor) -> torch.Tensor:
        if self.augment_rot90:
            k = int(torch.randint(0, 4, (1,)).item())
            tensor = torch.rot90(tensor, k, dims=[-2, -1])
        if self.augment_hflip and torch.rand(1).item() > 0.5:
            tensor = torch.flip(tensor, dims=[-1])
        if self.augment_vflip and torch.rand(1).item() > 0.5:
            tensor = torch.flip(tensor, dims=[-2])
        return tensor

    def __getitem__(self, idx: int):
        if self._cache is not None:
            tensor, sample_info = self._cache[idx]
            tensor = self._augment(tensor)
            if self.return_metadata:
                return tensor, sample_info
            return tensor

        # Fallback: read from ROOT on demand (slow, used when cache_in_memory=False)
        path, hist_index = self.samples[idx]
        tensor, metadata = root_to_normalized_tensor(
            path,
            hist_index=hist_index,
            max_value=self.max_value,
        )
        tensor = self._augment(tensor.to(dtype=torch.float32))

        if self.return_metadata:
            sample_info = {
                "path": path,
                "hist_index": hist_index,
                "hist_name": metadata.name,
                "pad_name": metadata.pad_name,
                "display_min": metadata.display_min,
                "display_max": metadata.display_max,
                "max_value": self.max_value,
            }
            return tensor, sample_info

        return tensor
