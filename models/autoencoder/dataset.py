import os 
from torch.utils.data import Dataset
from torchvision import transforms
from utils import *
from PIL import Image
from typing import Optional, Tuple, List
from matplotlib.image import imread


class QcdbImageDataset(Dataset):
    def __init__(self, folder, limit=None, image_size=None):
        
        if image_size: 
            self.transform = transforms.Compose([
                transforms.Resize(image_size),
                transforms.ToTensor()  # (C,H,W) in [0,1]
            ])
        else: 
            self.transform = transforms.ToTensor()  # (C,H,W) in [0,1]
            
        paths = [
            os.path.join(folder, f)
            for f in os.listdir(folder)
            if f.lower().endswith((".png", ".jpg", ".jpeg"))
        ]
        
        if limit is not None:
            self.paths = paths[:limit]
        else: 
            self.paths = paths

    def __len__(self):
        return len(self.paths)

    def __getitem__(self, idx):
        img = Image.open(self.paths[idx]).convert("RGB")
        return self.transform(img)


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
        
class QcdbNpyFakeTensorDataset(Dataset):
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
        