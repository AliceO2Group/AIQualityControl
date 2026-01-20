import os 
from torch.utils.data import Dataset
from torchvision import transforms
from models.autoencoder.src.utils import *
from PIL import Image

class QcdbImageDataset(Dataset):
    def __init__(self, folder, limit, image_size):
        self.transform = transforms.Compose([
            transforms.Resize(image_size),
            transforms.ToTensor()  # (C,H,W) in [0,1]
        ])
        paths = [
            os.path.join(folder, f)
            for f in os.listdir(folder)
            if f.lower().endswith((".png", ".jpg", ".jpeg"))
        ]
        
        self.paths = paths[:limit]

    def __len__(self):
        return len(self.paths)

    def __getitem__(self, idx):
        img = Image.open(self.paths[idx]).convert("RGB")
        return self.transform(img)

