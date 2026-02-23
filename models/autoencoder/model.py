import os
import csv
import json
import time
import random
import numpy as np
import torch
import torch.nn as nn

class LinearAE(nn.Module):
    def __init__(self, latent_dim=64, image_size=(330, 330), channels=3, hidden_dim=512):
        super().__init__()
        h, w = image_size
        input_dim = h * w * channels

        self.encoder = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, latent_dim)
        )
        self.decoder = nn.Sequential(
            nn.Linear(latent_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, input_dim),
            nn.Sigmoid()
        )

        self.image_size = image_size
        self.channels = channels

    def forward(self, x):
        b = x.size(0)
        x = x.view(b, -1)  # (B,3,H,W)->(B,H*W*3)
        z = self.encoder(x)
        out = self.decoder(z)
        h, w = self.image_size
        return out.view(b, self.channels, h, w)
    
class ConvAE(nn.Module):
    def __init__(self):
        super().__init__()

        self.encoder = nn.Sequential(
            nn.Conv2d(3, 16, 3, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(2),
            nn.Conv2d(16, 8, 3, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(2),
        )

        self.decoder = nn.Sequential(
            nn.Conv2d(8, 16, 3, padding=1),
            nn.ReLU(),
            nn.Conv2d(16, 3, 3, padding=1),
            nn.Sigmoid(),
        )

    def forward(self, x):
        h, w = x.shape[-2:]

        x = self.encoder(x)
        x = nn.functional.interpolate(x, size=(h, w), mode="bilinear", align_corners=False)
        x = self.decoder(x)
        return x

class ConvAE_Strided(nn.Module):
    def __init__(
        self,
        in_channels=3,
        base_channels=165,
        conv=None,
        deconv=None,
        use_bn=False,                  
    ):
        super().__init__()

        conv = conv or {}
        deconv = deconv or {}

        # Encoder conv params
        k = conv.get("kernel_size", 3)
        s = conv.get("stride", 2)
        p = conv.get("padding", 1)

        # Decoder deconv params
        dk = deconv.get("kernel_size", k)
        ds = deconv.get("stride", s)
        dp = deconv.get("padding", p)

        in_ch = in_channels
        base = base_channels

        self.enc = nn.Sequential(
            nn.Conv2d(in_ch, base, k, s, p),
            nn.LeakyReLU(inplace=True),

            nn.Conv2d(base, base*2, k, s, p),
            nn.LeakyReLU(inplace=True),

            nn.Conv2d(base*2, base*4, k, s, p),
            nn.LeakyReLU(inplace=True),

            nn.Conv2d(base*4, base*8, k, s, p),
            nn.LeakyReLU(inplace=True),
        )

        out_pads = deconv.get("out_pads", p)

        self.dec = nn.Sequential(
            nn.ConvTranspose2d(base*8, base*4, dk, ds, dp, output_padding=out_pads[0]),
            nn.ReLU(inplace=True),

            nn.ConvTranspose2d(base*4, base*2, dk, ds, dp, output_padding=out_pads[1]),
            nn.ReLU(inplace=True),

            nn.ConvTranspose2d(base*2, base, dk, ds, dp, output_padding=out_pads[2]),
            nn.ReLU(inplace=True),

            nn.ConvTranspose2d(base, in_ch, dk, ds, dp, output_padding=out_pads[3]),
            nn.Sigmoid(),
        )

    def forward(self, x):
        return self.dec(self.enc(x))
    