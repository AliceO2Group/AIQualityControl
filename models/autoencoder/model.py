import os
import csv
import json
import time
import random
import numpy as np
import torch
import torch.nn as nn

class LinearAE(nn.Module):
    def __init__(self, latent_dim=64, image_size=(128, 128), channels=3, hidden_dim=512):
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
    def __init__(self, latent_dim=64, image_size=(128, 128), channels=3, hidden_dim=512):
        super().__init__()
