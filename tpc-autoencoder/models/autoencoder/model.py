import os
import csv
import json
import time
import random
import numpy as np
import torch
import torch.nn as nn
from typing import List, Optional
import torch.nn.functional as F

import torch
import torch.nn as nn
import torch.nn.functional as F

import torch
import torch.nn as nn
import torch.nn.functional as F
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader


# class LossMapClassifier(nn.Module):
#     def __init__(self, channels=3, image_size=(330, 330), num_classes=4):
#         super().__init__()
#         h, w = image_size
#         input_dim = h * w * channels
        
#         self.classification_head = nn.Sequential(
#             nn.Linear(input_dim, 512),
#             nn.ReLU(),
#             nn.Dropout(0.4),

#             nn.Linear(512, 256),
#             nn.ReLU(),
#             nn.Dropout(0.4),
            
#             nn.Linear(256, 128),
#             nn.ReLU(),
#             nn.Dropout(0.3),
            
#             nn.Linear(128, num_classes)
#         )

#     def forward(self, loss_map):
#         x = loss_map.view(loss_map.size(0), -1)
#         logits = self.classification_head(x)
#         return logits
    
    

class LossMapClassifier(nn.Module):
    def __init__(self, channels=3, num_classes=6, base_channels=32, dropout=0.5):
        super().__init__()

        def _conv_block(in_c, out_c):
            return nn.Sequential(
                nn.Conv2d(in_c, out_c, kernel_size=3, stride=2, padding=1),
                nn.GroupNorm(num_groups=min(8, out_c), num_channels=out_c),
                #nn.BatchNorm2d(out_c),
                nn.GELU(),
            )

        c = base_channels
        self.features = nn.Sequential(
            _conv_block(channels, c),      # [B, c,   165, 165]
            _conv_block(c,     c * 2),     # [B, c*2,  83,  83]
            _conv_block(c * 2, c * 4),     # [B, c*4,  42,  42]
            _conv_block(c * 4, c * 8),     # [B, c*8,  21,  21]
            nn.AdaptiveAvgPool2d(1),        # [B, c*8,   1,   1]
        )

        feat_dim = c * 8
        self.head = nn.Sequential(
            nn.Flatten(),
            nn.Linear(feat_dim, feat_dim // 2),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(feat_dim // 2, num_classes),
        )

    def forward(self, loss_map):
        return self.head(self.features(loss_map))
    
    
    
class ConvClassifier(nn.Module):
    def __init__(self, in_channels=3, num_classes=5):
        super().__init__()

        self.features = nn.Sequential(
            nn.Conv2d(in_channels, 32, kernel_size=3, padding=1),
            nn.ReLU(inplace=True),
            nn.MaxPool2d(2),   # H/2, W/2

            nn.Conv2d(32, 64, kernel_size=3, padding=1),
            nn.ReLU(inplace=True),
            nn.MaxPool2d(2),   # H/4, W/4
            
            # makes output size independent of input image size
            nn.AdaptiveAvgPool2d((1, 1))
        )

        self.classifier = nn.Sequential(
            nn.Flatten(),
            nn.Linear(64, 128),
            nn.ReLU(inplace=True),
            nn.Dropout(0.3),
            nn.Linear(128, num_classes)
        )

    def forward(self, x):
        x = self.features(x)
        logits = self.classifier(x)
        return logits

    def predict_probs(self, x):
        logits = self.forward(x)
        return F.softmax(logits, dim=1)

    def predict(self, x):
        probs = self.predict_probs(x)
        return torch.argmax(probs, dim=1)
    
    
class LinearAE(nn.Module):
    def __init__(
        self,
        latent_dim=64,
        hidden_dim=512,
        input_dim=108900,  # 330x330
        channels=1
    ):
        super().__init__()

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

    def forward(self, x):
        original_shape = x.shape
        if x.ndim > 2:
            x = x.flatten(start_dim=1)

        z = self.encoder(x)
        out = self.decoder(z)
        return out.view(*original_shape)
    
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
        deconv=None
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

        out_pads = [1,0,0,1]

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


class ConvAE_Strided_Optuna(nn.Module):
    def __init__(
        self,
        in_channels: int = 3,

        # sampled by Optuna
        base_channels: int = 128,
        conv_kernel: int = 3,
        conv_stride: int = 2,
        deconv_kernel: int = 3,
        deconv_stride: int = 2,
        num_stages: int = 4,
        blocks_per_stage: int = 1,

        # constant from YAML 
        conv_padding: Optional[int] = None,
        deconv_padding: Optional[int] = None,
        out_pads: Optional[List[int]] = None,
        activation: str = "leaky_relu",
    ):
        super().__init__()

        if conv_padding is None:
            conv_padding = conv_kernel // 2 # keep spatial sixe constant 
        if deconv_padding is None:
            deconv_padding = deconv_kernel // 2

        # output_padding must be < stride, so easiest safe choice is 0.
        if out_pads is None:
            out_pads = [0] * num_stages

        if len(out_pads) != num_stages:
            raise ValueError(f"out_pads must have length {num_stages}, got {len(out_pads)}")

        for i, op in enumerate(out_pads):
            if op >= deconv_stride:
                raise ValueError(
                    f"out_pads[{i}]={op} must be < deconv_stride={deconv_stride}"
                )

        if activation == "leaky_relu":
            act_enc = lambda: nn.LeakyReLU(inplace=True)
        elif activation == "relu":
            act_enc = lambda: nn.ReLU(inplace=True)
        else:
            raise ValueError(f"Unknown activation: {activation}")

        act_dec = lambda: nn.ReLU(inplace=True)

        base = base_channels
        in_ch = in_channels

        # -------- Encoder --------
        enc_layers = []
        ch_in = in_ch
        ch_out = base

        for stage in range(num_stages):
            
            enc_layers += [
                nn.Conv2d(ch_in, ch_out, kernel_size=conv_kernel, stride=conv_stride, padding=conv_padding),
                act_enc(),
            ]

            # no further downsampling
            for _ in range(blocks_per_stage - 1):
                enc_layers += [
                    nn.Conv2d(ch_out, ch_out, kernel_size=conv_kernel, stride=1, padding=conv_padding),
                    act_enc(),
                ]
            
            ch_in = ch_out
            ch_out = ch_out * 2  # compensate for losing spatial res by increasing feature richness 
            

            
            
        self.enc = nn.Sequential(*enc_layers)

        # -------- Decoder --------
        # Mirror: each stage upsamples once with ConvTranspose2d (stride=deconv_stride)
        # and has (blocks_per_stage-1) extra convs with stride=1.
        dec_layers = []
        enc_final_ch = base * (2 ** (num_stages - 1))

        ch_in = enc_final_ch
        ch_out = enc_final_ch // 2 if num_stages > 1 else base  # next lower stage

        for stage in range(num_stages):
            op = out_pads[stage]
            
            dec_layers += [
                nn.ConvTranspose2d(
                    ch_in, ch_out if stage < num_stages - 1 else base,  # keep base near the end
                    kernel_size=deconv_kernel, stride=deconv_stride,
                    padding=deconv_padding, output_padding=op
                ),
                act_dec(),
            ]

            for _ in range(blocks_per_stage - 1):
                dec_layers += [
                    nn.Conv2d(
                        ch_out if stage < num_stages - 1 else base,
                        ch_out if stage < num_stages - 1 else base,
                        kernel_size=deconv_kernel, stride=1, padding=deconv_padding
                    ),
                    act_dec(),
                ]

            ch_in = ch_out if stage < num_stages - 1 else base
            ch_out = max(ch_out // 2, base)

        dec_layers += [
            nn.Conv2d(base, in_ch, kernel_size=1, stride=1, padding=0),
            nn.Sigmoid(),
        ]

        self.dec = nn.Sequential(*dec_layers)


    def forward(self, x: torch.Tensor) -> torch.Tensor:
        inp_size = x.shape[-2:]          # save original H,W

        x = self.enc(x)
        x = self.dec(x)

        if x.shape[-2:] != inp_size:     # fix mismatch automatically
            x = F.interpolate(x, size=inp_size, mode="bilinear", align_corners=False)

        return x
