
import torch
import torch.nn as nn
import torch.nn.functional as F


# Original linear classifier (kept for reference)
# class LossMapClassifier(nn.Module):
#     def __init__(self, channels=3, image_size=(330, 330), num_classes=4):
#         super().__init__()
#         h, w = image_size
#         input_dim = h * w * channels
#
#         self.classification_head = nn.Sequential(
#             nn.Linear(input_dim, 512),
#             nn.ReLU(),
#             nn.Dropout(0.4),
#
#             nn.Linear(512, 256),
#             nn.ReLU(),
#             nn.Dropout(0.4),
#
#             nn.Linear(256, 128),
#             nn.ReLU(),
#             nn.Dropout(0.3),
#
#             nn.Linear(128, num_classes)
#         )
#
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
