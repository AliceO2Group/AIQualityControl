from models.autoencoder.src.dataset import QcdbImageDataset
from models.autoencoder.src.model import LinearAE as Model
from models.autoencoder.src.eval import evaluate_epoch
from mlflow.models import infer_signature
import numpy as np
import torch
import shutil
import os
import csv
import json
import time
from typing import Dict, Any

import torch
import torch.nn as nn
from torch.utils.data import DataLoader, random_split
from torchvision.utils import save_image
import matplotlib.pyplot as plt

import mlflow
import mlflow.pytorch

from utils import *


CONFIG = load_yaml('params.yaml')

mlflow.end_run()
mlflow.set_tracking_uri(CONFIG["mlflow"]["tracking"]["server_uri"])

assert mlflow.get_tracking_uri().startswith("http"), (
    f"You're not logging to the MLflow server. tracking_uri={mlflow.get_tracking_uri()}"
)

mlflow.set_experiment(experiment_name=CONFIG["mlflow"]["experiment_name"])


dataset = QcdbImageDataset(**CONFIG["dataset"])

train_size = int(CONFIG["data_split"]["train_split"] * len(dataset))
val_size = len(dataset) - train_size

train_set, val_set = random_split(
    dataset,
    [train_size, val_size],
    generator=torch.Generator().manual_seed(CONFIG["data_split"]["split_seed"]),
)

train_iterator = DataLoader(
    train_set,
    **CONFIG["dataloader_args"]
)
val_iterator = DataLoader(
    val_set,
    **CONFIG["dataloader_args"]
)

model = Model(
    image_size=CONFIG["dataset"]["image_size"],
    **CONFIG["model_parameters"]
)

loss_fn = nn.MSELoss()
opt = torch.optim.Adam(model.parameters(), lr=CONFIG["train"]["lr"])

# Infer model signature
batch = next(iter(train_iterator))
if isinstance(batch, (tuple, list)):
    batch = batch[0]
    
model.eval()
with torch.no_grad():
    x = batch.detach().cpu().numpy().astype(np.float32)
    y = model(batch).detach().cpu().numpy().astype(np.float32)

signature = infer_signature(x, y)

# Return to training mode 
model.train()

mlflow.enable_system_metrics_logging()

with mlflow.start_run(run_name = CONFIG["mlflow"]["run_name"]):
    mlflow.log_params(CONFIG["model_parameters"])

    for epoch in range(1, CONFIG['train']['epochs'] + 1):
        for batch in train_iterator:
            img_batch = batch[0] if isinstance(batch, (tuple, list)) else batch

            preds = model(img_batch)
            loss = loss_fn(preds, img_batch)

            opt.zero_grad()
            loss.backward()
            opt.step()

        mlflow.log_metric("train_loss", float(loss.item()), step=epoch)
        print(f"Epoch {epoch} | Loss: {loss.item():.4f}")

        if epoch % 4 == 0:
            with torch.no_grad():
                sample = img_batch[:4]
                recon = model(sample)

            fig, axes = plt.subplots(2, 4, figsize=(10, 5))
            for i in range(4):
                axes[0, i].imshow(sample[i].detach().cpu().permute(1, 2, 0))
                axes[0, i].set_title("Original")
                axes[0, i].axis("off")

                axes[1, i].imshow(recon[i].detach().cpu().permute(1, 2, 0))
                axes[1, i].set_title("Reconstructed")
                axes[1, i].axis("off")

            mlflow.log_figure(fig, f"train_imgs/recon_epoch_{epoch:03d}.png")
            plt.close(fig)

    
    mlflow.log_text(str(model), "model_architecture.txt")

    # in "Model info"
    mlflow.pytorch.log_model(model, name="model", signature=signature, input_example=x)
    
    # in "Artifacts"
    export_dir = "exported_model_tmp"
    if os.path.exists(export_dir):
        shutil.rmtree(export_dir)

    mlflow.pytorch.save_model(
        model,
        path=export_dir,
        signature=signature,
        input_example=x,
    )

    mlflow.log_artifacts(export_dir, artifact_path="exported_model")
    shutil.rmtree(export_dir)
    
    log_git_to_mlflow(log_diff=True)
    
    print("Run:", mlflow.active_run().info.run_id)
    print("Artifact URI:", mlflow.get_artifact_uri())
    
    mlflow.end_run()
