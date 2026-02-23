from dataset import QcdbImageDataset, QcdbNpyTensorDataset
from model import ConvAE_Strided as Model
from tqdm import tqdm
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
import torch

if torch.backends.mps.is_available():
    device = torch.device("mps")
elif torch.cuda.is_available():
    device = torch.device("cuda")
else:
    device = torch.device("cpu")

print("Using device:", device)

CONFIG = load_yaml('params.yaml')

mlflow.end_run()
mlflow.set_tracking_uri(CONFIG["mlflow"]["tracking"]["server_uri"])

assert mlflow.get_tracking_uri().startswith("http"), (
    f"You're not logging to the MLflow server. tracking_uri={mlflow.get_tracking_uri()}"
)

mlflow.set_experiment(experiment_name=CONFIG["mlflow"]["experiment_name"])

dataset = QcdbImageDataset(**CONFIG["dataset"])
#dataset = QcdbNpyTensorDataset(**CONFIG["dataset"])

train_size = int(CONFIG["data_split"]["train_split"] * len(dataset))
val_size = len(dataset) - train_size

print("Training set size : ", train_size)
print("Validation set size: ", val_size)

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
    #**CONFIG["model_parameters"]
    **CONFIG["convolutional_model_parameters"]
)


model = model.to(device)
loss_fn = nn.MSELoss()
opt = torch.optim.Adam(model.parameters(), lr=float(CONFIG["train"]["lr"]))

# Infer model signature
batch = next(iter(train_iterator))
#print("batch shape:", batch[0].shape)

if isinstance(batch, (tuple, list)):
    batch = batch[0]
    
model.eval()
with torch.no_grad():
    batch = batch.to(device)
    x = batch.detach().cpu().numpy().astype(np.float32)
    y = model(batch).detach().cpu().numpy().astype(np.float32)

signature = infer_signature(x, y)

mlflow.enable_system_metrics_logging()

with mlflow.start_run(run_name = CONFIG["mlflow"]["run_name"]):
    
    try: 
        mlflow.log_params(CONFIG["convolutional_model_parameters"])
        mlflow.log_params(CONFIG["train"])
        mlflow.log_params(CONFIG["dataloader_args"])
        mlflow.log_params(CONFIG["data_split"])

        for epoch in tqdm(range(1, CONFIG['train']['epochs'] + 1)):
            
            model.train()
            for train_batch in train_iterator:
                img_batch = train_batch[0] if isinstance(train_batch, (tuple, list)) else train_batch
                img_batch = img_batch.to(device)
                
                # print("model device:", next(model.parameters()).device)
                # print("batch device:", batch.device)

                preds = model(img_batch)
                # print("preds:", preds.shape, "target:", img_batch.shape)
                train_loss = loss_fn(preds, img_batch)

                opt.zero_grad()
                train_loss.backward()
                opt.step()
                        
            mlflow.log_metric("train_loss", float(train_loss.item()), step=epoch)
            
            model.eval()
            for eval_batch in val_iterator:
                imgs = eval_batch
                loss_vals, mse_vals, mae_vals, ssim_vals = [], [], [], []
                
                imgs = imgs.to(device)

                with torch.no_grad():
                        recon = model(imgs)
                        eval_loss = loss_fn(recon, imgs)
                        # mse = ((imgs - recon) ** 2).mean(dim=(1, 2, 3))
                        # mae = (imgs - recon).abs().mean(dim=(1, 2, 3))

                        # mse_vals.append(mse.detach().cpu())
                        # mae_vals.append(mae.detach().cpu())

            # aggregate
            # val_mse  = torch.cat(mse_vals).mean().item()
            # val_mae  = torch.cat(mae_vals).mean().item()

            mlflow.log_metric("val_loss", eval_loss.item(), step=epoch)
            # mlflow.log_metric("val_mse",  val_mse,  step=epoch)
            # mlflow.log_metric("val_mae",  val_mae,  step=epoch)

            print(
                f"Epoch {epoch:03d} | "
                f"Train: {train_loss.item():.4f} | "
                f"Val: {eval_loss.item():.4f} | "
            )
            
            # Return to training mode 
            model.train()
            
            # if epoch % 4 == 0:
            #     with torch.no_grad():
            #         sample = img_batch[:4]
            #         recon = model(sample)

            #     fig, axes = plt.subplots(2, 4, figsize=(10, 5))
            #     for i in range(4):
            #         axes[0, i].imshow(sample[i].detach().cpu().permute(1, 2, 0))
            #         axes[0, i].set_title("Original")
            #         axes[0, i].axis("off")

            #         axes[1, i].imshow(recon[i].detach().cpu().permute(1, 2, 0))
            #         axes[1, i].set_title("Reconstructed")
            #         axes[1, i].axis("off")

            #     mlflow.log_figure(fig, f"train_imgs/recon_epoch_{epoch:03d}.png")
            #     plt.close(fig)

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

    except BaseException as e:
        print(e)
        
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
