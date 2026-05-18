import multiprocessing as mp
import os
import random
import shutil
from pathlib import Path

import mlflow
import mlflow.pytorch
import numpy as np
import torch
import torch.nn as nn
from mlflow.models import infer_signature
from torch.utils.data import DataLoader, random_split
from tqdm import tqdm

from dataset import QcdbRootTensorDataset
from model import LinearAE as Model
import ROOT

from root_tensor_utils import (
    _apply_pad_style,
    compute_global_max,
    denormalize_tensor_log1p,
    extract_histogram_info,
    save_root_comparison,
    tensor_to_th2,
    th2_to_root_object,
)
from utils import load_yaml, log_git_to_mlflow


if torch.backends.mps.is_available():
    device = torch.device("mps")
elif torch.cuda.is_available():
    device = torch.device("cuda")
else:
    device = torch.device("cpu")


#######################################################################################
# This file contains the code logic to train the Linear AE on the QcdbRootTensorDataset
#######################################################################################


def set_seed(seed):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def resolve_input_dim(dataset):
    sample = dataset[0]
    if isinstance(sample, (tuple, list)):
        sample = sample[0]
    return int(sample.numel())


def export_reconstructions(model, dataloader, export_config, dataset):
    export_root = Path(export_config["base_dir"])
    root_dir = export_root / export_config.get("root_dirname", "root")
    cmp_dir = export_root / "comparisons"
    root_dir.mkdir(parents=True, exist_ok=True)
    cmp_dir.mkdir(parents=True, exist_ok=True)

    max_samples = export_config.get("max_samples")
    saved = 0
    model.eval()

    with torch.no_grad():
        for batch in dataloader:
            imgs, metadata = batch
            imgs = imgs.to(device)
            recon = model(imgs).detach().cpu()
            imgs_cpu = imgs.detach().cpu()

            for i in range(recon.shape[0]):
                if max_samples is not None and saved >= max_samples:
                    return

                root_path = metadata["path"][i]
                hist_index = int(metadata["hist_index"][i])
                hist_name = str(metadata["hist_name"][i]).replace("/", "_")
                source_stem = Path(root_path).stem
                sample_name = f"{saved:04d}_{source_stem}_hist{hist_index}_{hist_name}"

                info = extract_histogram_info(root_path, hist_index=hist_index)

                # Save reconstructed histogram as a ROOT object
                root_tensor = denormalize_tensor_log1p(recon[i], max_value=dataset.max_value)
                th2_back = tensor_to_th2(root_tensor, info, hist_name=f"{hist_name}_reco")
                th2_to_root_object(
                    th2_back,
                    canvas_name="ccdb_object",
                    plot=False,
                    save_path=root_dir / f"{sample_name}.root",
                )

                # Save 3-panel ROOT comparison (orig | reco | residual) as PNG + ROOT
                save_root_comparison(
                    orig_norm=imgs_cpu[i],
                    recon_norm=recon[i],
                    info=info,
                    max_value=dataset.max_value,
                    out_path=cmp_dir / sample_name,  # suffix added inside the function
                )
                saved += 1


def save_epoch_monitoring_plot(
    model,
    train_iterator: DataLoader,
    epoch: int,
    output_dir: Path,
    dataset,
    n_samples: int = 4,
) -> Path:
    """Save a 2-row ROOT canvas of augmented inputs (top) vs reconstructions (bottom).

    Pulls one fresh batch from train_iterator each call so you see a different
    set of augmented samples every epoch. Uses extract_histogram_info for axis
    metadata so the ROOT axes match the original detector layout.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    model.eval()
    batch = next(iter(train_iterator))
    imgs, metadata = batch
    imgs = imgs[:n_samples].to(device)

    with torch.no_grad():
        recons = model(imgs).detach().cpu()
    imgs_cpu = imgs.detach().cpu()
    n = imgs_cpu.shape[0]

    ROOT.gStyle.SetOptStat(0)
    ROOT.gStyle.SetPalette(ROOT.kBird)

    canvas = ROOT.TCanvas(f"monitor_e{epoch:03d}", f"Epoch {epoch}", 900 * n, 1600)
    canvas.Divide(n, 2)

    # Keep all TH2 objects alive until after canvas.SaveAs — ROOT will GC any
    # histogram that loses its Python reference, leaving blank pads.
    th2_inps = []
    th2_recs = []

    for i in range(n):
        path = metadata["path"][i]
        hist_index = int(metadata["hist_index"][i])
        hist_name = str(metadata["hist_name"][i]).replace("/", "_")
        info = extract_histogram_info(path, hist_index=hist_index)

        inp_raw = denormalize_tensor_log1p(imgs_cpu[i], max_value=dataset.max_value)
        rec_raw = denormalize_tensor_log1p(recons[i], max_value=dataset.max_value)

        th2_inp = tensor_to_th2(inp_raw, info, hist_name=f"inp_{i}")
        th2_rec = tensor_to_th2(rec_raw, info, hist_name=f"rec_{i}")
        th2_inps.append(th2_inp)
        th2_recs.append(th2_rec)

        # Share the same z-scale between input and reconstruction for fair comparison
        zmax = float(th2_inp.GetMaximum())
        th2_inp.SetMaximum(zmax)
        th2_rec.SetMaximum(zmax)

        pad_inp = canvas.cd(i + 1)
        _apply_pad_style(pad_inp)
        th2_inp.SetTitle(f"Input — {hist_name} (aug)")
        th2_inp.Draw("COLZ")
        pad_inp.Update()

        pad_rec = canvas.cd(n + i + 1)
        _apply_pad_style(pad_rec)
        th2_rec.SetTitle(f"Reco — {hist_name}")
        th2_rec.Draw("COLZ")
        pad_rec.Update()

    canvas.Update()
    out_path = output_dir / f"epoch_{epoch:03d}.png"
    canvas.SaveAs(str(out_path))
    canvas.Close()
    return out_path


def log_model_artifacts(model, signature=None, x=None):
    mlflow.log_text(str(model), "model_architecture.txt")

    # in "Model info"
    log_model_kwargs = {"name": "model"}
    if signature is not None:
        log_model_kwargs["signature"] = signature
    if x is not None:
        log_model_kwargs["input_example"] = x
    mlflow.pytorch.log_model(model, **log_model_kwargs)

    # in "Artifacts"
    export_dir = "exported_model_tmp"
    if os.path.exists(export_dir):
        shutil.rmtree(export_dir)

    save_model_kwargs = {"path": export_dir}
    if signature is not None:
        save_model_kwargs["signature"] = signature
    if x is not None:
        save_model_kwargs["input_example"] = x
    mlflow.pytorch.save_model(model, **save_model_kwargs)

    mlflow.log_artifacts(export_dir, artifact_path="exported_model")
    shutil.rmtree(export_dir)


def main():
    print("Using device:", device)

    config = load_yaml("params.yaml")
    set_seed(int(config["train"].get("seed", 123)))

    mlflow.end_run()
    mlflow.set_tracking_uri(config["mlflow"]["tracking"]["server_uri"])

    assert mlflow.get_tracking_uri().startswith("http"), (
        f"You're not logging to the MLflow server. tracking_uri={mlflow.get_tracking_uri()}"
    )

    mlflow.set_experiment(experiment_name=config["mlflow"]["experiment_name"])

    dataset_log_config = dict(config["qcdb_root_tensor_train_dataset"])

    print("Computing global max from training ROOT files...")
    computed_max = compute_global_max(
        folder=dataset_log_config["folder"],
        hist_indices=tuple(dataset_log_config.get("hist_indices", [0, 1])),
    )
    print(f"  global max = {computed_max:.1f}  (config had {dataset_log_config['max_value']})")
    dataset_log_config["max_value"] = computed_max

    dataset = QcdbRootTensorDataset(**dataset_log_config)
    config["linear_model_parametrs"]["input_dim"] = resolve_input_dim(dataset)

    train_size = int(config["data_split"]["train_split"] * len(dataset))
    val_size = len(dataset) - train_size

    print("Training set size : ", train_size)
    print("Validation set size: ", val_size)

    train_set, val_set = random_split(
        dataset,
        [train_size, val_size],
        generator=torch.Generator().manual_seed(config["data_split"]["split_seed"]),
    )

    train_dataloader_args = dict(config["dataloader_args"])
    val_dataloader_args = dict(config["dataloader_args"])
    val_dataloader_args["shuffle"] = False

    train_iterator = DataLoader(train_set, **train_dataloader_args)
    val_iterator = DataLoader(val_set, **val_dataloader_args)

    model = Model(**config["linear_model_parametrs"]).to(device)
    opt = torch.optim.Adam(model.parameters(), lr=float(config["train"]["lr"]))

    # Build masked loss: only compute MSE on active detector pixels.
    # mask shape is [H, W] from the dataset; broadcast to [1, 1, H, W] for [B, 1, H, W] tensors.
    if dataset.mask is not None:
        loss_mask = dataset.mask.unsqueeze(0).unsqueeze(0).to(device).float()  # [1, 1, H, W]
        loss_mask_sum = float(loss_mask.sum())
        print(f"Mask loaded: {int(loss_mask_sum)} active pixels / {loss_mask.numel()} total "
              f"({100 * loss_mask_sum / loss_mask.numel():.1f}% active)")

        def loss_fn(pred, target):
            return ((pred - target) ** 2 * loss_mask).sum() / (loss_mask_sum * pred.shape[0])

        def mae_fn(pred, target):
            return ((pred - target).abs() * loss_mask).sum() / (loss_mask_sum * pred.shape[0])
    else:
        print("No mask — loss computed over all pixels.")
        loss_fn = nn.MSELoss()
        mae_fn  = nn.L1Loss()

    mlflow.enable_system_metrics_logging()

    with mlflow.start_run(run_name=config["mlflow"]["run_name"]):
        signature = None
        x = None
        best_val_loss = float("inf")
        best_model_state = None
        epochs_without_improvement = 0
        early_stopping_patience = config["train"].get("early_stopping_patience")
        early_stopping_min_delta = float(config["train"].get("early_stopping_min_delta", 0.0))
        try:
            # Infer model signature
            batch = next(iter(train_iterator))
            if isinstance(batch, (tuple, list)):
                batch = batch[0]

            model.eval()
            with torch.no_grad():
                batch = batch.to(device)
                x = batch.detach().cpu().numpy().astype(np.float32)
                y = model(batch).detach().cpu().numpy().astype(np.float32)

            signature = infer_signature(x, y)

            mlflow.log_params(config["linear_model_parametrs"])
            mlflow.log_params(config["train"])
            mlflow.log_params(config["dataloader_args"])
            mlflow.log_params(config["data_split"])
            mlflow.log_params(dataset_log_config)
            mlflow.log_param("max_value", dataset.max_value)
            mlflow.log_param("num_train_samples", train_size)
            mlflow.log_param("num_val_samples", val_size)

            for epoch in tqdm(range(1, config["train"]["epochs"] + 1)):
                epoch_train_loss = 0.0
                num_train_batches = 0

                model.train()
                for train_batch in train_iterator:
                    img_batch = train_batch[0] if isinstance(train_batch, (tuple, list)) else train_batch
                    img_batch = img_batch.to(device)

                    opt.zero_grad()
                    preds = model(img_batch)
                    train_loss = loss_fn(preds, img_batch)
                    train_loss.backward()
                    opt.step()

                    epoch_train_loss += train_loss.item()
                    num_train_batches += 1

                epoch_train_loss /= max(num_train_batches, 1)
                mlflow.log_metric("train_loss", epoch_train_loss, step=epoch)

                model.eval()
                val_loss_sum = 0.0
                val_mse_sum = 0.0
                val_mae_sum = 0.0
                num_val_batches = 0

                for eval_batch in val_iterator:
                    imgs = eval_batch[0] if isinstance(eval_batch, (tuple, list)) else eval_batch
                    imgs = imgs.to(device)

                    with torch.no_grad():
                        recon = model(imgs)
                        eval_loss = loss_fn(recon, imgs)
                        mae = mae_fn(recon, imgs)

                    val_loss_sum += eval_loss.item()
                    val_mse_sum += eval_loss.item()
                    val_mae_sum += mae.item()
                    num_val_batches += 1

                val_loss = val_loss_sum / max(num_val_batches, 1)
                val_mse = val_mse_sum / max(num_val_batches, 1)
                val_mae = val_mae_sum / max(num_val_batches, 1)

                mlflow.log_metric("val_loss", val_loss, step=epoch)
                mlflow.log_metric("val_mse", val_mse, step=epoch)
                mlflow.log_metric("val_mae", val_mae, step=epoch)

                print(
                    f"Epoch {epoch:03d} | "
                    f"Train: {epoch_train_loss:.4f} | "
                    f"Val: {val_loss:.4f} | "
                    f"MSE: {val_mse:.4f} | "
                    f"MAE: {val_mae:.4f}"
                )

                if val_loss < (best_val_loss - early_stopping_min_delta):
                    best_val_loss = val_loss
                    epochs_without_improvement = 0
                    best_model_state = {
                        key: value.detach().cpu().clone()
                        for key, value in model.state_dict().items()
                    }
                else:
                    epochs_without_improvement += 1

                mlflow.log_metric("best_val_loss", best_val_loss, step=epoch)
                mlflow.log_metric("epochs_without_improvement", epochs_without_improvement, step=epoch)

                viz_cfg = config.get("viz", {})
                every_n = int(viz_cfg.get("every_n_epochs", 1))
                n_mon   = int(viz_cfg.get("num_samples", 4))
                if epoch % every_n == 0 or epoch == 1:
                    monitor_dir = Path(config["reconstruction_export"]["base_dir"]) / "epoch_monitor"
                    save_epoch_monitoring_plot(
                        model, train_iterator, epoch, monitor_dir, dataset, n_samples=n_mon
                    )

                if (
                    early_stopping_patience is not None
                    and epochs_without_improvement >= early_stopping_patience
                ):
                    print(
                        f"Early stopping at epoch {epoch:03d}. "
                        f"Validation loss did not improve for {early_stopping_patience} epochs."
                    )
                    break

            if best_model_state is not None:
                model.load_state_dict(best_model_state)

            log_model_artifacts(model, signature=signature, x=x)

            export_reconstructions(
                model,
                val_iterator,
                config["reconstruction_export"],
                dataset,
            )
            mlflow.log_artifacts(
                config["reconstruction_export"]["base_dir"],
                artifact_path="reconstruction_exports",
            )

            monitor_dir = Path(config["reconstruction_export"]["base_dir"]) / "epoch_monitor"
            if monitor_dir.exists():
                mlflow.log_artifacts(str(monitor_dir), artifact_path="epoch_monitor")

            log_git_to_mlflow(log_diff=True)

            print("Run:", mlflow.active_run().info.run_id)
            print("Artifact URI:", mlflow.get_artifact_uri())

            mlflow.end_run()

        except (KeyboardInterrupt, BrokenPipeError):
            log_model_artifacts(model, signature=signature, x=x)

            log_git_to_mlflow(log_diff=True)

            print("Run:", mlflow.active_run().info.run_id)
            print("Artifact URI:", mlflow.get_artifact_uri())

            mlflow.end_run()


if __name__ == "__main__":
    mp.freeze_support()
    main()
