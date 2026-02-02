
import mlflow
from mlflow.tracking import MlflowClient
import torch
import numpy as np
from model import LinearAE
from dataset import QcdbImageDataset
from torch.utils.data import DataLoader, random_split
import matplotlib.pyplot as plt
from utils import * 
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import torch


def infer_scores_mse(model, loader, device):
    model.eval()
    scores = []

    if device:
        model = model.to(device)

    with torch.no_grad():
        for batch in loader:
            imgs = batch[0] if isinstance(batch, (tuple, list)) else batch

            if device:
                imgs = imgs.to(device)

            recon = model(imgs)

            # per-image anomaly score
            mse = ((imgs - recon) ** 2).mean(dim=(1, 2, 3))  # [B]
            scores.append(mse.detach().cpu())

    return torch.cat(scores).numpy()


if __name__ == '__main__': 

    if torch.backends.mps.is_available():
        device = torch.device("mps")
    elif torch.cuda.is_available():
        device = torch.device("cuda")
    else:
        device = torch.device("cpu")

    print("Using device:", device)

            
    CONFIG = load_yaml('params.yaml')
    mlflow.set_tracking_uri("http://127.0.0.1:8080")
    client = MlflowClient()

    
    mvs = list(client.search_model_versions("name='LinearAE'"))
    if not mvs:
        raise RuntimeError("No model versions found for 'LinearAE'")

    latest_mv = max(mvs, key=lambda mv: int(mv.version))

    print("Latest:", latest_mv.version, latest_mv.current_stage, latest_mv.run_id, latest_mv.source)

    model = mlflow.pytorch.load_model(latest_mv.source)
    
    dataset = QcdbImageDataset(
        folder="/Users/zetasourpi/cernbox/training-data/tpc/good",
        limit=None,
        image_size=[128, 128]
        )
    
    train_size = int(CONFIG["data_split"]["train_split"] * len(dataset))
    val_size = len(dataset) - train_size

    train_set, val_set = random_split(
        dataset,
        [train_size, val_size],
        generator=torch.Generator().manual_seed(CONFIG["data_split"]["split_seed"]),
    )

    good_loader = DataLoader(
        val_set,
        **CONFIG["dataloader_args"]
    )

    bad_dataset = QcdbImageDataset(
    folder="/Users/zetasourpi/cernbox/training-data/tpc/bad",
    limit=None,
    image_size=[128, 128]
    )
    
    bad_loader = DataLoader(
    bad_dataset,
    batch_size=CONFIG["dataloader_args"]["batch_size"],
    shuffle=False,
    )
    
    scores_good = infer_scores_mse(model, good_loader, device=device)
    scores_bad  = infer_scores_mse(model, bad_loader, device=device)

    thr = np.quantile(scores_good, 0.999)  # 99.9th percentile of the anomaly scores on good data. Meaning I allow <1% to be false negative from the good data

    fpr = np.sum(scores_good > thr) / len(scores_good) # FP / N_good
    tpr = np.sum(scores_bad > thr) / len(scores_bad) # TP / N_bad
    fdr = np.sum(scores_good > thr) /(np.sum(scores_good > thr) +np.sum(scores_bad > thr))

    print(f"Threshold: {thr:.4e}")
    print(f"FPR (good): {fpr:.4%}")
    print(f"TPR (bad):  {tpr:.4%}")
    print(f"FDR:  {fdr:.4%}")

    fig, ax = plt.subplots(figsize=(7, 5))

    ax.hist(
        scores_good,
        bins=100,
        histtype="step",
        linewidth=1,
        label="Good"
    )

    ax.hist(
        scores_bad,
        bins=100,
        histtype="step",
        linewidth=1,
        label="Bad"
    )

    ax.axvline(thr, linestyle="--", linewidth=2, label="99.9% threshold")

    ax.set_xscale("log")
    ax.set_yscale("log")

    ax.set_xlabel("Anomaly score (MSE)")
    ax.set_ylabel("Count")
    ax.legend()

    ax.grid(True, which="both", linestyle="--", alpha=0.4)

    ax.xaxis.set_major_locator(mticker.LogLocator(base=10.0))
    ax.xaxis.set_major_formatter(mticker.LogFormatterMathtext(base=10.0))
    ax.yaxis.set_major_locator(mticker.LogLocator(base=10.0))
    ax.yaxis.set_major_formatter(mticker.LogFormatterMathtext(base=10.0))

    plt.tight_layout()
    
    mlflow.set_tracking_uri(CONFIG["mlflow"]["tracking"]["server_uri"]) 
    mlflow.set_experiment("exp_001_linearAE_image_flat") # must be the same name as in the training process 

    with mlflow.start_run(run_name=f"test_linearAE_mse_q99"):
        mlflow.log_metric("threshold", float(thr))
        mlflow.log_metric("fpr", float(fpr))
        mlflow.log_metric("tpr", float(tpr))
        mlflow.log_metric("fdr", float(fdr))
    
        # Save and log plot under testing/
        os.makedirs("tmp_plots", exist_ok=True)
        plot_path = os.path.join("tmp_plots", "score_hist.png")
        fig.savefig(plot_path, dpi=150)
        mlflow.log_artifact(plot_path, artifact_path="testing/plots")

        # (Optional) also log the raw scores for debugging/repro
        np.savez_compressed(os.path.join("tmp_plots", "scores.npz"),
                            scores_good=scores_good, scores_bad=scores_bad)
        mlflow.log_artifact(os.path.join("tmp_plots", "scores.npz"), artifact_path="testing/data")
    print("Logged testing run to MLflow.")
    plt.show()
