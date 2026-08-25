import os
from pathlib import Path

import mlflow
from mlflow.tracking import MlflowClient
import torch
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from sklearn.metrics import precision_recall_curve
from torch.utils.data import DataLoader, random_split

from dataset import QcdbImageDataset
from utils import load_yaml


# def infer_scores_mse(model, loader, device):
#     model.eval()
#     scores = []

#     model = model.to(device)

#     with torch.no_grad():
#         for batch in loader:
#             imgs = batch[0] if isinstance(batch, (tuple, list)) else batch
#             imgs = imgs.to(device)
#             recon = model(imgs)

#             mse = ((imgs - recon) ** 2).mean(dim=(1, 2, 3))
#             scores.append(mse.detach().cpu())

#     return torch.cat(scores).numpy()

import torch
import numpy as np
from scipy.ndimage import gaussian_filter


def infer_scores(model, loader, device, topk_ratio=0.001):
    model.eval()

    scores_mean = []
    scores_p995 = []
    scores_p995_smooth = []
    scores_topk = []
    scores_spike = []

    model = model.to(device)

    with torch.no_grad():
        for batch in loader:
            imgs = batch[0] if isinstance(batch, (tuple, list)) else batch
            imgs = imgs.to(device)

            recon = model(imgs)

            # pixel reconstruction error
            error = (imgs - recon) ** 2
            error = error.mean(dim=1)  # collapse channel dimension → (B, H, W)

            for e in error:
                e_np = e.detach().cpu().numpy()

                # 1️⃣ original mean MSE
                scores_mean.append(e_np.mean())

                # 2️⃣ percentile
                scores_p995.append(np.percentile(e_np, 99))

                # 3️⃣ smoothed percentile
                bg = gaussian_filter(e_np, sigma=4.0)
                enhanced = np.clip(e_np - bg, 0, None)
                scores_p995_smooth.append(np.percentile(enhanced, 99.5))

                # 4️⃣ top-k mean
                flat = e_np.ravel()
                k = max(1, int(topk_ratio * flat.size))
                topk = np.partition(flat, -k)[-k:]
                scores_topk.append(topk.mean())

                # 5️⃣ spike-enhanced score (difference of Gaussians idea)
                bg = gaussian_filter(e_np, sigma=3.0)
                enhanced = np.clip(e_np - bg, 0, None)
                scores_spike.append(np.percentile(enhanced, 99.99))

    return {
        "mean_mse": np.array(scores_mean),
        "p995": np.array(scores_p995),
        "p995_smooth": np.array(scores_p995_smooth),
        "topk": np.array(scores_topk),
        "spike": np.array(scores_spike),
    }
    
    
    
def tensor_to_display_image(x):
    x = x.detach().cpu()
    if x.ndim == 3 and x.shape[0] in (1, 3):
        x = x.permute(1, 2, 0).numpy()
    else:
        x = x.numpy()

    x = np.nan_to_num(x)

    xmin, xmax = x.min(), x.max()
    if xmax > xmin:
        x = (x - xmin) / (xmax - xmin)
    else:
        x = np.zeros_like(x)

    if x.ndim == 3 and x.shape[-1] == 1:
        x = x[..., 0]

    return x


def lossmap_to_display_image(loss_map):
    loss_map = loss_map.detach().cpu()
    if loss_map.ndim == 3:
        loss_map = loss_map.mean(dim=0)
    loss_map = loss_map.numpy()
    loss_map = np.nan_to_num(loss_map)

    xmin, xmax = loss_map.min(), loss_map.max()
    if xmax > xmin:
        loss_map = (loss_map - xmin) / (xmax - xmin)
    else:
        loss_map = np.zeros_like(loss_map)

    return loss_map


def predict_and_save_results(model, loader, device, save_dir, prefix):
    model.eval()
    model = model.to(device)

    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    scores = []
    global_idx = 0

    with torch.no_grad():
        for batch_idx, batch in enumerate(loader):
            imgs = batch[0] if isinstance(batch, (tuple, list)) else batch
            imgs = imgs.to(device)
            recons = model(imgs)

            loss_maps = (imgs - recons) ** 4
            batch_scores = loss_maps.mean(dim=(1, 2, 3)).detach().cpu().numpy()
            scores.append(torch.tensor(batch_scores))

            for i in range(imgs.shape[0]):
                img = tensor_to_display_image(imgs[i])
                recon = tensor_to_display_image(recons[i])
                loss_img = lossmap_to_display_image(loss_maps[i])

                fig, axes = plt.subplots(1, 4, figsize=(12, 3))

                axes[0].imshow(img, cmap="gray" if img.ndim == 2 else None)
                axes[0].set_title("Original")
                axes[0].axis("off")

                axes[1].imshow(recon, cmap="gray" if recon.ndim == 2 else None)
                axes[1].set_title("Reconstruction")
                axes[1].axis("off")

                im = axes[2].imshow(loss_img, cmap="hot")
                axes[2].set_title("Loss map")
                axes[2].axis("off")
                fig.colorbar(im, ax=axes[2], fraction=0.046, pad=0.04)

                # quality map
                quality = np.zeros((*loss_img.shape, 3))
                quality[(loss_img <= 0.4) & (loss_img > 0.1)] = [0, 1, 0]
                quality[loss_img <= 0.1] = [1, 1, 1]
                quality[loss_img > 0.4] = [1, 0, 0]

                axes[3].imshow(quality)
                axes[3].set_title("Quality")
                axes[3].axis("off")
                
                fig.suptitle(f"{prefix} | idx={global_idx} | score={batch_scores[i]:.6e}")
                plt.tight_layout()

                out_path = save_dir / f"{prefix}_{global_idx:05d}_score_{batch_scores[i]:.6e}.png"
                fig.savefig(out_path, dpi=150, bbox_inches="tight")
                plt.close(fig)

                global_idx += 1

    return torch.cat(scores).numpy()


def compute_metrics(scores_good, scores_bad, thr):
    FP = np.sum(scores_good > thr)
    TN = np.sum(scores_good <= thr)
    TP = np.sum(scores_bad > thr)
    FN = np.sum(scores_bad <= thr)

    fpr = FP / len(scores_good) if len(scores_good) > 0 else 0.0
    tpr = TP / len(scores_bad) if len(scores_bad) > 0 else 0.0
    precision = TP / (TP + FP) if (TP + FP) > 0 else 0.0
    recall = TP / (TP + FN) if (TP + FN) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    fdr = FP / (FP + TP) if (FP + TP) > 0 else 0.0

    labels = np.concatenate([np.zeros(len(scores_good)), np.ones(len(scores_bad))])
    scores = np.concatenate([scores_good, scores_bad])

    precisions, recalls, _ = precision_recall_curve(labels, scores)

    mask_95 = precisions >= 0.95
    max_recall_95 = recalls[mask_95].max() if np.any(mask_95) else 0.0

    mask_99 = precisions >= 0.99
    max_recall_99 = recalls[mask_99].max() if np.any(mask_99) else 0.0

    return {
        "n_good": int(len(scores_good)),
        "n_bad": int(len(scores_bad)),
        "threshold": float(thr),
        "tp": int(TP),
        "fp": int(FP),
        "tn": int(TN),
        "fn": int(FN),
        "fpr": float(fpr),
        "tpr": float(tpr),
        "precision": float(precision),
        "recall": float(recall),
        "f1": float(f1),
        "fdr": float(fdr),
        "max_recall_95": float(max_recall_95),
        "max_recall_99": float(max_recall_99),
        "mean_score_good": float(np.mean(scores_good)),
        "mean_score_bad": float(np.mean(scores_bad)),
        "median_score_good": float(np.median(scores_good)),
        "median_score_bad": float(np.median(scores_bad)),
    }


def find_category_folders(root):
    root = Path(root)
    return sorted([p for p in root.iterdir() if p.is_dir()])


def _apply_hist_style(ax, thr, title):
    ax.axvline(thr, linestyle="--", linewidth=2, color="black", label="Threshold")
    ax.set_xscale("log")
    ax.set_xlabel("Anomaly score (MSE)")
    ax.set_ylabel("Count")
    ax.set_title(title)
    ax.grid(True, which="both", linestyle="--", alpha=0.4)
    ax.xaxis.set_major_locator(mticker.LogLocator(base=10.0))
    ax.xaxis.set_major_formatter(mticker.LogFormatterMathtext(base=10.0))


def make_histogram(scores_good, scores_bad, thr, title, save_path, bad_label="Bad"):
    fig, ax = plt.subplots(figsize=(7, 5))

    scores_good = scores_good[scores_good > 0]
    scores_bad = scores_bad[scores_bad > 0]

    ax.hist(scores_good, bins="fd", histtype="step", linewidth=1.5, label="Good")
    ax.hist(scores_bad, bins="fd", histtype="step", linewidth=1.5, label=bad_label)

    _apply_hist_style(ax, thr, title)
    ax.legend()

    plt.tight_layout()
    fig.savefig(save_path, dpi=150)
    plt.close(fig)


def make_combined_bad_histogram(scores_good, scores_by_category, thr, title, save_path):
    fig, ax = plt.subplots(figsize=(9, 6))

    scores_good = scores_good[scores_good > 0]
    
    ax.hist(scores_good, bins=100, histtype="step", linewidth=2.0, label="Good")

    for category, scores_bad in sorted(scores_by_category.items()):
        scores_bad = np.asarray(scores_bad, dtype=np.float64)
        scores_bad = scores_bad[np.isfinite(scores_bad)]
        print(len(scores_bad))
        scores_bad = scores_bad[scores_bad > 0]

        ax.hist(
            scores_bad,
            bins=100,
            histtype="step",
            linewidth=1.5,
            label=category,
        )

    _apply_hist_style(ax, thr, title)
    ax.legend(fontsize=9)

    plt.tight_layout()
    fig.savefig(save_path, dpi=150)
    plt.close(fig)


def get_device():
    if torch.backends.mps.is_available():
        return torch.device("mps")
    if torch.cuda.is_available():
        return torch.device("cuda")
    return torch.device("cpu")


if __name__ == "__main__":
    device = get_device()
    print("Using device:", device)

    CONFIG = load_yaml("params.yaml")

    mlflow.set_tracking_uri(CONFIG["mlflow"]["tracking"]["server_uri"])
    client = MlflowClient()

    mvs = list(client.search_model_versions(f"name='{CONFIG['testing']['model_name']}'"))
    if not mvs:
        raise RuntimeError("No model versions found")

    latest_mv = max(mvs, key=lambda mv: int(mv.version))
    print("Latest version:", latest_mv.version, latest_mv.source)

    model = mlflow.pytorch.load_model(latest_mv.source)

    training_dataset = QcdbImageDataset(folder=CONFIG["train_dataset"]["folder"])

    train_size = int(CONFIG["data_split"]["train_split"] * len(training_dataset))
    val_size = len(training_dataset) - train_size

    _, val_dataset = random_split(
        training_dataset,
        [train_size, val_size],
        generator=torch.Generator().manual_seed(CONFIG["data_split"]["split_seed"]),
    )

    good_loader = DataLoader(
        val_dataset,
        batch_size=CONFIG["dataloader_args"]["batch_size"],
        shuffle=False,
    )

    scores_good = infer_scores(model, good_loader, device=device)
    scores_good = scores_good["spike"]

    thr_quantile = CONFIG["testing"].get("threshold_quantile", 0.995)
    thr = np.quantile(scores_good, thr_quantile)

    bad_root = CONFIG["testing"]["bad_root_folder"]
    results_root = Path(CONFIG["testing"]["results_folder"])
    results_root.mkdir(parents=True, exist_ok=True)

    category_dirs = find_category_folders(bad_root)

    if not category_dirs:
        raise RuntimeError(f"No category folders found in: {bad_root}")

    out_dir = Path("tmp_eval_conv")
    plots_dir = out_dir / "plots"
    data_dir = out_dir / "data"
    plots_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    results = []
    scores_by_category = {}

    for cat_dir in category_dirs:
        print(f"Evaluating category: {cat_dir.name}")

        bad_dataset = QcdbImageDataset(folder=str(cat_dir))
        bad_loader = DataLoader(
            bad_dataset,
            batch_size=CONFIG["dataloader_args"]["batch_size"],
            shuffle=False,
        )

        category_result_dir = results_root / cat_dir.name
        scores_bad = predict_and_save_results(
            model=model,
            loader=bad_loader,
            device=device,
            save_dir=category_result_dir,
            prefix=cat_dir.name,
        )
        scores_by_category[cat_dir.name] = scores_bad

        metrics = compute_metrics(scores_good, scores_bad, thr)
        metrics["category"] = cat_dir.name
        results.append(metrics)

        make_histogram(
            scores_good=scores_good,
            scores_bad=scores_bad,
            thr=thr,
            title=f"Good vs {cat_dir.name}",
            save_path=plots_dir / f"hist_{cat_dir.name}.png",
            bad_label=cat_dir.name,
        )

    results_df = pd.DataFrame(results).sort_values("category").reset_index(drop=True)

    all_bad_scores = np.concatenate(list(scores_by_category.values()))
    overall_metrics = compute_metrics(scores_good, all_bad_scores, thr)
    overall_metrics["category"] = "ALL_BAD"

    results_df = pd.concat([results_df, pd.DataFrame([overall_metrics])], ignore_index=True)

    print("\nPer-category results:")
    print(results_df[["category", "n_bad", "precision", "recall", "f1", "tpr", "fpr"]])

    make_histogram(
        scores_good=scores_good,
        scores_bad=all_bad_scores,
        thr=thr,
        title="Good vs ALL_BAD",
        save_path=plots_dir / "hist_all_bad.png",
        bad_label="ALL_BAD",
    )

    make_combined_bad_histogram(
        scores_good=scores_good,
        scores_by_category=scores_by_category,
        thr=thr,
        title="Good vs All Bad Categories",
        save_path=plots_dir / "hist_all_categories.png",
    )

    results_csv = data_dir / "per_category_metrics.csv"
    results_df.to_csv(results_csv, index=False)

    scores_path = data_dir / "scores_by_category.npz"
    np.savez_compressed(
        scores_path,
        scores_good=scores_good,
        all_bad_scores=all_bad_scores,
        **{f"scores_bad_{k}": v for k, v in scores_by_category.items()},
    )

    mlflow.set_experiment(CONFIG['mlflow']['experiment_name'])

    with mlflow.start_run(run_name=f"multi_category_eval_q{thr_quantile}"):
        mlflow.log_param("model_version", latest_mv.version)
        mlflow.log_param("threshold_quantile", thr_quantile)
        mlflow.log_metric("threshold", float(thr))

        for key, value in overall_metrics.items():
            if key == "category":
                continue
            mlflow.log_metric(f"overall_{key}", float(value))

        mlflow.log_artifact(str(results_csv), artifact_path="testing/data")
        mlflow.log_artifact(str(scores_path), artifact_path="testing/data")
        mlflow.log_artifact(str(plots_dir / "hist_all_bad.png"), artifact_path="testing/plots")
        mlflow.log_artifact(str(plots_dir / "hist_all_categories.png"), artifact_path="testing/plots")

        for cat_dir in category_dirs:
            plot_file = plots_dir / f"hist_{cat_dir.name}.png"
            if plot_file.exists():
                mlflow.log_artifact(str(plot_file), artifact_path=f"testing/plots/{cat_dir.name}")

            category_result_dir = results_root / cat_dir.name
            if category_result_dir.exists():
                mlflow.log_artifacts(str(category_result_dir), artifact_path=f"testing/results/{cat_dir.name}")

        for _, row in results_df.iterrows():
            category = row["category"]
            if category == "ALL_BAD":
                continue

            with mlflow.start_run(run_name=f"category_{category}", nested=True):
                mlflow.log_param("category", category)
                for col, val in row.items():
                    if col == "category":
                        continue
                    if isinstance(val, (int, float, np.integer, np.floating)):
                        mlflow.log_metric(col, float(val))

    print("\nLogged testing run to MLflow.")