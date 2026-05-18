import mlflow
from mlflow.tracking import MlflowClient
import os 
import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
import matplotlib.pyplot as plt
from torch.utils.data import DataLoader, random_split, ConcatDataset, WeightedRandomSampler
from tqdm import tqdm
from sklearn.metrics import precision_score, recall_score, f1_score, fbeta_score


from sklearn.metrics import (
    precision_recall_curve,
    roc_curve,
    roc_auc_score,
    average_precision_score,
    confusion_matrix,
    classification_report,
)

from model import LossMapClassifier
from dataset import QcdbImageDataset, QcdbNpyTensorDataset, LabeledDataset
from utils import load_yaml


def freeze_model(model):
    model.eval()
    for p in model.parameters():
        p.requires_grad = False
    return model


@torch.no_grad()
def compute_loss_map(ae_model, imgs, device):
    """
    imgs: [B, C, H, W]
    returns:
        recon:    [B, C, H, W]
        loss_map: [B, C, H, W] = (imgs - recon)^2
    """
    imgs = imgs.to(device)
    recon = ae_model(imgs)
    # Mean Squared Error 
    loss_map = (imgs - recon) ** 2
    return recon, loss_map


def split_and_label_dataset(dataset, label, train_ratio, split_seed):
    train_size = int(train_ratio * len(dataset))
    val_size = len(dataset) - train_size

    train_subset, val_subset = random_split(
        dataset,
        [train_size, val_size],
        generator=torch.Generator().manual_seed(split_seed),
    )

    train_labeled = LabeledDataset(train_subset, label=label)
    val_labeled = LabeledDataset(val_subset, label=label)
    return train_labeled, val_labeled


def get_dataset_labels(dataset):
    if isinstance(dataset, LabeledDataset):
        return torch.full((len(dataset),), int(dataset.label), dtype=torch.long)

    if isinstance(dataset, ConcatDataset):
        return torch.cat([get_dataset_labels(child) for child in dataset.datasets])

    if isinstance(dataset, DeterministicAugmentedDataset):
        base_labels = get_dataset_labels(dataset.base_dataset)
        return base_labels.repeat_interleave(dataset.num_variants)

    return torch.tensor([int(dataset[idx][1]) for idx in range(len(dataset))], dtype=torch.long)


def compute_class_weights(labels, num_classes):
    # used in criterion = nn.CrossEntropyLoss(weight=class_weights)
    # so that the loss weights the classes appropriately 
    counts = torch.bincount(labels, minlength=num_classes).float()
    
    ideal_count_per_class = labels.numel() / num_classes
    actual_count_per_class = counts.clamp_min(1.0) # clamp values less than 1 to 1 to avoid division by 0 
    
    weights = (ideal_count_per_class / actual_count_per_class) ** 0.5
    weights[counts == 0] = 0.0
    return weights


def build_weighted_sampler(labels, split_seed):
    # this function will change the DataLoader sampling
    # to show rare-class samples more often during training
    counts = torch.bincount(labels).float() # counts[x] is the number of class-x samples
    sample_weights = 1.0 / counts[labels].clamp_min(1.0) # 1 / class_count
    return WeightedRandomSampler(
        weights=sample_weights,
        num_samples=len(sample_weights),
        replacement=True, # after a sample is drawn, it is put back in the pool and can be drawn again 
        generator=torch.Generator().manual_seed(split_seed),
    )


class DeterministicAugmentedDataset(torch.utils.data.Dataset):
    """
    Expand each sample into 4 deterministic views:
    original, vertical flip, horizontal flip, both flips.
    """

    def __init__(self, base_dataset):
        self.base_dataset = base_dataset
        self.num_variants = 4

    def __len__(self):
        return len(self.base_dataset) * self.num_variants

    def __getitem__(self, idx):
        base_idx = idx // self.num_variants
        variant_idx = idx % self.num_variants

        img, label = self.base_dataset[base_idx]

        if variant_idx == 1:
            img = torch.flip(img, dims=[-2])          # vflip
        elif variant_idx == 2:
            img = torch.flip(img, dims=[-1])          # hflip
        elif variant_idx == 3:
            img = torch.flip(img, dims=[-2, -1])      # vflip then hflip

        return img, label


def build_classifier_dataloaders(train_cfg, dl_args, folder_to_label):
    split_seed = train_cfg["split_seed"]
    train_ratio = train_cfg["train_split"]
    image_size = train_cfg.get("image_size")

    # good samples are the ones used at training of the AE 
    good_dataset_kwargs = {"folder": train_cfg["good_samples_folder"]}
    if train_cfg.get("good_samples_limit") is not None:
        good_dataset_kwargs["limit"] = train_cfg["good_samples_limit"]
    if image_size is not None:
        good_dataset_kwargs["image_size"] = image_size
    good_samples_dataset = QcdbImageDataset(**good_dataset_kwargs)
    train_good_labeled, val_good_labeled = split_and_label_dataset(
        good_samples_dataset,
        label=folder_to_label["good"],
        train_ratio=train_ratio,
        split_seed=split_seed,
    )

    bad_root = train_cfg["bad_samples_folders"]
    bad_categ_folders = sorted(
            folder_name
            for folder_name in os.listdir(bad_root)
            if os.path.isdir(os.path.join(bad_root, folder_name)) # keep only the folders/dirs 
        )
    
    train_bad_parts = []
    val_bad_parts = []
    
    for folder_name in bad_categ_folders:
        folder_path = os.path.join(bad_root, folder_name)
        bad_dataset_kwargs = {"folder": folder_path}
        if image_size is not None:
            bad_dataset_kwargs["image_size"] = image_size
        bad_samples_dataset = QcdbImageDataset(**bad_dataset_kwargs)
        train_bad_labeled, val_bad_labeled = split_and_label_dataset(
            bad_samples_dataset,
            label=folder_to_label[str(folder_name)],
            train_ratio=train_ratio,
            split_seed=split_seed,
        )

        train_bad_parts.append(train_bad_labeled)
        val_bad_parts.append(val_bad_labeled)

    total_train_bad_labeled = ConcatDataset(train_bad_parts)
    total_val_bad_labeled = ConcatDataset(val_bad_parts)

    # Augment bad samples only in training; good samples stay at 1x
    train_dataset = ConcatDataset([train_good_labeled, DeterministicAugmentedDataset(total_train_bad_labeled)])
    val_base = ConcatDataset([val_good_labeled, total_val_bad_labeled])
    val_dataset = DeterministicAugmentedDataset(val_base) if train_cfg.get("augment_val", True) else val_base

    train_labels = get_dataset_labels(train_dataset)
    sampler = None
    shuffle = dl_args.get("shuffle", True)
    if train_cfg.get("use_weighted_sampler", False):
        sampler = build_weighted_sampler(train_labels, split_seed=split_seed)
        shuffle = False

    num_workers = dl_args.get("num_workers", 0)
    persistent = num_workers > 0

    train_loader = DataLoader(
        train_dataset,
        batch_size=dl_args["batch_size"],
        shuffle=shuffle,
        sampler=sampler,
        num_workers=num_workers,
        persistent_workers=persistent,
    )

    val_loader = DataLoader(
        val_dataset,
        batch_size=dl_args["batch_size"],
        shuffle=False,
        num_workers=num_workers,
        persistent_workers=persistent,
    )

    return train_dataset, val_dataset, train_loader, val_loader


def tensor_to_display_image(tensor):
    tensor = tensor.detach().cpu().clamp(0, 1)
    if tensor.dim() == 3:
        tensor = tensor.permute(1, 2, 0)
    return tensor.numpy()


@torch.no_grad()
def save_validation_predictions(ae_model, classifier, loader, device, label_to_name, output_dir, max_samples=100):
    os.makedirs(output_dir, exist_ok=True)

    classifier.eval()
    ae_model.eval()

    sample_index = 0
    for imgs, labels in loader:
        if sample_index >= max_samples:
            break
        imgs = imgs.to(device)
        labels = labels.to(device)

        recon_imgs = ae_model(imgs)
        loss_maps = (imgs - recon_imgs) ** 2
        logits = classifier(loss_maps)
        preds = torch.argmax(logits, dim=1)

        for img, recon_img, label, pred in zip(imgs, recon_imgs, labels, preds):
            if sample_index >= max_samples:
                break
            true_name = label_to_name[int(label.item())]
            pred_name = label_to_name[int(pred.item())]

            fig, axes = plt.subplots(1, 2, figsize=(8, 4))
            axes[0].imshow(tensor_to_display_image(img))
            axes[0].set_title(f"Input\ntrue: {true_name}")
            axes[0].axis("off")

            axes[1].imshow(tensor_to_display_image(recon_img))
            axes[1].set_title(f"Output\npred: {pred_name}")
            axes[1].axis("off")

            fig.tight_layout()
            fig.savefig(
                os.path.join(output_dir, f"val_{sample_index:04d}_pred_{pred_name}.png"),
                dpi=150,
                bbox_inches="tight",
            )
            plt.close(fig)
            sample_index += 1



def run_classifier_epoch(ae_model, classifier, loader, criterion, optimizer, device):
            train = optimizer is not None

            if train:
                classifier.train()
            else:
                classifier.eval()

            ae_model.eval()
            
            total_loss, total_correct_samples = 0.0, 0
            total_correct_score, total_samples = 0, 0
            total_loss_per_batch, total_loss_per_sample = 0, 0
            
            all_labels = []
            all_preds = []

            phase = "train" if train else "val"
            pbar = tqdm(loader, desc=phase, leave=False, unit="batch")
            for batch in pbar:
                imgs, labels = batch
                imgs = imgs.to(device)
                labels = labels.to(device)
                
                with torch.no_grad():
                    # Pass input image to frozen pre-trained AE 
                    recon_imgs = ae_model(imgs)
                    # Compute the loss map 
                    loss_maps = (imgs - recon_imgs) ** 2
                
                # Pass loss map to classifier head 
                logits = classifier(loss_maps)
                
                # nn.CrossEntropyLoss internally applies log-softmax, so we don't need to apply it seperately
                # Compare predicted labels with actual labels - our Loss 
                mean_batch_loss = criterion(logits,labels)
                
                if train: 
                    optimizer.zero_grad()
                    # For optimization we want the batch mean loss -> for a stable training
                    mean_batch_loss.backward()
                    optimizer.step()
                    
                # For a correct epoch metric we should compute the total loss weighted by the batch size
                batch_size = imgs.size(0)
                total_loss_per_batch += mean_batch_loss.item() * batch_size
                
                preds = torch.argmax(logits, dim=1)
                all_labels.append(labels.detach().cpu())
                all_preds.append(preds.detach().cpu())
                total_correct_samples += (preds == labels).sum().item()
                total_samples += imgs.size(0)
                pbar.set_postfix(loss=f"{mean_batch_loss.item():.4f}")
                
           # Compute final metrics over the whole epoch
            all_labels = torch.cat(all_labels).numpy()
            all_preds = torch.cat(all_preds).numpy()

            total_loss_per_sample = total_loss_per_batch / total_samples
            accuracy = total_correct_samples / total_samples
            precision = precision_score(all_labels, all_preds, average="macro", zero_division=0)
            recall = recall_score(all_labels, all_preds, average="macro", zero_division=0)
            f1 = f1_score(all_labels, all_preds, average="macro", zero_division=0)
            f2 = fbeta_score(all_labels, all_preds, beta=2, average="macro", zero_division=0)
            cm = confusion_matrix(all_labels, all_preds)
            report = classification_report(all_labels, all_preds, digits=4, zero_division=0)

            # Note: "macro" metric: evaluates each class equally when reporting performance
            metrics = {
                "loss": total_loss_per_sample,
                "acc": accuracy,
                "precision_macro": precision,
                "recall_macro": recall,
                "f1_macro": f1,
                "f2_macro": f2,
            }

            return metrics, cm, report, all_labels, all_preds


def plot_confusion_matrix(cm, class_names, title, output_path):
    cm_norm = cm.astype(float) / cm.sum(axis=1, keepdims=True).clip(min=1)
    n = len(class_names)

    fig, ax = plt.subplots(figsize=(max(5, n * 1.5), max(4, n * 1.3)))
    fig.patch.set_facecolor("white")
    im = ax.imshow(cm_norm, cmap="Blues", vmin=0, vmax=1, interpolation="nearest")

    cbar = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    cbar.set_label("Recall (row-normalised)", fontsize=10)
    cbar.ax.tick_params(labelsize=9)

    ax.set_xticks(np.arange(n))
    ax.set_yticks(np.arange(n))
    ax.set_xticklabels(class_names, rotation=35, ha="right", fontsize=10)
    ax.set_yticklabels(class_names, fontsize=10)
    ax.set_xlabel("Predicted label", fontsize=12, labelpad=8)
    ax.set_ylabel("True label", fontsize=12, labelpad=8)
    ax.set_title(title, fontsize=14, fontweight="bold", pad=14)

    for i in range(n):
        for j in range(n):
            color = "white" if cm_norm[i, j] > 0.5 else "#222222"
            weight = "bold" if i == j else "normal"
            ax.text(j, i, f"{cm[i, j]}\n({cm_norm[i, j]:.0%})",
                    ha="center", va="center", fontsize=9,
                    color=color, fontweight=weight)

    # Grid lines between cells
    ax.set_xticks(np.arange(n) - 0.5, minor=True)
    ax.set_yticks(np.arange(n) - 0.5, minor=True)
    ax.grid(which="minor", color="white", linewidth=1.5)
    ax.tick_params(which="minor", length=0)

    fig.tight_layout()
    fig.savefig(output_path, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  Saved: {output_path}")


if __name__ == "__main__":
    
    FOLDER_TO_LABEL = {
        "good": 0,
        "empty_histogram": 1,
        "empty_roc": 2,
        "holes": 3,
        "transient_effect": 4,
        # "underperforming_region": 5,  # removed, merged into transient_effect
    }
    LABEL_TO_FOLDER = {label: folder for folder, label in FOLDER_TO_LABEL.items()}

    FINE_TO_COARSE = {0: 0, 1: 1, 2: 1, 3: 2, 4: 2}   # good=0, bad=1, medium=2
    COARSE_NAMES   = ["Good", "Bad", "Medium"]
    FINE_NAMES     = [LABEL_TO_FOLDER[i] for i in range(len(FOLDER_TO_LABEL))]
            
    if torch.backends.mps.is_available():
        device = torch.device("mps")
    elif torch.cuda.is_available():
        device = torch.device("cuda")
    else:
        device = torch.device("cpu")

    print("Using device:", device)
    CONFIG = load_yaml("params.yaml")

    SUP_CFG = CONFIG["supervised_classifier"]
    TRAIN_CFG = CONFIG["train_classifier"]
    DL_ARGS = CONFIG["dataloader_args"]

    # Load AE from MLFlow
    mlflow.set_tracking_uri("http://127.0.0.1:8080")
    client = MlflowClient()

    mvs = list(client.search_model_versions(f"name='{SUP_CFG['ae_model_name']}'"))
    if not mvs:
        raise RuntimeError("No model versions found for AE")

    latest_mv = max(mvs, key=lambda mv: int(mv.version))
    print("Latest AE version:", latest_mv.version, latest_mv.source)
    
    mlflow.set_experiment(CONFIG["mlflow"]["experiment_name"])
    ae_model = mlflow.pytorch.load_model(latest_mv.source)
    ae_model = ae_model.to(device)
    ae_model = freeze_model(ae_model)

    # Dataset preparation
    train_dataset, val_dataset, train_loader_itr, val_loader_itr = (
        build_classifier_dataloaders(
            train_cfg=TRAIN_CFG,
            dl_args=DL_ARGS,
            folder_to_label=FOLDER_TO_LABEL,
        )
    )
    
    num_classes = SUP_CFG["num_classes"]
    # train_dataset = ConcatDataset([good (1x), DeterministicAugmentedDataset(bad (4x))])
    aug_bad_ds     = train_dataset.datasets[1]   # DeterministicAugmentedDataset
    pre_aug_labels = get_dataset_labels(ConcatDataset([train_dataset.datasets[0], aug_bad_ds.base_dataset]))
    train_labels   = get_dataset_labels(train_dataset)
    val_labels     = get_dataset_labels(val_dataset)

    pre_counts  = torch.bincount(pre_aug_labels, minlength=num_classes).tolist()
    post_counts = torch.bincount(train_labels,   minlength=num_classes).tolist()
    val_counts  = torch.bincount(val_labels,     minlength=num_classes).tolist()
    class_weights = compute_class_weights(train_labels, num_classes=num_classes)

    augment_val  = TRAIN_CFG.get("augment_val", True)
    val_variants = aug_bad_ds.num_variants if augment_val else 1
    val_aug_note = f"all: x{val_variants}"  if augment_val else "none"

    total_counts = [p + v for p, v in zip(pre_counts, val_counts)]

    train_pct = int(TRAIN_CFG["train_split"] * 100)
    print(f"\nSplit: {train_pct}% train / {100 - train_pct}% val  (seed={TRAIN_CFG['split_seed']})\n")

    col_w = 22
    print(f"{'Category':<{col_w}} {'Total':>8} {'Train (pre-aug)':>16} {'Train (post-aug)':>17} {'Val (no aug)':>13}")
    print("-" * (col_w + 57))
    for i in range(num_classes):
        name = LABEL_TO_FOLDER[i]
        print(f"{name:<{col_w}} {total_counts[i]:>8} {pre_counts[i]:>16} {post_counts[i]:>17} {val_counts[i]:>13}")
    print("-" * (col_w + 57))
    print(f"{'Total':<{col_w}} {sum(total_counts):>8} {sum(pre_counts):>16} {sum(post_counts):>17} {sum(val_counts):>13}")
    print(f"{'Augmentation':<{col_w}} {'':>8} {'good: none':>16} {f'bad: x{aug_bad_ds.num_variants}':>17} {val_aug_note:>13}")
    print(f"{'Class weights':<{col_w}} {[round(w,3) for w in class_weights.tolist()]!s}")
    print()

    # Classifier
    classifier = LossMapClassifier(
        channels=SUP_CFG['channels'],
        num_classes=SUP_CFG['num_classes'],
        base_channels=SUP_CFG.get('base_channels', 32),
        dropout=SUP_CFG.get('dropout', 0.5),
    ).to(device)

    criterion = nn.CrossEntropyLoss(
        weight=class_weights.to(device) if TRAIN_CFG.get("use_class_weights", False) else None,
        reduction="mean"
    )
    ae_criterion = nn.MSELoss(reduction="none")
    
    optimizer = torch.optim.Adam(
        classifier.parameters(),
        lr=TRAIN_CFG["lr"],
        weight_decay=TRAIN_CFG.get("weight_decay", 0.0),
    )

    epochs = TRAIN_CFG["epochs"]

    # MLflow run
    best_val_loss = float("inf")
    best_path = "best_lossmap_classifier.pt"
    total_train_loss = 0 
    
    with mlflow.start_run(run_name=TRAIN_CFG["run_name"]):
        mlflow.log_params({
            "ae_model_name": SUP_CFG["ae_model_name"],
            "num_classes": num_classes,
            "epochs": epochs,
            "lr": TRAIN_CFG["lr"],
            "weight_decay": TRAIN_CFG.get("weight_decay", 0.0),
            "batch_size": DL_ARGS["batch_size"],
            "device": str(device),
            "use_class_weights": TRAIN_CFG.get("use_class_weights", False),
            "use_weighted_sampler": TRAIN_CFG.get("use_weighted_sampler", False),
            "train_split": TRAIN_CFG["train_split"],
            "split_seed": TRAIN_CFG["split_seed"],
            "good_samples_limit": str(TRAIN_CFG.get("good_samples_limit")),
            "image_size": str(TRAIN_CFG.get("image_size")),
            "augment_val": TRAIN_CFG.get("augment_val", False),
            "bad_train_aug_variants": aug_bad_ds.num_variants,
        })
        
        early_stopping_patience = TRAIN_CFG.get("early_stopping_patience", 5)
        epochs_without_improvement = 0
        try:
            epoch_bar = tqdm(range(epochs), desc="Epochs", unit="epoch")
            for epoch in epoch_bar:

                train_metrics, *_ = run_classifier_epoch(
                                ae_model=ae_model,
                                classifier=classifier,
                                loader=train_loader_itr,
                                criterion=criterion,
                                optimizer=optimizer,
                                device=device,
                            )

                val_metrics, val_cm, val_report, *_ = run_classifier_epoch(
                    ae_model=ae_model,
                    classifier=classifier,
                    loader=val_loader_itr,
                    criterion=criterion,
                    optimizer=None,
                    device=device,
                )

                epoch_bar.set_postfix(
                    train_loss=f"{train_metrics['loss']:.4f}",
                    val_loss=f"{val_metrics['loss']:.4f}",
                    val_acc=f"{val_metrics['acc']:.4f}",
                )

                for metric_name, metric_value in train_metrics.items():
                    mlflow.log_metric(f"train_{metric_name}", metric_value, step=epoch)

                for metric_name, metric_value in val_metrics.items():
                    mlflow.log_metric(f"val_{metric_name}", metric_value, step=epoch)

                # Apply early-stopping functionality 
                if val_metrics["loss"] < best_val_loss:
                    best_val_loss = val_metrics["loss"]
                    epochs_without_improvement = 0
                    torch.save(classifier.state_dict(), best_path)
                    mlflow.log_artifact(best_path, artifact_path="classifier_ckpt")
                    np.savetxt("best_val_confusion_matrix.txt", val_cm, fmt="%d")
                    with open("best_val_classification_report.txt", "w") as f:
                        f.write(val_report)
                    mlflow.log_artifact("best_val_confusion_matrix.txt", artifact_path="best_val_metrics")
                    mlflow.log_artifact("best_val_classification_report.txt", artifact_path="best_val_metrics")
                else:
                    epochs_without_improvement += 1

                if epochs_without_improvement >= early_stopping_patience:
                    print(
                        f"Early stopping at epoch {epoch+1:02d}/{epochs} | "
                        f"best_val_loss={best_val_loss:.4f}"
                    )
                    break
        except KeyboardInterrupt:
            mlflow.set_tag("interrupted", True)
            print("\nKeyboardInterrupt received, saving current classifier state.")
        if not os.path.exists(best_path):
            torch.save(classifier.state_dict(), best_path)
            mlflow.log_artifact(best_path, artifact_path="classifier_ckpt")

        # log trained classifier model
        classifier.load_state_dict(torch.load(best_path, map_location=device))
        classifier.eval()
        sample_imgs, _ = next(iter(val_loader_itr))
        sample_imgs = sample_imgs.to(device)
        with torch.no_grad():
            sample_recon = ae_model(sample_imgs)
            sample_lossmap = (sample_imgs - sample_recon) ** 2
            sample_logits = classifier(sample_lossmap)
        signature = mlflow.models.infer_signature(
            sample_lossmap.cpu().numpy(),
            sample_logits.cpu().numpy(),
        )
        mlflow.pytorch.log_model(
            pytorch_model=classifier,
            artifact_path="lossmap_classifier",
            signature=signature,
        )

        save_validation_predictions(
            ae_model=ae_model,
            classifier=classifier,
            loader=val_loader_itr,
            device=device,
            label_to_name=LABEL_TO_FOLDER,
            output_dir="validation_predictions",
            max_samples=TRAIN_CFG.get("save_val_predictions_max", 100),
        )

        # ── Final evaluation on best checkpoint ──────────────────────────
        print("\n─── Final evaluation (best checkpoint) ───")
        final_metrics, _, _, final_labels, final_preds = run_classifier_epoch(
            ae_model=ae_model,
            classifier=classifier,
            loader=val_loader_itr,
            criterion=criterion,
            optimizer=None,
            device=device,
        )
        for k, v in final_metrics.items():
            mlflow.log_metric(f"final_{k}", v)

        # Fine-grained report
        fine_report = classification_report(
            final_labels, final_preds, target_names=FINE_NAMES, digits=4, zero_division=0
        )
        fine_cm = confusion_matrix(final_labels, final_preds)
        print("\nFine-grained classification report:")
        print(fine_report)
        print("Fine-grained confusion matrix:")
        print(fine_cm)

        # Coarse report
        coarse_labels = np.array([FINE_TO_COARSE[l] for l in final_labels])
        coarse_preds  = np.array([FINE_TO_COARSE[p] for p in final_preds])
        coarse_report = classification_report(
            coarse_labels, coarse_preds, target_names=COARSE_NAMES, digits=4, zero_division=0
        )
        coarse_cm = confusion_matrix(coarse_labels, coarse_preds)
        print("\nCoarse classification report (Good / Bad / Medium):")
        print(coarse_report)
        print("Coarse confusion matrix:")
        print(coarse_cm)

        # Save text artefacts
        for fname, content in [
            ("final_classification_report_fine.txt",   fine_report),
            ("final_classification_report_coarse.txt", coarse_report),
        ]:
            with open(fname, "w") as f:
                f.write(content)
            mlflow.log_artifact(fname, artifact_path="final_metrics")

        np.savetxt("final_confusion_matrix_fine.txt",   fine_cm,   fmt="%d")
        np.savetxt("final_confusion_matrix_coarse.txt", coarse_cm, fmt="%d")
        mlflow.log_artifact("final_confusion_matrix_fine.txt",   artifact_path="final_metrics")
        mlflow.log_artifact("final_confusion_matrix_coarse.txt", artifact_path="final_metrics")

        # Plot confusion matrices
        print("\nSaving confusion matrix plots:")
        plot_confusion_matrix(
            fine_cm, FINE_NAMES,
            title="Fine-grained confusion matrix (val set)",
            output_path="final_confusion_matrix_fine.png",
        )
        plot_confusion_matrix(
            coarse_cm, COARSE_NAMES,
            title="Coarse confusion matrix — Good / Bad / Medium (val set)",
            output_path="final_confusion_matrix_coarse.png",
        )
        mlflow.log_artifact("final_confusion_matrix_fine.png",   artifact_path="final_metrics")
        mlflow.log_artifact("final_confusion_matrix_coarse.png", artifact_path="final_metrics")

        print("Training done.")
        print("Best classifier saved to:", best_path)
