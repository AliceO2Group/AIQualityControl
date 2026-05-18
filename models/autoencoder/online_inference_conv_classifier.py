import os
import warnings
warnings.filterwarnings("ignore", message="CPyCppyy API not found")
import mlflow
from mlflow.tracking import MlflowClient
import torch
import numpy as np
import matplotlib.pyplot as plt
from torch.utils.data import DataLoader, ConcatDataset, random_split
from sklearn.metrics import classification_report, confusion_matrix

from tqdm import tqdm
from model import LossMapClassifier
from dataset import QcdbImageDataset, LabeledDataset
from utils import load_yaml


class DeterministicAugmentedDataset(torch.utils.data.Dataset):
    """Expands each sample into 3 deterministic views: original, x-flip, y-flip."""

    def __init__(self, base_dataset):
        self.base_dataset = base_dataset
        self.num_variants = 2

    def __len__(self):
        return len(self.base_dataset) * self.num_variants

    def __getitem__(self, idx):
        base_idx = idx // self.num_variants
        variant_idx = idx % self.num_variants
        img, label = self.base_dataset[base_idx]
        if variant_idx == 1:
            img = torch.flip(img, dims=[-1])
        elif variant_idx == 2:
            img = torch.flip(img, dims=[-2])
        return img, label


FOLDER_TO_LABEL = {
    "good": 0,
    "empty_histogram": 1,
    "empty_roc": 2,
    "holes": 3,
    "transient_effect": 4,
    # "underperforming_region": 5,  # removed, merged into transient_effect
}
LABEL_TO_FOLDER = {v: k for k, v in FOLDER_TO_LABEL.items()}

FINE_TO_COARSE = { # mapping to operational labels 
    0: 0,  # good -> Good
    1: 1,  # empty_histogram -> Bad
    2: 1,  # empty_roc -> Bad
    3: 2,  # holes -> Medium
    4: 2,  # transient_effect -> Medium (includes former underperforming_region)
    # 5: 2,  # underperforming_region removed, merged into transient_effect
}
COARSE_LABEL_NAMES = ["Good", "Bad", "Medium"]


def coarse_name(fine_label: int) -> str:
    return COARSE_LABEL_NAMES[FINE_TO_COARSE[fine_label]]


def tensor_to_display_image(tensor):
    tensor = tensor.detach().cpu().clamp(0, 1)
    if tensor.dim() == 3:
        tensor = tensor.permute(1, 2, 0)
    return tensor.numpy()


def build_inference_dataset(data_root, image_size, good_samples_cfg=None):
    parts = []

    if good_samples_cfg is not None:
        good_ds = QcdbImageDataset(
            folder=good_samples_cfg["folder"],
            image_size=image_size,
            limit=good_samples_cfg.get("limit"),
        )
        train_size = int(good_samples_cfg["train_split"] * len(good_ds))
        val_size = len(good_ds) - train_size
        _, val_good = random_split(
            good_ds,
            [train_size, val_size],
            generator=torch.Generator().manual_seed(good_samples_cfg["split_seed"]),
        )
        parts.append(LabeledDataset(val_good, label=FOLDER_TO_LABEL["good"]))
        print(f"  good (val split): {val_size} samples (label=0)")

    for folder_name in sorted(os.listdir(data_root)):
        folder_path = os.path.join(data_root, folder_name)
        if not os.path.isdir(folder_path):
            continue
        if folder_name not in FOLDER_TO_LABEL:
            print(f"  Skipping unknown folder: {folder_name}")
            continue
        label = FOLDER_TO_LABEL[folder_name]
        ds = QcdbImageDataset(folder=folder_path, image_size=image_size)
        parts.append(LabeledDataset(ds, label=label))
        print(f"  {folder_name}: {len(ds)} samples (label={label})")
    if not parts:
        raise RuntimeError(f"No recognized class subfolders found in {data_root}")
    return ConcatDataset(parts)


def get_original_path(dataset, idx):
    """Trace ConcatDataset -> LabeledDataset -> Subset -> QcdbImageDataset to get the filename."""
    from torch.utils.data import ConcatDataset, Subset
    if isinstance(dataset, ConcatDataset):
        for ds, cum in zip(dataset.datasets, dataset.cumulative_sizes):
            if idx < cum:
                return get_original_path(ds, idx - (cum - len(ds)))
    if isinstance(dataset, LabeledDataset):
        return get_original_path(dataset.base_dataset, idx)
    if isinstance(dataset, Subset):
        return get_original_path(dataset.dataset, dataset.indices[idx])
    if hasattr(dataset, "paths"):
        return dataset.paths[idx]
    return None


def compute_skeleton_mask(ae_model, good_folder, image_size, device, n_samples=100, mask_percentile=80):
    """Average pixel-wise loss over good samples + augmentations; zero out the top-percentile (detector skeleton)."""
    aug_fns = [
        lambda x: x,
        lambda x: torch.flip(x, dims=[-2]),
        lambda x: torch.flip(x, dims=[-1]),
        lambda x: torch.flip(x, dims=[-2, -1]),
    ]
    ds_ref = QcdbImageDataset(folder=good_folder, image_size=image_size, limit=n_samples)
    acc, count = None, 0
    with torch.no_grad():
        for idx in range(len(ds_ref)):
            for aug in aug_fns:
                img_in = aug(ds_ref[idx]).unsqueeze(0).to(device)
                recon = ae_model(img_in)
                loss_2d = ((img_in - recon) ** 2).squeeze(0).mean(0).detach().cpu()
                acc = loss_2d if acc is None else acc + loss_2d
                count += 1
    struct_norm = acc / count
    struct_norm = struct_norm / (struct_norm.max() + 1e-8)
    threshold = torch.quantile(struct_norm, mask_percentile / 100)
    keep_mask = (struct_norm <= threshold).float().to(device)  # [H, W]: 0 on skeleton, 1 elsewhere
    n_zeroed = int((keep_mask == 0).sum())
    print(f"Skeleton mask: {n_zeroed}/{keep_mask.numel()} pixels zeroed ({n_zeroed / keep_mask.numel() * 100:.1f}%, p{mask_percentile} threshold)")
    return keep_mask


@torch.no_grad()
def run_inference(ae_model, classifier, loader, device, output_dir, dataset=None, save_misclassifications_only=False, skeleton_mask=None):
    for name in FOLDER_TO_LABEL:
        os.makedirs(os.path.join(output_dir, name), exist_ok=True)

    ae_model.eval()
    classifier.eval()

    all_labels = []
    all_preds = []
    misclassified = []
    sample_index = 0

    for imgs, labels in tqdm(loader, desc="Inference", unit="batch"):
        imgs = imgs.to(device)
        labels = labels.to(device)

        recon_imgs = ae_model(imgs)
        loss_maps = (imgs - recon_imgs) ** 2
        if skeleton_mask is not None:
            loss_maps = loss_maps * skeleton_mask
        logits = classifier(loss_maps)
        preds = torch.argmax(logits, dim=1)

        all_labels.append(labels.cpu())
        all_preds.append(preds.cpu())

        for img, recon_img, loss_map, label, pred in zip(imgs, recon_imgs, loss_maps, labels, preds):
            true_name = LABEL_TO_FOLDER[int(label.item())]
            pred_name = LABEL_TO_FOLDER[int(pred.item())]
            correct = label == pred

            if not correct:
                path = get_original_path(dataset, sample_index) if dataset is not None else None
                fname_orig = os.path.basename(path) if path else f"idx_{sample_index}"
                misclassified.append((fname_orig, true_name, pred_name))

            if not save_misclassifications_only or not correct:
                loss_display = loss_map.mean(dim=0).cpu().numpy()
                fig, axes = plt.subplots(1, 3, figsize=(12, 4))
                axes[0].imshow(tensor_to_display_image(img))
                axes[0].set_title(f"Input — true: {true_name}")
                axes[0].axis("off")
                axes[1].imshow(tensor_to_display_image(recon_img))
                axes[1].set_title(f"Reconstruction — pred: {pred_name} ({'OK' if correct else 'WRONG'})")
                axes[1].axis("off")
                im = axes[2].imshow(loss_display, cmap="hot")
                axes[2].set_title("Loss map")
                axes[2].axis("off")
                fig.colorbar(im, ax=axes[2], fraction=0.046, pad=0.04)
                fig.tight_layout()
                out_fname = f"{sample_index:04d}_pred_{pred_name}{'_OK' if correct else '_WRONG'}.png"
                fig.savefig(os.path.join(output_dir, true_name, out_fname), dpi=150, bbox_inches="tight")
                plt.close(fig)

            sample_index += 1

    print(f"\n─── Misclassified samples ({len(misclassified)}) ───")
    for fname_orig, true_name, pred_name in misclassified:
        print(f"  {fname_orig}  |  true: {true_name}  →  pred: {pred_name}")

    return torch.cat(all_labels).numpy(), torch.cat(all_preds).numpy()


def main():
    config = load_yaml("params.yaml")
    cfg = config["inference_conv_classifier"]
    mlflow_cfg = config["mlflow"]

    if torch.backends.mps.is_available():
        device = torch.device("mps")
    elif torch.cuda.is_available():
        device = torch.device("cuda")
    else:
        device = torch.device("cpu")
    print("Device:", device)

    mlflow.set_tracking_uri(mlflow_cfg["tracking"]["server_uri"])
    client = MlflowClient()

    # Load AE
    ae_versions = list(client.search_model_versions(f"name='{cfg['ae_model_name']}'"))
    if not ae_versions:
        raise RuntimeError(f"No MLflow versions found for AE model '{cfg['ae_model_name']}'")
    latest_ae = max(ae_versions, key=lambda mv: int(mv.version))
    print(f"Loading AE '{cfg['ae_model_name']}' version {latest_ae.version}")
    ae_model = mlflow.pytorch.load_model(latest_ae.source).to(device).eval()
    for p in ae_model.parameters():
        p.requires_grad = False

    # Load conv classifier
    clf_versions = list(client.search_model_versions(f"name='{cfg['classifier_model_name']}'"))
    if not clf_versions:
        raise RuntimeError(f"No MLflow versions found for classifier '{cfg['classifier_model_name']}'")
    latest_clf = max(clf_versions, key=lambda mv: int(mv.version))
    print(f"Loading conv classifier '{cfg['classifier_model_name']}' version {latest_clf.version}")
    classifier = mlflow.pytorch.load_model(latest_clf.source).to(device).eval()

    # Dataset
    image_size = tuple(cfg["image_size"])
    print(f"Building dataset from {cfg['test_data_root']}")
    dataset = build_inference_dataset(cfg["test_data_root"], image_size, cfg.get("good_samples"))
    if cfg.get("augment", False):
        dataset = DeterministicAugmentedDataset(dataset)
        print(f"Augmentation enabled: {len(dataset)} samples (3x)")
    loader = DataLoader(
        dataset,
        batch_size=cfg.get("batch_size", 16),
        shuffle=False,
        num_workers=0,
    )
    print(f"Total samples: {len(dataset)}")

    # Skeleton mask
    skeleton_mask = None
    good_samples_cfg = cfg.get("good_samples")
    if good_samples_cfg is not None:
        print("Computing skeleton mask from good samples...")
        skeleton_mask = compute_skeleton_mask(ae_model, good_samples_cfg["folder"], image_size, device)

    # Run
    output_dir = cfg["output_dir"]
    save_misclassifications_only = cfg.get("save_misclassifications_only", False)
    print(f"Running inference. Results at: {output_dir}  (misclassifications only: {save_misclassifications_only})")
    all_labels, all_preds = run_inference(ae_model, classifier, loader, device, output_dir, dataset, save_misclassifications_only, skeleton_mask)

    # # Metrics
    # class_names = [LABEL_TO_FOLDER[i] for i in range(len(FOLDER_TO_LABEL))]
    # report = classification_report(all_labels, all_preds, target_names=class_names, digits=4, zero_division=0)
    # cm = confusion_matrix(all_labels, all_preds)

    # print("\nClassification Report:")
    # print(report)
    # print("Confusion Matrix:")
    # print(cm)

    # report_path = os.path.join(output_dir, "classification_report.txt")
    # cm_path = os.path.join(output_dir, "confusion_matrix.txt")
    # with open(report_path, "w") as f:
    #     f.write(report)
    # np.savetxt(cm_path, cm, fmt="%d")
    # print(f"\nSaved report  -> {report_path}")
    # print(f"Saved cm     ->  {cm_path}")

    # Coarse evaluation (Good / Bad / Medium)
    coarse_labels = np.array([FINE_TO_COARSE[l] for l in all_labels])
    coarse_preds  = np.array([FINE_TO_COARSE[p] for p in all_preds])
    # coarse_report = classification_report(
    #     coarse_labels, coarse_preds, target_names=COARSE_LABEL_NAMES, digits=4, zero_division=0
    # )
    # coarse_cm = confusion_matrix(coarse_labels, coarse_preds)

    # print("\nCoarse Classification Report (Good / Bad / Medium):")
    # print(coarse_report)
    # print("Coarse Confusion Matrix:")
    # print(coarse_cm)

    # coarse_report_path = os.path.join(output_dir, "classification_report_coarse.txt")
    # coarse_cm_path     = os.path.join(output_dir, "confusion_matrix_coarse.txt")
    # with open(coarse_report_path, "w") as f:
    #     f.write(coarse_report)
    # np.savetxt(coarse_cm_path, coarse_cm, fmt="%d")
    # print(f"Saved coarse report -> {coarse_report_path}")
    # print(f"Saved coarse cm     -> {coarse_cm_path}")

    if cfg.get("log_to_mlflow", False):
        mlflow.set_experiment(mlflow_cfg["experiment_name"])
        with mlflow.start_run(run_name=cfg.get("run_name", "conv-classifier-inference")):
            # mlflow.log_artifact(report_path)
            # mlflow.log_artifact(cm_path)
            # mlflow.log_artifact(coarse_report_path)
            # mlflow.log_artifact(coarse_cm_path)
            mlflow.log_artifacts(output_dir, artifact_path="predictions")
        print("Artifacts logged to MLflow.")


def main_online():
    """Online inference: fetch from QCDB, run model, publish prediction."""
    from online_data_preparation import prepare_tensors
    from online_publishing import publish_predictions

    config = load_yaml("params.yaml")
    cfg = config["online_inference"]

    device = torch.device("mps" if torch.backends.mps.is_available() else "cuda" if torch.cuda.is_available() else "cpu")
    print("Device:", device)

    ae_model = torch.load(cfg["ae_model_path"], map_location=device, weights_only=False).eval()
    for p in ae_model.parameters():
        p.requires_grad = False
    classifier = torch.load(cfg["classifier_model_path"], map_location=device, weights_only=False).eval()

    image_size = tuple(cfg.get("image_size", [330, 330]))
    pad_indices = tuple(cfg.get("pad_indices", [0, 1]))
    tensors = prepare_tensors(cfg["ccdb_url"], cfg["qc_source_path"], image_size, pad_indices)

    predictions = []
    for pad_idx, tensor in zip(pad_indices, tensors):
        img = tensor.unsqueeze(0).to(device)
        with torch.no_grad():
            loss_map = (img - ae_model(img)) ** 2
            fine_label = int(torch.argmax(classifier(loss_map), dim=1).item())
        
        predictions.append({
            "pad_index": pad_idx,
            "fine_label": fine_label,
            "fine_name": LABEL_TO_FOLDER[fine_label],
            "coarse_name": coarse_name(fine_label),
        })
        print(f"  Pad {pad_idx}: {predictions[-1]['fine_name']} → {predictions[-1]['coarse_name']}")

    result = publish_predictions(predictions, cfg)
    print(f"\nFinal prediction: {result}")
    return result

if __name__ == "__main__":
    main()
