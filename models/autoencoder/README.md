# Autoencoder — TPC Quality Control Models

Anomaly detection and classification pipeline for ALICE TPC cluster occupancy maps. The workflow trains a **Linear Autoencoder** (unsupervised) to reconstruct good-quality histograms, then uses per-pixel reconstruction loss maps to drive a supervised **Convolutional Loss-Map Classifier** that identifies the defect category.

---

## Models

| Class | File | Purpose |
|---|---|---|
| `LinearAE` | `model.py` | Fully-connected autoencoder; trained only on good-quality maps |
| `LossMapClassifier` | `model.py` | Conv classifier that takes AE loss maps as input |
| `ConvLossMapClassifier` | `loss_map_classifier.py` | Deeper conv variant of the loss-map classifier |

---

## Scripts

| Script | Description |
|---|---|
| `train_linear_ae.py` | Train the `LinearAE` on ROOT tensor data from QCDB |
| `train_conv_classifier.py` | Train the loss-map classifier (frozen AE backbone) |
| `inference_linear_ae.py` | Score test samples with the trained AE; outputs anomaly scores |
| `inference_conv_classifier.py` | Run the full pipeline (AE → loss map → classifier) on test data |

---

## Quickstart

### 1 — Start MLflow UI

Open a terminal and run:

```bash
mlflow ui \
  --backend-store-uri sqlite:////Users/zetasourpi/cernbox/mlflow-backend/mlflow.db \
  --default-artifact-root /Users/zetasourpi/cernbox/mlflow-backend/mlruns \
  --port 8080
```

The UI will be available at <http://127.0.0.1:8080>.

### 2 — Train

In a second terminal, from the `AIQualityControl/models/autoencoder` directory:

1. Edit `params.yaml` to point dataset paths and tune hyperparameters.
2. Run training:

```bash
uv run python train_linear_ae.py       # unsupervised AE
uv run python train_conv_classifier.py # supervised classifier on top
```

### 3 — Inference

```bash
uv run python inference_linear_ae.py       # AE anomaly scores
uv run python inference_conv_classifier.py # defect classification
```

---

## Configuration (`params.yaml`)

Key sections:

| Section | Controls |
|---|---|
| `qcdb_root_tensor_train_dataset` | Training data path, augmentations, histogram index |
| `linear_model_parametrs` | `latent_dim`, `hidden_dim`, `channels` |
| `supervised_classifier` | Number of classes, base channels, dropout |
| `train` | Epochs, learning rate, early stopping |
| `optuna-hpo` | Hyperparameter search space and pruning settings |
| `mlflow` | Experiment name, run name, tracking URI |

### Defect classes

| Label | Description |
|---|---|
| `good` | Nominal TPC occupancy map |
| `empty_histogram` | All-zero or near-zero map |
| `empty_roc` | One or more ROC sectors missing |
| `holes` | Localised dead-zone clusters |
| `transient_effect` | Temporary distortions / noise bursts |

---

## Optuna HPO

Set `optuna-hpo.enabled: true` in `params.yaml` to run a hyperparameter search over `base_channels`, `lr`, `weight_decay`, and `batch_size`. Results are logged to MLflow and pruned with a median pruner.

---

## Key dependencies

Managed via `uv` — see `pyproject.toml` in the repo root.

- PyTorch
- MLflow
- Optuna
- ROOT (for `.root` tensor loading via `root_tensor_utils.py`)
- scikit-learn (metrics)
