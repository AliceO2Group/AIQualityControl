# AI for Data Quality Control

Automated quality-control pipeline for ALICE TPC detector data. The system ingests histogram objects from QCDB and the Bookkeeping API, curates a labelled dataset, and trains deep-learning models to flag anomalous TPC cluster occupancy maps.

---

## Repository layout

```
AIQualityControl/
├── data_curation/          # Fetch, filter, and convert ROOT objects from QCDB & Bookkeeping
├── models/
│   └── autoencoder/        # LinearAE + LossMapClassifier training, inference, HPO
├── data/                   # Local data (not committed — managed via DVC)
├── docs/                   # Project documentation (uv setup, testing guide, certs)
└── pyproject.toml          # Single source of truth for dependencies (uv)
```

---

## Pipeline overview

```
QCDB / Bookkeeping
       │
       ▼
data_curation/          ← filter runs, download ROOT objects, convert to tensors
       │
       ▼
models/autoencoder/
  ├── train_linear_ae.py       ← unsupervised: trains on good-quality maps only
  ├── train_conv_classifier.py ← supervised: frozen AE → loss map → defect class
  ├── inference_linear_ae.py   ← anomaly scores on test data
  └── inference_conv_classifier.py  ← end-to-end defect classification
```

Experiments and artefacts are tracked with **MLflow**.

---

## Setup

### Prerequisites

- [uv](https://astral.sh/uv) — replaces pip/venv (see `docs/getting-started.md` for details)
- [ROOT](https://root.cern) — `brew install root tbb` on macOS
- Bookkeeping `.pem` certificate — place under `data_curation/permissions/` (see `docs/`)

### Install

```bash
git clone <repo-url>
cd AIQualityControl

uv python install 3.12   # optional: pin Python version
uv sync                  # create venv + install all dependencies
```

---

## Quickstart

### 1 — Start MLflow UI

```bash
mlflow ui \
  --backend-store-uri sqlite:////Users/zetasourpi/cernbox/mlflow-backend/mlflow.db \
  --default-artifact-root /Users/zetasourpi/cernbox/mlflow-backend/mlruns \
  --port 8080
```

UI available at <http://127.0.0.1:8080>.

### 2 — Curate data

```bash
cd data_curation
uv run jupyter lab main.ipynb   # or run individual scripts
```

### 3 — Train models

```bash
cd models/autoencoder
# edit params.yaml to set data paths and hyperparameters, then:
uv run python train_linear_ae.py        # step 1: autoencoder
uv run python train_conv_classifier.py  # step 2: classifier on top
```

### 4 — Run inference

```bash
uv run python inference_linear_ae.py        # anomaly scores
uv run python inference_conv_classifier.py  # defect classification
```

---

## Data curation (`data_curation/`)

| Script / Notebook | Purpose |
|---|---|
| `download_data_from_qcdb.py` | Download ROOT histogram objects from QCDB |
| `bookkeeping_fetch_and_filter_runs.py` | Fetch and filter run lists from the Bookkeeping API |
| `filter_qcdb_objects_based_on_bkkp_runs.py` | Keep only objects for validated runs |
| `filter_qcdb_objects_based_on_quality_summaries.py` | Keep only good-quality objects |
| `main.ipynb` | End-to-end curation notebook |

Setup note — ROOT + Jupyter:

```bash
source "$(brew --prefix root)/bin/thisroot.sh"
uv run jupyter lab
```

---

## Models (`models/autoencoder/`)

See [models/autoencoder/README.md](models/autoencoder/README.md) for full details.

**Defect classes detected:**

| Class | Description |
|---|---|
| `good` | Nominal TPC occupancy map |
| `empty_histogram` | All-zero or near-zero map |
| `empty_roc` | One or more ROC sectors missing |
| `holes` | Localised dead-zone clusters |
| `transient_effect` | Temporary distortions / noise bursts |

---

## Dependency management

This project uses **uv**. Common commands:

| Task | Command |
|---|---|
| Install deps | `uv sync` |
| Run a script | `uv run python script.py` |
| Add a dependency | `uv add <pkg>` |
| Update all deps | `uv lock --upgrade && uv sync` |
| Run tests | `uv run pytest` |

Commit both `pyproject.toml` and `uv.lock` after any dependency change.

---

## Documentation

Full docs live under `docs/` and are served with MkDocs:

```bash
uv run mkdocs serve
```
