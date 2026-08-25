"""Export the latest MLflow model versions to .pt files for offline inference."""
import torch
import mlflow
from mlflow.tracking import MlflowClient
from pathlib import Path
from utils import load_yaml

config = load_yaml("params.yaml")
mlflow_cfg = config["mlflow"]
online_cfg = config["online_inference"]

mlflow.set_tracking_uri(mlflow_cfg["tracking"]["server_uri"])
client = MlflowClient()

def export(model_name: str, out_path: str):
    versions = list(client.search_model_versions(f"name='{model_name}'"))
    if not versions:
        raise RuntimeError(f"No MLflow versions found for '{model_name}'")
    latest = max(versions, key=lambda v: int(v.version))
    print(f"Exporting '{model_name}' v{latest.version} → {out_path}")
    model = mlflow.pytorch.load_model(latest.source)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    torch.save(model, out_path)
    print(f"  Saved.")

export(config["inference_conv_classifier"]["ae_model_name"],  online_cfg["ae_model_path"])
export(config["inference_conv_classifier"]["classifier_model_name"], online_cfg["classifier_model_path"])
