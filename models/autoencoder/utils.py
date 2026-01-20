import os 
import random
from typing import Dict, Any
import numpy as np
import torch
from pathlib import Path
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple
import yaml
import mlflow
import subprocess
import torch


def seed_everything(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)


def flatten_dict(d: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """Flatten nested dicts for MLflow params."""
    out = {}
    for k, v in d.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            out.update(flatten_dict(v, key))
        else:
            # MLflow params are best as str/int/float/bool
            out[key] = v
    return out




def _git(cmd: list[str]) -> str:
    return subprocess.check_output(["git", *cmd], text=True).strip()

def log_git_to_mlflow(log_diff: bool = False) -> None:
    try:
        commit = _git(["rev-parse", "HEAD"])
        branch = _git(["rev-parse", "--abbrev-ref", "HEAD"])
        is_dirty = subprocess.call(["git", "diff", "--quiet"]) != 0 or \
                   subprocess.call(["git", "diff", "--cached", "--quiet"]) != 0

        # searchable/filterable in the MLflow UI
        mlflow.set_tags({
            "git.commit": commit,
            "git.branch": branch,
            "git.dirty": str(is_dirty).lower(),
        })

        # Optional: remote URL
        try:
            remote = _git(["remote", "get-url", "origin"])
            mlflow.set_tag("git.remote.origin", remote)
        except Exception:
            pass

        if log_diff and is_dirty:
            diff = subprocess.check_output(["git", "diff"], text=True)
            mlflow.log_text(diff, artifact_file="git_diff.patch")

    except Exception:
        # If not in a git repo or git not installed, just skip quietly
        mlflow.set_tag("git.info", "unavailable")




def load_yaml(path: str | Path) -> dict:
    path = Path(path).expanduser().resolve()

    if not path.exists():
        raise FileNotFoundError(f"YAML config not found: {path}")

    text = path.read_text()
    if not text.strip():
        raise ValueError(f"YAML config is empty: {path}")

    cfg = yaml.safe_load(text)

    if cfg is None:
        raise ValueError(f"YAML parsed to None (empty/invalid): {path}")
    if not isinstance(cfg, dict):
        raise TypeError(f"Top-level YAML must be a dict, got {type(cfg)} in {path}")

    # helpful debug
    print(f"[load_yaml] Loaded {path}")
    return cfg

def resolve_repo_root(repo_root: str | Path | None = None) -> Path:
    return Path(repo_root).resolve() if repo_root is not None else Path.cwd().resolve()

