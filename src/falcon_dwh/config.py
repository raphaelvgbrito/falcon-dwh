"""Load the YAML config. The path comes from --config or the FALCON_CONFIG env var."""
import os
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PATH = REPO_ROOT / "config" / "config.yaml"


def repo_path(path):
    """Relative paths are relative to the repo root (a job's working directory can be anything)."""
    p = Path(path)
    return p if p.is_absolute() else REPO_ROOT / p


def load_config(path=None, schema=None):
    path = repo_path(path or os.environ.get("FALCON_CONFIG") or DEFAULT_PATH)
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # Optional override so tests / trial runs write to a separate schema without editing the file.
    schema = schema or os.environ.get("FALCON_SCHEMA")
    if schema:
        cfg["warehouse"]["schema"] = schema
    return cfg


def table_name(cfg, name):
    wh = cfg["warehouse"]
    return f"{wh['catalog']}.{wh['schema']}.{name}"


def day_path(cfg, load_date):
    return f"{cfg['input']['base_path'].rstrip('/')}/dt={load_date}"
