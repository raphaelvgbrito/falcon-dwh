"""Load the YAML config. The path comes from --config or the FALCON_CONFIG env var."""
import os
from pathlib import Path

import yaml

DEFAULT_PATH = Path(__file__).resolve().parents[2] / "config" / "config.yaml"


def load_config(path=None):
    path = Path(path or os.environ.get("FALCON_CONFIG") or DEFAULT_PATH)
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # Optional override so tests can write to a separate schema without editing the file.
    if os.environ.get("FALCON_SCHEMA"):
        cfg["warehouse"]["schema"] = os.environ["FALCON_SCHEMA"]
    return cfg


def table_name(cfg, name):
    wh = cfg["warehouse"]
    return f"{wh['catalog']}.{wh['schema']}.{name}"


def day_path(cfg, load_date):
    return f"{cfg['input']['base_path'].rstrip('/')}/dt={load_date}"
