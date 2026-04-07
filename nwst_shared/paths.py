"""Deterministic paths for modules in this folder (no directory walk in each app)."""

from __future__ import annotations

from pathlib import Path

NWST_ACCENT_CONFIG_PY = Path(__file__).resolve().parent / "nwst_accent_config.py"


def resolved_nwst_accent_config_path() -> Path | None:
    return NWST_ACCENT_CONFIG_PY if NWST_ACCENT_CONFIG_PY.is_file() else None
