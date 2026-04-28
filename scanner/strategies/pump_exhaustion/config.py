from typing import Dict


def get_pump_cfg(cfg: Dict) -> Dict:
    """Return pump_exhaustion section from root config."""
    return cfg.get("pump_exhaustion", {})
