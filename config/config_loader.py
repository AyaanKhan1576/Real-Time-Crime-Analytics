import os
import yaml
from copy import deepcopy


def deep_merge(a, b):
    """Recursively merge dict b into dict a and return result."""
    if not isinstance(b, dict):
        return b
    result = deepcopy(a)
    for k, v in b.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = deep_merge(result[k], v)
        else:
            result[k] = deepcopy(v)
    return result


def load_yaml(path):
    if not os.path.exists(path):
        return {}
    with open(path, 'r') as f:
        return yaml.safe_load(f) or {}


def overlay_env_vars(cfg):
    # Support ENV vars like KAFKA__BOOTSTRAP_SERVERS to set cfg['kafka']['bootstrap_servers']
    for key, val in os.environ.items():
        if '__' in key:
            parts = key.lower().split('__')
            node = cfg
            for p in parts[:-1]:
                if p not in node or not isinstance(node[p], dict):
                    node[p] = {}
                node = node[p]
            node[parts[-1]] = _coerce_env_value(val)
    return cfg


def _coerce_env_value(val):
    # Try to coerce common types
    if val.lower() in ('true', 'false'):
        return val.lower() == 'true'
    try:
        if '.' in val:
            return float(val)
        return int(val)
    except Exception:
        return val


def load_config(base_path='config/config.yaml', local_path='config/config.local.yaml'):
    base = load_yaml(base_path)
    local = load_yaml(local_path)
    merged = deep_merge(base, local)
    merged = overlay_env_vars(merged)
    return merged


if __name__ == '__main__':
    import pprint
    pprint.pprint(load_config())
