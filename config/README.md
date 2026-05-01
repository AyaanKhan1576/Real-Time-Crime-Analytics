# Config: per-developer overrides

Use `config/config.yaml` as the canonical project configuration. To avoid conflicts when multiple developers have different local paths, credentials, or ports, do not edit `config/config.yaml` in your commits.

Local developer overrides
1. Copy `config/config.local.yaml.example` to `config/config.local.yaml`.
2. Update only the fields you need (e.g., `data.base_path`, `postgres.host`, `kafka.bootstrap_servers`).
3. `config/config.local.yaml` is listed in `.gitignore` and will not be committed.

Environment variable overrides
- You can override nested config values using environment variables with double underscores.
  Example: set `KAFKA__BOOTSTRAP_SERVERS=localhost:9092` to override `kafka.bootstrap_servers`.

Usage in code
- Use `from config.config_loader import load_config` and call `load_config()`; it merges:
  1. `config/config.yaml`
  2. `config/config.local.yaml` (if present)
  3. environment variable overrides

Notes
- Keep shared keys (topic names, table names) in `config/config.yaml` only. Local paths/credentials belong in the local override file.
- If you must commit a change to a shared key, coordinate via PR and update teammates.
