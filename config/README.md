# Config Workflow

This repository tracks `config/config.yaml.example` and ignores `config/config.yaml`.

Setup
1. Copy `config/config.yaml.example` to `config/config.yaml`.
2. Edit `config/config.yaml` with your local paths, ports, and credentials.
3. Since `config/config.yaml` is ignored, teammates can keep their own values with no merge conflicts.

Environment variable overrides
- You can override nested config values using environment variables with double underscores.
- Example: set `KAFKA__BOOTSTRAP_SERVERS=localhost:9092` to override `kafka.bootstrap_servers`.

Usage in code
- Use `from config.config_loader import load_config` and call `load_config()`.
- Loader reads `config/config.yaml` and then applies environment-variable overrides.

Team rule
- Commit shared config changes to `config/config.yaml.example` only.
- Never commit secrets or machine-specific values.
