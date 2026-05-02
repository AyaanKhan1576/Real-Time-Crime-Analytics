# Config Workflow

This repository tracks the shared Docker defaults in `config/config.yaml` and keeps `config/config.yaml.example` as a template for new config profiles.

Setup
1. Use `config/config.yaml` for the normal Docker run.
2. Use `config/config.spark_15pct.yaml` for fast Spark testing.
3. Copy `config/config.yaml.example` only when creating a new local profile.

Environment variable overrides
- You can override nested config values using environment variables with double underscores.
- Example: set `KAFKA__BOOTSTRAP_SERVERS=kafka:9092` to override `kafka.bootstrap_servers`.
- Docker host paths are not read from YAML. Use `.env` for `CRIME_ANALYTICS_PROJECT_ROOT` and `CRIME_ANALYTICS_DATA_DIR`.

Usage in code
- Use `from config.config_loader import load_config` and call `load_config()`.
- Loader reads `config/config.yaml` and then applies environment-variable overrides.

Team rule
- Do not hardcode machine-specific paths in Python, Java, Makefile, or Compose files.
- Keep Docker container data paths stable as `/app/data`; customize host paths with `.env`.
