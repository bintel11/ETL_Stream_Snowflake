# app/config.py
import os
from typing import Dict, Any
from .clients.secrets_manager import SecretsManager

def load_config() -> Dict[str, Any]:
    env = os.getenv("ENV", "dev")
    region = os.getenv("AWS_REGION", None)
    secrets_name = os.getenv("SECRETS_NAME")  # e.g. "prod/my-data-pipeline"
    cfg = {"ENV": env}

    if secrets_name:
        sm = SecretsManager(region_name=region)
        secrets = sm.get_secret(secrets_name)
        cfg.update(secrets)
    # fallbacks from environment
    cfg.setdefault("SNOWFLAKE_ACCOUNT", os.getenv("SNOWFLAKE_ACCOUNT"))
    cfg.setdefault("SNOWFLAKE_USER", os.getenv("SNOWFLAKE_USER"))
    cfg.setdefault("SNOWFLAKE_ROLE", os.getenv("SNOWFLAKE_ROLE"))
    cfg.setdefault("API_BASE_URL", os.getenv("API_BASE_URL"))
    return cfg
