# app/clients/secrets_manager.py
from typing import Dict, Any
import boto3
import json
from botocore.exceptions import ClientError
from app.logger import get_logger

logger = get_logger(__name__)


class SecretsManager:
    def __init__(self, region_name: str = None):
        self.client = boto3.client("secretsmanager", region_name=region_name)

    def get_secret(self, secret_name: str) -> Dict[str, Any]:
        try:
            resp = self.client.get_secret_value(SecretId=secret_name)
            secret = resp.get("SecretString")
            if secret:
                return json.loads(secret)
            else:
                # binary secret not expected
                return {}
        except ClientError as e:
            logger.exception(
                "Failed to retrieve secret", extra={"secret_name": secret_name}
            )
            raise
