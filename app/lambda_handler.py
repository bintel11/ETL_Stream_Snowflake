# app/lambda_handler.py

import json
from app.config import load_config
from app.logger import get_logger
from app.clients.http_client import HttpClient
from app.connectors.snowflake_client import SnowflakeClient
from app.etl.pipeline import ETLPipeline

logger = get_logger(__name__)

cfg = load_config()


def build_clients(cfg):
    http_client = HttpClient(base_url=cfg.get("API_BASE_URL"))
    sf_client = SnowflakeClient(
        account=cfg["SNOWFLAKE_ACCOUNT"],
        user=cfg["SNOWFLAKE_USER"],
        password=cfg["SNOWFLAKE_PASSWORD"],
        warehouse=cfg["SNOWFLAKE_WAREHOUSE"],
        database=cfg["SNOWFLAKE_DATABASE"],
        schema=cfg["SNOWFLAKE_SCHEMA"],
        role=cfg.get("SNOWFLAKE_ROLE"),
    )
    return http_client, sf_client


# Lambda handler
def handler(event, context):
    logger.info("lambda:invoked", extra={"event": event})
    try:
        http_client, sf_client = build_clients(cfg)
        pipeline = ETLPipeline(
            http_client=http_client, snowflake_client=sf_client, cfg=cfg
        )
        # determine path from event (API Gateway, scheduled, Step Functions input)
        path = event.get("path", cfg.get("DEFAULT_API_PATH", "/data"))
        pipeline.run(path=path)
        return {"statusCode": 200, "body": json.dumps({"message": "success"})}
    except Exception as e:
        logger.exception("lambda:failed")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "failed", "error": str(e)}),
        }
