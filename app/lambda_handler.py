import logging
from pythonjsonlogger import json  # ✅ FIX: use json instead of jsonlogger
from app.etl.pipeline import ETLPipeline


logger = logging.getLogger("app.lambda_handler")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = json.JsonFormatter()
handler.setFormatter(formatter)
logger.addHandler(handler)


def handler(event, context):
    logger.info("lambda:invoked", extra={"event": event, "request_id": getattr(context, "aws_request_id", None)})

    pipeline = ETLPipeline()
    try:
        pipeline.run()
        return {"status": "success"}
    except Exception as e:
        logger.error("lambda:failed", exc_info=e, extra={"event": event, "request_id": getattr(context, "aws_request_id", None)})
        return {"status": "error", "message": str(e)}



# from app.clients.http_client import HttpClient
# from app.connectors.snowflake_client import SnowflakeClient
# from app.etl.pipeline import ETLPipeline
# from app.logger import get_logger

# logger = get_logger(__name__)


# def build_clients(cfg: dict):
#     http_client = HttpClient(base_url=cfg.get("API_BASE_URL") or "http://dummy.local")
#     sf_client = SnowflakeClient(connect=False)  # prevent real Snowflake connection in tests
#     return http_client, sf_client


# def handler(event, context):
#     logger.info("lambda:invoked", extra={"event": event})
#     try:
#         cfg = event.get("config", {})
#         http_client, sf_client = build_clients(cfg)

#         pipeline = ETLPipeline()
#         pipeline.http = http_client
#         pipeline.snowflake = sf_client

#         pipeline.run()

#         return {"status": "success"}
#     except Exception as e:
#         logger.error("lambda:failed", exc_info=True)
#         return {"status": "error", "message": str(e)}
