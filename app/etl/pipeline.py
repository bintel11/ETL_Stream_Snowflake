# ETL pipeline using S3 + Snowflake.
from app.connectors.s3_client import S3Client
from app.connectors.snowflake_client import SnowflakeClient


class ETLPipeline:
    def __init__(self):
        self.s3 = S3Client()
        self.snowflake = SnowflakeClient()

    def run_batch(
        self, local_file: str, s3_key: str, staging_table: str, target_table: str
    ):
        """Batch pipeline: upload → load → merge."""
        # 1. Upload raw file to S3
        s3_uri = self.s3.upload_file(local_file, s3_key)

        # 2. Load file from S3 into Snowflake staging
        self.snowflake.load_data("my_s3_stage", s3_key, staging_table)

        # 3. Merge staging into target
        self.snowflake.merge_data(staging_table, target_table)

        return {"status": "success", "s3_uri": s3_uri}


"""
# app/etl/pipeline.py
from typing import Any, Dict, List
from app.logger import get_logger
from app.clients.http_client import HttpClient, HttpClientError
from app.connectors.snowflake_client import SnowflakeClient
import uuid
import time

logger = get_logger(__name__)

class ETLPipeline:
    def __init__(self, http_client: HttpClient, snowflake_client: SnowflakeClient, cfg: Dict[str, Any]):
        self.http = http_client
        self.sf = snowflake_client
        self.cfg = cfg

    def _generate_request_id(self) -> str:
        return str(uuid.uuid4())

    def fetch_source(self, path: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        request_id = self._generate_request_id()
        logger.info("fetch:start", extra={"request_id": request_id, "path": path})
        start = time.time()
        try:
            data = self.http.get(path, params=params)
            logger.info("fetch:success", extra={"request_id": request_id, "count": len(data)})
            return data
        except HttpClientError:
            logger.exception("fetch:failed", extra={"request_id": request_id})
            raise

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # Example transformation (normalize keys, validate)
        transformed = []
        for r in records:
            try:
                # minimal example; adapt to business logic
                transformed.append({
                    "id": r["id"],
                    "name": r.get("name"),
                    "value": float(r.get("value") or 0),
                    "ingested_at": r.get("timestamp")
                })
            except Exception:
                logger.exception("transform:row_failed", extra={"row": r})
                # optionally continue or raise depending on policy
        logger.info("transform:complete", extra={"count": len(transformed)})
        return transformed

    def load_to_snowflake(self, records: List[Dict[str, Any]], staging_table: str):
        # For demonstration we do per-row insert (not for high volume). For production, bulk-load to stage then COPY INTO.
        insert_sql = f"INSERT INTO {staging_table} (id, name, value, ingested_at) VALUES (%(id)s, %(name)s, %(value)s, %(ingested_at)s)"
        cur = self.sf.conn.cursor()
        try:
            for rec in records:
                cur.execute(insert_sql, rec)
            cur.connection.commit()
            logger.info("load:success", extra={"count": len(records)})
        finally:
            cur.close()

    def run(self, path: str):
        request_id = self._generate_request_id()
        try:
            records = self.fetch_source(path)
            transformed = self.transform(records)
            self.load_to_snowflake(transformed, self.cfg.get("STAGING_TABLE", "MY_SCHEMA.STG_TABLE"))
            # perform MERGE
            self.sf.merge_upsert(
                target_table=self.cfg.get("TARGET_TABLE", "MY_SCHEMA.TARGET"),
                staging_table=self.cfg.get("STAGING_TABLE", "MY_SCHEMA.STG_TABLE"),
                key_columns=["id"],
                update_columns=["name", "value", "ingested_at"]
            )
            logger.info("pipeline:complete", extra={"request_id": request_id})
        except Exception:
            logger.exception("pipeline:failed", extra={"request_id": request_id})
            raise
            
"""
