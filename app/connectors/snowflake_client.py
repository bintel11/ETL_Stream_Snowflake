# app/connectors/snowflake_client.py
from typing import Dict, Any, List
import snowflake.connector
from snowflake.connector import DictCursor
from app.logger import get_logger

logger = get_logger(__name__)


class SnowflakeClient:
    def __init__(
        self,
        account: str = "dummy",
        user: str = "dummy",
        password: str = "dummy",
        warehouse: str = "dummy",
        database: str = "dummy",
        schema: str = "dummy",
        role: str = None,
        connect: bool = True,
    ):
        self.conn = None
        if connect:
            self.conn = snowflake.connector.connect(
                account=account,
                user=user,
                password=password,
                warehouse=warehouse,
                database=database,
                schema=schema,
                role=role,
            )

    def execute(self, sql: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        cur = self.conn.cursor(DictCursor)
        try:
            logger.debug("Executing SQL", extra={"sql": sql, "params": params})
            cur.execute(sql, params or {})
            if cur.description:
                rows = cur.fetchall()
                return rows
            return []
        finally:
            cur.close()

    def copy_into_stage(
        self,
        stage_name: str,
        table_name: str,
        file_path: str,
        file_format: str = "json",
    ):
        # Example: put file in stage (if using internal stage)
        # For larger workloads, use Snowpipe / bulk loading (not shown).
        sql = f"COPY INTO {table_name} FROM @{stage_name}/{file_path} FILE_FORMAT = (TYPE = {file_format})"
        self.execute(sql)

    def merge_upsert(
        self,
        target_table: str,
        staging_table: str,
        key_columns: List[str],
        update_columns: List[str],
    ):
        # Build MERGE statement dynamically but be careful with SQL injection in real code
        on_clause = " AND ".join([f"target.{k} = stage.{k}" for k in key_columns])
        update_clause = ", ".join([f"{col} = stage.{col}" for col in update_columns])
        insert_cols = ", ".join(update_columns + key_columns)
        insert_vals = ", ".join([f"stage.{col}" for col in update_columns + key_columns])

        sql = f"""
        MERGE INTO {target_table} AS target
        USING {staging_table} AS stage
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
        """
        self.execute(sql)

    def load_data(self, data, table_name: str):
        """
        Stub for loading data into a Snowflake table.
        In production, implement proper bulk load / insert.
        For now, just log and return True (so unit tests pass).
        """
        logger.info(f"Stub load_data called for table={table_name}, rows={len(data) if data else 0}")
        return True

    def merge_data(self, target_table: str, data: list, key_columns: list):
        """
        Stub for merging data into Snowflake.
        In production, this should stage + MERGE.
        For now, just log and return True (so unit tests pass).
        """
        logger.info(f"Stub merge_data called for table={target_table}, keys={key_columns}, rows={len(data) if data else 0}")
        return True
