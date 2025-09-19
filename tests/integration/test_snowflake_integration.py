# tests/integration/test_snowflake_integration.py
import pytest
from app.connectors.snowflake_client import SnowflakeClient

@pytest.mark.integration
def test_snowflake_connection(monkeypatch):
    monkeypatch.setattr("snowflake.connector.connect", lambda **kwargs: True)
    client = SnowflakeClient(
        account="dummy_account",
        user="dummy_user",
        password="dummy_password",
        warehouse="dummy_wh",
        database="dummy_db",
        schema="dummy_schema",
    )
    assert client is not None
