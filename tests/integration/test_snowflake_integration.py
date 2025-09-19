import pytest
from app.connectors.snowflake_client import SnowflakeClient


@pytest.mark.integration
def test_snowflake_connection():
    client = SnowflakeClient()
    conn = client.connect()
    assert conn is not None
