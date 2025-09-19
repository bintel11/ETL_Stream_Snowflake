import pytest
from unittest.mock import patch
from app.etl.pipeline import ETLPipeline

@patch("app.clients.http_client.HttpClient.get", return_value={"id": 1})
@patch("app.connectors.snowflake_client.SnowflakeClient.load_data", return_value=True)
@patch("app.connectors.snowflake_client.SnowflakeClient.merge_data", return_value=True)
def test_pipeline_run(mock_merge, mock_load, mock_http):
    pipeline = ETLPipeline()
    result = pipeline.run()
    assert result is True
    mock_http.assert_called_once()
    mock_load.assert_called_once()
    mock_merge.assert_called_once()
