#import pytest
from unittest.mock import patch
from app.utils import to_json
from app.etl.pipeline import ETLPipeline


def test_to_json_serialization():
    """Ensure utils.to_json converts dicts to valid JSON string."""
    obj = {"key": "value"}
    result = to_json(obj)
    assert isinstance(result, str)
    assert '"key": "value"' in result


@patch("app.clients.http_client.HttpClient.get")
@patch("app.connectors.snowflake_client.SnowflakeClient.load_data")
@patch("app.connectors.snowflake_client.SnowflakeClient.merge_data")
def test_pipeline_run(mock_merge, mock_load, mock_http):
    """Test ETL pipeline end-to-end with mocked external services."""
    # Mock external dependencies
    mock_http.return_value = {"id": 1, "name": "Test"}
    mock_load.return_value = True
    mock_merge.return_value = True

    # Instantiate pipeline and run
    pipeline = ETLPipeline()
    result = pipeline.run()

    assert result is True
    mock_http.assert_called_once()
    mock_load.assert_called_once()
    mock_merge.assert_called_once()
