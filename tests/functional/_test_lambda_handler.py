from unittest.mock import patch, MagicMock
from app import lambda_handler

@patch("app.etl.pipeline.SnowflakeClient")
@patch("app.etl.pipeline.HttpClient.get", return_value={"id": 1, "name": "Test"})
def test_lambda_handler_success(mock_http_get, mock_sf_client_class):
    # Create a mock SnowflakeClient instance
    mock_sf_instance = MagicMock()
    
    # Make methods accept any args/kwargs
    mock_sf_instance.load_data.side_effect = lambda *args, **kwargs: True
    mock_sf_instance.merge_data.side_effect = lambda *args, **kwargs: True

    # Return our mock instance when SnowflakeClient() is called
    mock_sf_client_class.return_value = mock_sf_instance

    # Call Lambda
    event = {"action": "run_pipeline"}
    context = {}
    result = lambda_handler.handler(event, context)

    # Assert success
    assert result["status"] == "success"

    # Optional: ensure HTTP GET was called
    mock_http_get.assert_called_once_with("/data")
