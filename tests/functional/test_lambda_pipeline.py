import pytest
from unittest.mock import patch, MagicMock
from app import lambda_handler


@patch("app.etl.pipeline.SnowflakeClient")
@patch("app.etl.pipeline.HttpClient.get", return_value={"id": 1, "name": "Test"})
def test_lambda_handler_success(mock_http_get, mock_sf_client_class):
    mock_sf_instance = MagicMock()
    mock_sf_instance.load_data.return_value = True
    mock_sf_instance.merge_data.return_value = True
    mock_sf_client_class.return_value = mock_sf_instance

    event = {"action": "run_pipeline"}
    context = {}

    result = lambda_handler.handler(event, context)

    assert result["status"] == "success"



#  from unittest.mock import patch, MagicMock
# from app import lambda_handler

# @patch("app.etl.pipeline.SnowflakeClient")
# @patch("app.etl.pipeline.HttpClient.get", return_value={"id": 1, "name": "Test"})
# def test_lambda_handler_success(mock_http_get, mock_sf_client_class):
#     mock_sf_instance = MagicMock()
#     # FIX: match the real method signatures
#     mock_sf_instance.load_data.side_effect = lambda data, table_name=None: True
#     mock_sf_instance.merge_data.side_effect = lambda src, tgt=None: True
#     mock_sf_client_class.return_value = mock_sf_instance

#     event = {"action": "run_pipeline"}
#     context = {}

#     result = lambda_handler.handler(event, context)

#     assert result["status"] == "success"







# from unittest.mock import patch, MagicMock
# import pytest
# from app.etl.pipeline import ETLPipeline
# from app import lambda_handler

# # 1️⃣ Test the pipeline run() in isolation
# @patch("app.etl.pipeline.SnowflakeClient")
# @patch("app.etl.pipeline.HttpClient.get", return_value={"id": 1, "name": "Test"})
# def test_pipeline_run_isolated(mock_http_get, mock_sf_client_class):
#     # Create mock Snowflake client
#     mock_sf_instance = MagicMock()
#     mock_sf_instance.load_data.side_effect = lambda *args, **kwargs: True
#     mock_sf_instance.merge_data.side_effect = lambda *args, **kwargs: True

#     mock_sf_client_class.return_value = mock_sf_instance

#     # Create pipeline and run
#     pipeline = ETLPipeline()
#     result = pipeline.run()

#     # Check the run() returned True (successful)
#     assert result is True

#     # HTTP GET and Snowflake methods were called
#     mock_http_get.assert_called_once_with("/data")
#     mock_sf_instance.load_data.assert_called_once()
#     mock_sf_instance.merge_data.assert_called_once()


# 2️⃣ Test Lambda handler in isolation
# @patch("app.etl.pipeline.SnowflakeClient")
# @patch("app.etl.pipeline.HttpClient.get", return_value={"id": 1, "name": "Test"})
# def test_lambda_handler_success(mock_http_get, mock_sf_client_class):
#     mock_sf_instance = MagicMock()
#     mock_sf_instance.load_data.side_effect = lambda *args, **kwargs: True
#     mock_sf_instance.merge_data.side_effect = lambda *args, **kwargs: True
#     mock_sf_client_class.return_value = mock_sf_instance

#     event = {"action": "run_pipeline"}
#     context = {}

#     result = lambda_handler.handler(event, context)

#     # Assert the lambda reports success
#     assert result["status"] == "success"
