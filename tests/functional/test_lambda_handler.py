from app import lambda_handler


def test_lambda_handler_success():
    event = {"action": "run_pipeline"}
    context = {}
    result = lambda_handler.handler(event, context)
    assert result["status"] == "success"
