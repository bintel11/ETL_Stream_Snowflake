from app.utils import to_json, safe_get

def test_to_json():
    obj = {"foo": "bar"}
    result = to_json(obj)
    assert isinstance(result, str)
    assert '"foo": "bar"' in result

def test_safe_get_existing_key():
    d = {"key": "value"}
    assert safe_get(d, "key") == "value"

def test_safe_get_missing_key():
    d = {}
    assert safe_get(d, "missing", default="fallback") == "fallback"
