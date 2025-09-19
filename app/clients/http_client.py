# app/clients/http_client.py
from typing import Dict, Any, Optional
import requests
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)
from requests.exceptions import RequestException
from app.logger import get_logger

logger = get_logger(__name__)


class HttpClientError(Exception):
    pass


class HttpClient:
    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(RequestException),
    )
    def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=self.timeout)
            resp.raise_for_status()
            return resp.json()
        except RequestException as e:
            logger.exception("HTTP GET failed", extra={"url": url, "params": params})
            raise HttpClientError(str(e)) from e
