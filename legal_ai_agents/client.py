import os
import logging
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class LegalAIAgentsClient:
    """Simple client for interacting with the legal-ai-agents service."""

    def __init__(self, base_url: str | None = None, retries: int = 3, backoff_factor: float = 0.5) -> None:
        self.base_url = base_url or os.getenv("LEGAL_AI_AGENTS_URL", "https://api.legal-ai-agents.example")
        self.session = requests.Session()

        retry = Retry(total=retries, backoff_factor=backoff_factor, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def screen_address(self, address: str, timeout: int = 10) -> Any:
        """Return sanction screening data for an address."""
        url = f"{self.base_url}/screen"
        try:
            response = self.session.post(url, json={"address": address}, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            logger.error("Legal AI Agents request failed for %s: %s", address, exc)
            raise
