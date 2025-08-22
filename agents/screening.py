import logging
from typing import Any

from legal_ai_agents.client import LegalAIAgentsClient

logger = logging.getLogger(__name__)

_client = LegalAIAgentsClient()


def run_sanction_screen(address: str, timeout: int = 10) -> Any:
    """Run a sanction screen for the given address.

    Returns the raw screening data from legal-ai-agents.
    """
    try:
        return _client.screen_address(address, timeout=timeout)
    except Exception as exc:  # pragma: no cover - logging of unexpected errors
        logger.error("Failed to screen address %s: %s", address, exc)
        raise
