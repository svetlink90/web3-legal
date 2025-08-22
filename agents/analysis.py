"""Analyze screening results and return compliance recommendation."""

from __future__ import annotations

import logging
import os
from enum import Enum
from pathlib import Path
from typing import Any, Dict

import yaml

logger = logging.getLogger(__name__)

# Default location of policy YAML relative to this file
_POLICY_PATH = Path(__file__).with_name("policy.yaml")


class Recommendation(str, Enum):
    """Possible recommendations from sanction screening analysis."""

    POSITIVE = "positive"
    NEEDS_REVIEW = "needs_review"
    NEGATIVE = "negative"


def _load_thresholds(path: Path | None = None) -> Dict[str, float]:
    """Load policy thresholds from YAML configuration."""

    cfg_path = Path(os.getenv("ANALYSIS_POLICY_PATH", path or _POLICY_PATH))
    with cfg_path.open("r", encoding="utf-8") as handle:
        data: Dict[str, Any] = yaml.safe_load(handle) or {}
    return {
        "positive": float(data.get("positive", 0)),
        "needs_review": float(data.get("needs_review", 0)),
        "negative": float(data.get("negative", 0)),
    }


def analyze_screening(data: Dict[str, Any]) -> Recommendation:
    """Return a recommendation enum based on screening ``data``.

    The decision is determined by comparing the ``risk_score`` in ``data``
    against thresholds defined in the YAML policy file. A rationale explaining
    the decision is logged for audit purposes.
    """

    thresholds = _load_thresholds()
    score = float(data.get("risk_score", 0))

    # Evaluate thresholds from highest to lowest so the first match wins
    for label, threshold in sorted(
        thresholds.items(), key=lambda item: item[1], reverse=True
    ):
        if score >= threshold:
            recommendation = Recommendation(label)
            rationale = f"score {score} >= {label} threshold {threshold}"
            logger.info(
                "Screening analysis recommendation=%s rationale=%s",
                recommendation.value,
                rationale,
            )
            return recommendation

    # Fallback if no thresholds matched
    logger.info(
        "Screening analysis recommendation=%s rationale=%s",
        Recommendation.NEEDS_REVIEW.value,
        "score below all configured thresholds",
    )
    return Recommendation.NEEDS_REVIEW
