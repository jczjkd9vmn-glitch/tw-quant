"""Source evidence records for enrichment output."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from typing import Any


@dataclass(frozen=True)
class SourceEvidence:
    source_name: str
    source_type: str
    source_date: str
    field_name: str
    field_value: Any
    confidence: float = 0.8
    warning: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def evidence_json(items: list[SourceEvidence]) -> str:
    return json.dumps([item.to_dict() for item in items], ensure_ascii=False)
