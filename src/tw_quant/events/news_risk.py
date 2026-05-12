"""News-risk compatibility wrapper.

The first version intentionally relies on official material-event style data
and keyword classification instead of crawling broad news sites.
"""

from __future__ import annotations

from tw_quant.events.material_events import classify_event_text, score_material_events_for_symbols

__all__ = ["classify_event_text", "score_material_events_for_symbols"]
