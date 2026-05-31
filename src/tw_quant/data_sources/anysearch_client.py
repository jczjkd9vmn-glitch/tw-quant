"""Minimal AnySearch JSON-RPC client used for proposal-only research."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
import time
from typing import Any
from urllib import error, request


ANYSEARCH_ENDPOINT = "https://api.anysearch.com/mcp"


class AnySearchError(RuntimeError):
    """Raised when AnySearch cannot return a usable response."""


@dataclass
class AnySearchClient:
    api_key: str = field(repr=False)
    endpoint: str = ANYSEARCH_ENDPOINT
    timeout_seconds: int = 15
    retry_count: int = 2

    @classmethod
    def from_env(
        cls,
        *,
        timeout_seconds: int = 15,
        retry_count: int = 2,
        endpoint: str = ANYSEARCH_ENDPOINT,
    ) -> "AnySearchClient":
        api_key = os.getenv("ANYSEARCH_API_KEY", "").strip()
        if not api_key:
            raise AnySearchError("missing ANYSEARCH_API_KEY")
        return cls(
            api_key=api_key,
            endpoint=endpoint,
            timeout_seconds=timeout_seconds,
            retry_count=retry_count,
        )

    def search(self, query: str, *, max_results: int = 5) -> str:
        return self.call_tool(
            "search",
            {
                "query": query,
                "max_results": int(max_results),
                "content_types": ["web", "data", "news"],
            },
        )

    def call_tool(self, tool_name: str, arguments: dict[str, Any]) -> str:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        }
        body = json.dumps(payload).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

        attempts = max(int(self.retry_count), 0) + 1
        last_error: Exception | None = None
        for attempt in range(attempts):
            try:
                req = request.Request(self.endpoint, data=body, headers=headers, method="POST")
                with request.urlopen(req, timeout=self.timeout_seconds) as response:
                    data = json.loads(response.read().decode("utf-8"))
                return _extract_text(data)
            except (error.HTTPError, error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
                last_error = exc
                if attempt < attempts - 1:
                    time.sleep(0.25 * (attempt + 1))
                    continue
                break
        raise AnySearchError(str(last_error) if last_error else "AnySearch request failed")


def _extract_text(data: dict[str, Any]) -> str:
    if "error" in data:
        error_data = data.get("error") or {}
        if isinstance(error_data, dict):
            raise AnySearchError(str(error_data.get("message") or error_data))
        raise AnySearchError(str(error_data))

    result = data.get("result", {})
    content = result.get("content", []) if isinstance(result, dict) else []
    for item in content:
        if isinstance(item, dict) and item.get("type") == "text":
            return str(item.get("text", ""))
    return json.dumps(result, ensure_ascii=False)
