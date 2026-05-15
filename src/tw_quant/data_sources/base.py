"""Shared types for official data providers."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class ProviderResult:
    source_name: str
    data: pd.DataFrame
    status: str = "OK"
    warning: str = ""
    error_message: str = ""

    @property
    def rows(self) -> int:
        return int(len(self.data))


def empty_result(source_name: str, columns: list[str], warning: str = "") -> ProviderResult:
    return ProviderResult(
        source_name=source_name,
        data=pd.DataFrame(columns=columns),
        status="EMPTY",
        warning=warning,
    )


def failed_result(source_name: str, columns: list[str], exc: Exception) -> ProviderResult:
    return ProviderResult(
        source_name=source_name,
        data=pd.DataFrame(columns=columns),
        status="FAILED",
        warning="provider failed; fallback to local csv or neutral scores",
        error_message=f"{type(exc).__name__}: {exc}",
    )
