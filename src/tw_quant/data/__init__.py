"""Data ingestion and persistence.

Keep this package import lightweight so risk and strategy modules can import
domain exceptions without requiring database or HTTP dependencies.
"""

from tw_quant.data.exceptions import DataFetchError, DataQualityError, TradingHalted

__all__ = [
    "DataFetchError",
    "DataQualityError",
    "TradingHalted",
]
