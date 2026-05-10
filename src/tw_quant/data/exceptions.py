"""Domain exceptions for data and trading safety."""


class DataQualityError(RuntimeError):
    """Raised when market data is missing, duplicated, or internally invalid."""


class DataFetchError(RuntimeError):
    """Raised when market data cannot be fetched from the upstream service."""


class TradingHalted(RuntimeError):
    """Raised when the system must stop simulated trading for safety reasons."""
