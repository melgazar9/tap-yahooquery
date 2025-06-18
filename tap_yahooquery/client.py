"""REST client handling, including YahooQueryStream base class."""

from __future__ import annotations

from abc import ABC
from singer_sdk.streams import Stream
from singer_sdk import Tap
import logging
from tap_yahooquery.helpers import yahoo_api_retry
import yahooquery as yq


class YahooQueryStream(Stream, ABC):
    """YahooQuery stream class with ticker partitioning support."""

    _use_cached_tickers_default = True

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap)
        self.yahoo_client = YahooAPIClient()

    def _get_stream_config(self) -> dict:
        """Get configuration for this specific stream."""
        return self.config.get(self.name, {})

    @property
    def use_cached_tickers(self) -> bool:
        """Whether to use cached tickers for this stream."""
        stream_config = self._get_stream_config()

        if "use_cached_tickers" in stream_config:
            use_cached = stream_config["use_cached_tickers"]
            assert isinstance(use_cached, bool), (
                f"Config for {self.name}.use_cached_tickers must be bool, "
                f"got {type(use_cached)}"
            )
            return use_cached

        if hasattr(type(self), "_use_cached_tickers_default"):
            return getattr(type(self), "_use_cached_tickers_default")

        raise AttributeError(
            f"use_cached_tickers is not defined for stream {self.name}"
        )

    @property
    def partitions(self) -> list[dict] | None:
        """Get partitions for ticker-based streams."""
        if not self.use_cached_tickers:
            return None
        ticker_records = self._tap.get_cached_tickers()
        partitions = [{"ticker": record["ticker"]} for record in ticker_records]
        self.logger.info(f"Created {len(partitions)} ticker partitions for {self.name}")
        return partitions


class CachedTickerProvider:
    """Provider for cached tickers (matching tap-polygon pattern)."""

    def __init__(self, tap):
        self.tap = tap
        self._tickers = None

    def get_tickers(self):
        if self._tickers is None:
            logging.info("Have not fetched tickers yet. Retrieving from tap cache...")
            self._tickers = self.tap.get_cached_tickers()
        return self._tickers


class YahooAPIClient:
    """Centralized Yahoo API client with standardized retry logic."""

    @yahoo_api_retry
    def get_sec_filings(self, ticker: str) -> dict:
        ticker_obj = yq.Ticker(ticker)
        return ticker_obj.sec_filings

    @yahoo_api_retry
    def get_income_statement(self, ticker: str) -> dict:
        ticker_obj = yq.Ticker(ticker)
        return ticker_obj.income_statement()

    @yahoo_api_retry
    def get_balance_sheet(self, ticker: str) -> dict:
        ticker_obj = yq.Ticker(ticker)
        return ticker_obj.balance_sheet()
