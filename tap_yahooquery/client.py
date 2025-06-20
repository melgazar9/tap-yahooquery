"""REST client handling, including YahooQueryStream base class."""

from __future__ import annotations

from abc import ABC
import time
import pandas as pd
from singer_sdk.helpers.types import Context
from tap_yahooquery.helpers import TickerFetcher
from typing import Union
from singer_sdk.streams import Stream
from singer_sdk import Tap
import logging

import yahooquery as yq


class YahooQueryStream(Stream, ABC):
    """YahooQuery stream class with ticker partitioning support."""

    _use_cached_tickers_default = True
    _valid_segments = None

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap)
        self._all_tickers = None

    def _get_stream_config(self) -> dict:
        """Get configuration for this specific stream."""
        return self.config.get(self.name, {})

    @property
    def use_cached_tickers(self) -> bool:
        """Whether to use cached tickers for this stream."""
        stream_config = self._get_stream_config()

        if "use_cached_tickers" in stream_config:
            use_cached_tickers = stream_config["use_cached_tickers"]
            assert isinstance(use_cached_tickers, bool), (
                f"Config for {self.name}.use_cached_tickers must be bool, "
                f"got {type(use_cached_tickers)}"
            )
            return use_cached_tickers

        if hasattr(type(self), "_use_cached_tickers_default"):
            return getattr(type(self), "_use_cached_tickers_default")

        raise AttributeError(
            f"use_cached_tickers is not defined for stream {self.name}"
        )

    @property
    def partitions(self) -> list[dict] | None:
        """Get partitions for ticker-based streams with segment filtering."""
        if not self.use_cached_tickers:
            return None

        ticker_fetcher = TickerFetcher()
        ticker_list = self.config.get("ticker_list") or self.config.get(
            "tickers", {}
        ).get("select_tickers")

        if ticker_list and ticker_list not in ("*", ["*"]):
            ticker_records = ticker_fetcher.fetch_specific_tickers(ticker_list)
            self.logger.info(
                f"{self.name}: Using specific tickers from config: {ticker_list}"
            )
        else:
            ticker_records = self._tap.get_cached_tickers()
            self.logger.info(f"{self.name}: Using cached tickers from tap")

        self._all_tickers = {ticker["ticker"]: ticker for ticker in ticker_records}

        filtered_tickers = self._filter_tickers_by_segments(
            ticker_records, allowed_segments=self._valid_segments
        )

        partitions = [{"ticker": ticker["ticker"]} for ticker in filtered_tickers]

        self.logger.info(f"Created {len(partitions)} ticker partitions for {self.name}")
        return partitions

    def _filter_tickers_by_segments(
        self, tickers: list[dict], allowed_segments: list[str] | None = None
    ) -> list[dict]:
        """Centralized ticker filtering by segments."""
        if allowed_segments is None:
            self.logger.info(
                f"{self.name}: Processing all {len(tickers)} tickers (no segment filtering)"
            )
            return tickers

        original_count = len(tickers)

        filtered_tickers = [
            ticker for ticker in tickers if ticker.get("segment") in allowed_segments
        ]

        self.logger.info(
            f"{self.name}: Filtered to {len(filtered_tickers)} tickers from {original_count} "
            f"(allowed segments: {allowed_segments})"
        )

        excluded_tickers = [
            f"{t['ticker']} ({t.get('segment', 'unknown')})"
            for t in tickers
            if t not in filtered_tickers
        ]
        if excluded_tickers:
            self.logger.info(f"{self.name}: Excluded tickers: {excluded_tickers}")

        return filtered_tickers

    def _is_valid_ticker_for_stream(self, ticker: str) -> bool:
        """
        Check if ticker is valid for this stream based on segment.
        Uses the actual segment data instead of regex patterns.
        """
        if not self._valid_segments:
            return True  # No segment restrictions

        if not self._all_tickers:
            return True  # No ticker data available, allow through

        ticker_data = self._all_tickers.get(ticker)
        if not ticker_data:
            return False

        return ticker_data.get("segment") in self._valid_segments

    def _get_ticker_from_context(self, context: Context) -> Union[str, None]:
        """Validates and returns ticker from context."""
        context = context or {}
        ticker = context.get("ticker")
        if not ticker:
            self.logger.error("No ticker found in context")
            return None

        if not self._is_valid_ticker_for_stream(ticker):
            self.logger.warning(
                f"Skipping {ticker} - not valid for {self.name} stream based on segment rules"
            )
            return None

        return ticker

    def _fetch_with_crumb_retry(
        self, ticker: str, method_name: str, is_callable: bool = True, **kwargs
    ) -> Union[dict, pd.DataFrame]:
        """Centralized Yahoo API call with crumb retry logic."""
        ticker_obj = yq.Ticker(ticker)
        method = getattr(ticker_obj, method_name)

        if is_callable:
            if kwargs:
                data = method(**kwargs)
            else:
                data = method()
        else:
            data = method

        if isinstance(data, dict) and "Invalid Crumb" in str(data):
            self.logger.warning(f"Invalid crumb for {ticker}, retrying {method_name}")
            ticker_obj.session.close()
            ticker_obj._session = None
            ticker_obj.crumb = None
            ticker_obj.cookie = None
            if hasattr(ticker_obj, "_session"):
                ticker_obj._session = None
            if hasattr(ticker_obj, "session"):
                ticker_obj.session.close()

            time.sleep(3)

            ticker_obj = yq.Ticker(ticker)
            method = getattr(ticker_obj, method_name)

            if is_callable:
                if kwargs:
                    data = method(**kwargs)
                else:
                    data = method()
            else:
                data = method

        return data


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
