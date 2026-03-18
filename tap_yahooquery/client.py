"""REST client handling, including YahooQueryStream base class."""

from __future__ import annotations

from abc import ABC
import random
import time
import pandas as pd
from singer_sdk.helpers.types import Context
from tap_yahooquery.helpers import (
    TickerFetcher,
    clean_strings,
    fix_empty_values,
    yahoo_api_retry,
)
from typing import Union
from singer_sdk.streams import Stream
from singer_sdk import Tap
import logging
from uuid import uuid5, NAMESPACE_DNS

import yahooquery as yq


class YahooQueryStream(Stream, ABC):
    """YahooQuery stream class with ticker partitioning support."""

    _use_cached_tickers_default = True
    _valid_segments = None
    _surrogate_key_cols = None

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap)
        self._all_tickers = None
        self._proxy_session = None

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

    def _get_proxy(self) -> Union[str, None]:
        """Get a proxy from the configured proxy list, rotating per call."""
        proxies = self.config.get("proxies", [])
        if not proxies:
            return None
        if isinstance(proxies, str):
            return proxies
        return random.choice(proxies)

    def _make_ticker(self, symbol: str) -> yq.Ticker:
        """Create a yahooquery Ticker with optional proxy rotation."""
        proxy = self._get_proxy()
        if proxy:
            if self._proxy_session is None:
                from curl_cffi import requests as cffi_requests

                self._proxy_session = cffi_requests.Session()
            self._proxy_session.proxies = {"http": proxy, "https": proxy}
            return yq.Ticker(symbol, session=self._proxy_session)
        return yq.Ticker(symbol)

    @yahoo_api_retry
    def _fetch_with_crumb_retry(
        self, ticker: str, method_name: str, is_callable: bool = True, **kwargs
    ) -> Union[dict, pd.DataFrame]:
        """Centralized Yahoo API call with crumb retry logic."""
        ticker_obj = self._make_ticker(ticker)
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
            self._proxy_session = None  # force new session on retry

            time.sleep(3)

            ticker_obj = self._make_ticker(ticker)
            method = getattr(ticker_obj, method_name)

            if is_callable:
                if kwargs:
                    data = method(**kwargs)
                else:
                    data = method()
            else:
                data = method

        return data

    def _add_surrogate_key_to_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add surrogate key column to dataframe if _surrogate_key_cols is defined."""
        if self._surrogate_key_cols:
            df["surrogate_key"] = df.apply(
                lambda row: self._make_surrogate_key(row, self._surrogate_key_cols),
                axis=1,
            )
        return df

    def _make_surrogate_key(self, row, cols):
        """Generate a UUID surrogate key from specified columns."""
        key = "".join([f"{str(row[col])}|{col}|" for col in cols if col in row])
        return uuid5(NAMESPACE_DNS, key)

    def _make_dict_transform(
        self,
        ticker,
        exclude_nested=False,
        exclude_fields=None,
        extra_renames=None,
    ):
        """Create a standard dict→record transform closure for _get_dict_records."""

        def transform(data):
            record = dict(data[ticker])  # shallow copy to avoid mutating source
            record["ticker"] = ticker
            # Apply explicit renames before clean_strings to control output names
            if extra_renames:
                for old_key, new_key in extra_renames.items():
                    if old_key in record:
                        record[new_key] = record.pop(old_key)
            keys = list(record.keys())
            cleaned_keys = clean_strings(keys)
            cleaned = {}
            for orig_key, clean_key in zip(keys, cleaned_keys):
                if exclude_fields and orig_key in exclude_fields:
                    continue
                if exclude_nested and isinstance(record[orig_key], (list, dict)):
                    continue
                cleaned[clean_key] = record[orig_key]
            cleaned = fix_empty_values(pd.DataFrame([cleaned])).to_dict("records")[0]
            yield cleaned

        return transform

    def _make_df_transform(
        self, extra_renames=None, date_columns=None, drop_columns=None
    ):
        """Create a standard DataFrame→records transform closure for _get_dataframe_records."""

        def transform(data):
            if data.empty:
                return
            df = data.reset_index().rename(columns={"symbol": "ticker"})
            if extra_renames:
                df = df.rename(columns=extra_renames)
            if drop_columns:
                df = df.drop(columns=[c for c in drop_columns if c in df.columns])
            df.columns = clean_strings(df.columns)
            if date_columns:
                for col, fmt in date_columns.items():
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime(
                            fmt
                        )
            df = fix_empty_values(df)
            yield from df.to_dict(orient="records")

        return transform

    def _get_dataframe_records(
        self,
        ticker: str,
        method_name: str,
        transformer,
        is_callable: bool = True,
        **kwargs,
    ):
        """Fetch, validate dataframe, and yield transformed records. Returns nothing if invalid."""
        data = self._fetch_with_crumb_retry(ticker, method_name, is_callable, **kwargs)
        if not isinstance(data, pd.DataFrame):
            self.logger.warning(f"No valid {method_name} data for {ticker}")
            return
        yield from transformer(data)

    def _get_dict_records(
        self,
        ticker: str,
        method_name: str,
        transformer,
        is_callable: bool = True,
        **kwargs,
    ):
        """Fetch, validate dict, and yield transformed records. Returns nothing if invalid."""
        data = self._fetch_with_crumb_retry(ticker, method_name, is_callable, **kwargs)
        if (
            not isinstance(data, dict)
            or ticker not in data
            or not isinstance(data[ticker], dict)
        ):
            self.logger.warning(f"No valid {method_name} data for {ticker}")
            return
        yield from transformer(data)


class CachedTickerProvider:
    """Provider for cached tickers (matching tap-yahooquery pattern)."""

    def __init__(self, tap):
        self.tap = tap
        self._tickers = None

    def get_tickers(self):
        if self._tickers is None:
            logging.info("Have not fetched tickers yet. Retrieving from tap cache...")
            self._tickers = self.tap.get_cached_tickers()
        return self._tickers
