"""Stream type classes for tap-yahooquery."""

from __future__ import annotations

import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context
import pandas as pd
from tap_yahooquery.client import YahooQueryStream
from tap_yahooquery.schema import INCOME_STMT_SCHEMA
from tap_yahooquery.helpers import (
    TickerFetcher,
    yahoo_api_retry,
    fix_empty_values,
    clean_strings,
)
import yahooquery as yq


class TickersStream(YahooQueryStream):
    """Stream to fetch all available tickers."""

    name = "tickers"
    primary_keys = ["ticker"]
    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("segment", th.StringType),
    ).to_dict()

    def get_ticker_list(self) -> list[str] | None:
        """Get list of selected tickers from config."""
        tickers_config = self.config.get("tickers", {})
        selected_tickers = tickers_config.get("select_tickers")

        if not selected_tickers or selected_tickers in ("*", ["*"]):
            return None

        if isinstance(selected_tickers, str):
            return selected_tickers.split(",")

        if isinstance(selected_tickers, list):
            if selected_tickers == ["*"]:
                return None
            return selected_tickers

        return None

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get ticker records - no partitions, handles all tickers directly."""
        selected_tickers = self.get_ticker_list()

        if not selected_tickers:
            self.logger.info("No specific tickers selected, fetching all tickers...")
            ticker_fetcher = TickerFetcher()
            ticker_records = ticker_fetcher.fetch_all_tickers()
        else:
            self.logger.info(f"Processing selected tickers: {selected_tickers}")
            ticker_fetcher = TickerFetcher()
            ticker_records = ticker_fetcher.fetch_specific_tickers(selected_tickers)

        if hasattr(ticker_records, "to_dict"):
            ticker_records = ticker_records.to_dict("records")

        for record in ticker_records:
            yield record


class SecFilingsStream(YahooQueryStream):
    """Stream for SEC filings data."""

    name = "sec_filings"
    primary_keys = ["ticker", "date", "type", "title"]
    _use_cached_tickers_default = True

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("date", th.DateType, required=True),
        th.Property("epoch_date", th.DateTimeType, required=True),
        th.Property("type", th.StringType),
        th.Property("title", th.StringType),
        th.Property("edgar_url", th.StringType),
        th.Property("exhibits", th.StringType),
        th.Property("max_age", th.NumberType),
        th.Property("timestamp_extracted", th.DateTimeType),
    ).to_dict()

    @yahoo_api_retry
    def _fetch_sec_filings(self, ticker: str) -> pd.DataFrame:
        """Fetch SEC filings data with retry protection."""
        ticker_obj = yq.Ticker(ticker)
        return ticker_obj.sec_filings

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get SEC filings records - context will have ticker from partition."""
        context = context or {}
        ticker = context.get("ticker")

        if not ticker:
            self.logger.error("No ticker found in context")
            return

        self.logger.info(f"Processing SEC filings for ticker: {ticker}")
        df = self._fetch_sec_filings(ticker)
        for record in df.to_dict(orient="records"):
            yield record


class IncomeStmtStream(YahooQueryStream):
    """Stream for income statements."""

    name = "income_stmt"
    primary_keys = ["ticker", "date"]
    _use_cached_tickers_default = True  # Uses partitions automatically

    schema = INCOME_STMT_SCHEMA

    @yahoo_api_retry
    def _fetch_income_statement(self, ticker: str) -> dict:
        """Fetch income statement data with retry protection."""
        ticker_obj = yq.Ticker(ticker)
        return ticker_obj.income_statement()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get financial records - context will have ticker from partition."""
        context = context or {}
        ticker = context.get("ticker")

        if not ticker:
            self.logger.error("No ticker found in context")
            return

        self.logger.info(f"Processing income_stmt for ticker: {ticker}")

        try:
            df = self._fetch_income_statement(ticker)
            assert isinstance(df, pd.DataFrame)
            df = df.reset_index().rename(
                columns={
                    "symbol": "ticker",
                    "asOfDate": "date",
                    "BasicEPS": "basic_eps",
                    "DilutedEPS": "diluted_eps",
                    "NormalizedEBITDA": "normalized_ebitda",
                    "DilutedNIAvailtoComStockholders": "diluted_ni_avail_to_common_stock_holders",
                    "EBIT": "ebit",
                    "EBITDA": "ebitda",
                }
            )
            df = fix_empty_values(df)
            df.columns = clean_strings(df.columns)
            df["date"] = df["date"].dt.strftime("%Y-%m-%d")
            for record in df.to_dict(orient="records"):
                yield record
        except Exception as e:
            self.logger.error(f"Error getting income_stmt for ticker {ticker}: {e}")
