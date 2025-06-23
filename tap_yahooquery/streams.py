"""Stream type classes for tap-yahooquery."""

from __future__ import annotations

import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context
import pandas as pd

from tap_yahooquery.client import YahooQueryStream
from tap_yahooquery.schema import INCOME_STMT_SCHEMA, ALL_FINANCIAL_DATA_SCHEMA
from tap_yahooquery.helpers import (
    TickerFetcher,
    fix_empty_values,
    clean_strings,
)


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

        for record in ticker_records:
            yield record


class BaseFinancialStream(YahooQueryStream):
    _use_cached_tickers_default = True
    _valid_segments = [
        "stock_tickers",
        "mutual_fund_tickers",
        "private_companies_tickers",
        "european_funds",
    ]


class SecFilingsStream(BaseFinancialStream):
    """Stream for SEC filings data."""

    name = "sec_filings"
    primary_keys = ["ticker", "date", "type", "title"]
    _valid_segments = [
        "stock_tickers",
        "mutual_fund_tickers",
        "private_companies_tickers",
    ]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("epoch_date", th.DateTimeType),
        th.Property("type", th.StringType),
        th.Property("title", th.StringType),
        th.Property("edgar_url", th.StringType),
        th.Property(
            "exhibits",
            th.ArrayType(
                th.ObjectType(
                    th.Property("type", th.StringType),
                    th.Property("url", th.StringType),
                    th.Property("downloadUrl", th.StringType),
                )
            ),
        ),
        th.Property("max_age", th.NumberType),
    ).to_dict()

    def _fetch_sec_filings(self, ticker: str) -> pd.DataFrame:
        """Fetch SEC filings."""
        df = self._fetch_with_crumb_retry(ticker, "sec_filings", is_callable=False)
        assert isinstance(
            df, pd.DataFrame
        ), f"sec_filings did not return a DataFrame for ticker {ticker}."
        df = df.reset_index(level=0).rename(columns={"symbol": "ticker"})
        df.columns = clean_strings(df.columns)
        df = fix_empty_values(df)
        return df

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get SEC filings records - context will have ticker from partition."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing SEC filings for ticker: {ticker}")
        df = self._fetch_sec_filings(ticker)
        for record in df.to_dict(orient="records"):
            yield record


class IncomeStmtStream(BaseFinancialStream):
    """Stream for income statements."""

    name = "income_stmt"
    primary_keys = ["ticker", "as_of_date"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]

    schema = INCOME_STMT_SCHEMA

    def _fetch_income_statement(self, ticker: str) -> pd.DataFrame:
        """Fetch income statement."""
        df = self._fetch_with_crumb_retry(ticker, "income_statement")
        assert isinstance(
            df, pd.DataFrame
        ), f"income_statement did not return a DataFrame for ticker {ticker}."
        df = df.reset_index().rename(
            columns={
                "symbol": "ticker",
                "BasicEPS": "basic_eps",
                "DilutedEPS": "diluted_eps",
                "NormalizedEBITDA": "normalized_ebitda",
                "DilutedNIAvailtoComStockholders": "diluted_ni_avail_to_common_stock_holders",
                "EBIT": "ebit",
                "EBITDA": "ebitda",
                "GrossPPE": "gross_ppe",
                "GainOnSaleOfPPE": "gain_on_sale_of_ppe",
                "OtherGandA": "other_g_and_a",
                "OtherunderPreferredStockDividend": "other_under_preferred_stock_dividend",
            }
        )
        df = fix_empty_values(df)
        df.columns = clean_strings(df.columns)
        df["as_of_date"] = df["as_of_date"].dt.strftime("%Y-%m-%d")
        return df

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get financial records - context will have ticker from partition."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing income_stmt for ticker: {ticker}")
        try:
            df = self._fetch_income_statement(ticker)
            for record in df.to_dict(orient="records"):
                yield record
        except Exception as e:
            self.logger.error(f"Error getting income_stmt for ticker {ticker}: {e}")


class AllFinancialDataStream(BaseFinancialStream):
    """Stream for all financial data."""

    name = "all_financial_data"
    primary_keys = ["ticker", "as_of_date"]

    schema = ALL_FINANCIAL_DATA_SCHEMA

    def _fetch_all_financial_data(self, ticker: str) -> pd.DataFrame:
        """Fetch income statement."""
        df = self._fetch_with_crumb_retry(ticker, "all_financial_data")
        assert isinstance(df, pd.DataFrame)
        df = df.reset_index().rename(
            columns={
                "symbol": "ticker",
                "BasicEPS": "basic_eps",
                "DilutedEPS": "diluted_eps",
                "NormalizedEBITDA": "normalized_ebitda",
                "DilutedNIAvailtoComStockholders": "diluted_ni_avail_to_common_stock_holders",
                "EBIT": "ebit",
                "EBITDA": "ebitda",
                "GainOnSaleOfPPE": "gain_on_sale_of_ppe",
                "EnterprisesValueEBITDARatio": "enterprises_value_ebitda_ratio",
                "NetPPE": "net_ppe",
                "NetPPEPurchaseAndSale": "net_ppe_purchase_and_sale",
                "PurchaseOfPPE": "purchase_of_ppe",
                "GrossPPE": "gross_ppe",
                "InvestmentinFinancialAssets": "investment_in_financial_assets",
            }
        )
        df.columns = clean_strings(df.columns)
        df["as_of_date"] = df["as_of_date"].dt.strftime("%Y-%m-%d")
        df = fix_empty_values(df)
        return df

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get financial records - context will have ticker from partition."""
        ticker = self._get_ticker_from_context(context)

        self.logger.info(f"Processing all_financial_data for ticker: {ticker}")

        try:
            df = self._fetch_all_financial_data(ticker)
            for record in df.to_dict(orient="records"):
                yield record
        except Exception as e:
            self.logger.error(
                f"Error getting all_financial_data for ticker {ticker}: {e}"
            )


class CorporateEventsStream(BaseFinancialStream):
    """Stream for corporate events."""

    name = "corporate_events"
    primary_keys = ["ticker", "date", "id"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("id", th.IntegerType),
        th.Property("significance", th.IntegerType),
        th.Property("headline", th.StringType),
        th.Property("description", th.StringType),
        th.Property("parent_topics", th.StringType),
    ).to_dict()

    def _fetch_corporate_events(self, ticker: str) -> pd.DataFrame:
        """Fetch corporate events."""
        df = self._fetch_with_crumb_retry(ticker, "corporate_events", is_callable=False)
        df = fix_empty_values(df.reset_index())
        df = df.rename(columns={"symbol": "ticker"})
        df["significance"] = df["significance"].astype(int)
        df.columns = clean_strings(df.columns)
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        return df

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing corporate_events for ticker: {ticker}")
        try:
            df = self._fetch_corporate_events(ticker)
            yield from df.to_dict(orient="records")
        except Exception as e:
            self.logger.error(
                f"Error getting corporate_events for ticker {ticker}: {e}"
            )


class CalendarEventsStream(BaseFinancialStream):
    """Stream for calendar events."""

    name = "calendar_events"
    primary_keys = ["ticker", "event_type", "event_date"]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("event_type", th.StringType, required=True),
        th.Property("event_date", th.DateTimeType, required=True),
        th.Property("event_category", th.StringType),
        th.Property("event_subtype", th.StringType),
        th.Property("priority_level", th.IntegerType),
        th.Property("market_impact_expected", th.StringType),
        th.Property("event_description", th.StringType),
        th.Property("earnings_average", th.NumberType),
        th.Property("earnings_low", th.NumberType),
        th.Property("earnings_high", th.NumberType),
        th.Property("revenue_average", th.NumberType),
        th.Property("revenue_low", th.NumberType),
        th.Property("revenue_high", th.NumberType),
        th.Property("is_estimate", th.BooleanType),
        th.Property("dividend_date", th.DateTimeType),
        th.Property("timestamp_extracted", th.DateTimeType),
    ).to_dict()

    def _fetch_calendar_events(self, ticker: str) -> pd.DataFrame:
        """Fetch calendar events."""
        data = self._fetch_with_crumb_retry(
            ticker, "calendar_events", is_callable=False
        )

        if not data or ticker not in data:
            return pd.DataFrame()

        return self._normalize_calendar_events(ticker, data)

    def _normalize_calendar_events(self, ticker: str, data: dict) -> pd.DataFrame:
        """Normalize Yahoo Finance calendar events."""
        try:
            if ticker not in data:
                return pd.DataFrame()

            ticker_data = data[ticker]
            records = []

            # Handle earnings events
            if "earnings" in ticker_data:
                earnings = ticker_data["earnings"]
                earnings_dates = earnings.get("earningsDate", [])

                for date in earnings_dates:
                    try:
                        if isinstance(date, str):
                            # Remove trailing 'S' and handle incomplete time format
                            clean_date = date.replace("S", "").rstrip(":")

                            # If the time part is incomplete (like '05:59:'), pad with seconds
                            if clean_date.count(":") == 2 and clean_date.endswith(":"):
                                clean_date = clean_date + "00"
                            elif clean_date.count(":") == 1:
                                clean_date = clean_date + ":00"

                            parsed_date = pd.to_datetime(clean_date)
                        else:
                            parsed_date = pd.to_datetime(date)

                        record = {
                            "ticker": ticker,
                            "event_type": "earnings",
                            "event_date": parsed_date,
                            "event_category": "earnings",
                            "earnings_average": earnings.get("earningsAverage"),
                            "earnings_low": earnings.get("earningsLow"),
                            "earnings_high": earnings.get("earningsHigh"),
                            "revenue_average": earnings.get("revenueAverage"),
                            "revenue_low": earnings.get("revenueLow"),
                            "revenue_high": earnings.get("revenueHigh"),
                            "is_estimate": earnings.get(
                                "isEarningsDateEstimate", False
                            ),
                            "timestamp_extracted": pd.Timestamp.utcnow(),
                        }
                        records.append(record)
                    except Exception as e:
                        self.logger.warning(
                            f"Could not parse earnings date '{date}' for {ticker}: {e}"
                        )
                        continue

            # Handle dividend events
            if "dividendDate" in ticker_data:
                record = {
                    "ticker": ticker,
                    "event_type": "dividend",
                    "event_date": pd.to_datetime(ticker_data["dividendDate"]),
                    "event_category": "dividend",
                    "dividend_date": pd.to_datetime(ticker_data["dividendDate"]),
                    "timestamp_extracted": pd.Timestamp.utcnow(),
                }
                records.append(record)

            df = pd.DataFrame(records) if records else pd.DataFrame()
            df = fix_empty_values(df)
            return df

        except Exception as e:
            self.logger.error(f"Error normalizing calendar events for {ticker}: {e}")
            return pd.DataFrame()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get calendar events records."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing calendar events for ticker: {ticker}")
        df = self._fetch_calendar_events(ticker)
        yield from df.to_dict("records")


class DividendHistoryStream(BaseFinancialStream):
    """Stream for dividend history."""

    name = "dividend_history"
    primary_keys = ["ticker", "date", "dividends"]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("dividends", th.NumberType),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get dividend history records."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing dividend history for ticker: {ticker}")
        df = self._fetch_with_crumb_retry(
            ticker, "dividend_history", start="1900-01-01"
        )
        df = fix_empty_values(df.reset_index()).rename(columns={"symbol": "ticker"})
        yield from df.to_dict("records")


class CorporateGuidanceStream(BaseFinancialStream):
    """Stream for corporate guidance."""

    name = "corporate_guidance"
    primary_keys = ["ticker", "date"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("id", th.IntegerType),
        th.Property("significance", th.IntegerType),
        th.Property("headline", th.StringType),
        th.Property("description", th.StringType),
        th.Property("parent_topics", th.StringType),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get corporate guidance records."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing corporate guidance for ticker: {ticker}")
        df = self._fetch_with_crumb_retry(
            ticker, "corporate_guidance", is_callable=False
        )
        df.columns = clean_strings(df.columns)
        df["significance"] = df["significance"].astype(int)
        df = fix_empty_values(df)
        yield from df.to_dict("records")


class CompanyOfficersStream(BaseFinancialStream):
    """Stream for company officers."""

    name = "company_officers"
    primary_keys = ["ticker", "date", "officers"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("age", th.NumberType),
        th.Property("title", th.StringType),
        th.Property("year_born", th.NumberType),
        th.Property("fiscal_year", th.IntegerType),
        th.Property("total_pay", th.NumberType),
        th.Property("exercised_value", th.NumberType),
        th.Property("unexercised_value", th.NumberType),
        th.Property("max_age", th.NumberType),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get company officers records."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing company officers for ticker: {ticker}")
        df = self._fetch_with_crumb_retry(ticker, "company_officers", is_callable=False)
        df = df.reset_index(level=0).rename(columns={"symbol": "ticker"})
        df.columns = clean_strings(df.columns)
        df = fix_empty_values(df)
        yield from df.to_dict("records")


class NewsStream(BaseFinancialStream):
    """Stream for news articles."""

    name = "news"
    primary_keys = ["ticker", "date", "news"]
    schema = th.PropertiesList().to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get news articles records."""
        # Note: Always returns ['error'] on numerous calls across different machines, IP addresses, Operating Systems,
        # and virtual environments.

        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing news for ticker: {ticker}")
        df = self._fetch_with_crumb_retry(ticker, "news")
        df = fix_empty_values(df)
        yield from df.to_dict("records")
