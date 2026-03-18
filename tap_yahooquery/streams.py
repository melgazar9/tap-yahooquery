"""Stream type classes for tap-yahooquery."""

from __future__ import annotations

import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context
import pandas as pd
from tap_yahooquery.client import YahooQueryStream
from tap_yahooquery.schema import (
    INCOME_STMT_SCHEMA,
    ALL_FINANCIAL_DATA_SCHEMA,
    CASH_FLOW_SCHEMA,
)
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
        th.Property("date", th.DateTimeType),
        th.Property("id", th.IntegerType),
        th.Property("significance", th.IntegerType),
        th.Property("headline", th.StringType),
        th.Property("description", th.StringType),
        th.Property("parent_topics", th.StringType),
    ).to_dict()

    def _fetch_corporate_events(self, ticker: str) -> pd.DataFrame:
        """Fetch corporate events."""
        data = self._fetch_with_crumb_retry(
            ticker, "corporate_events", is_callable=False
        )
        if not isinstance(data, pd.DataFrame):
            self.logger.warning(f"No valid corporate_events data for {ticker}")
            return pd.DataFrame()

        df = fix_empty_values(data.reset_index())
        df = df.rename(columns={"symbol": "ticker"})
        if "significance" in df.columns:
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

        if not isinstance(data, dict) or ticker not in data:
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
                            "timestamp_extracted": pd.Timestamp.now("UTC"),
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
                    "timestamp_extracted": pd.Timestamp.now("UTC"),
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
        th.Property("date", th.DateTimeType),
        th.Property("dividends", th.NumberType),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get dividend history records."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing dividend history for ticker: {ticker}")

        def transform(df):
            df = fix_empty_values(df.reset_index()).rename(columns={"symbol": "ticker"})
            yield from df.to_dict("records")

        yield from self._get_dataframe_records(
            ticker, "dividend_history", transform, start="1900-01-01"
        )


class CorporateGuidanceStream(BaseFinancialStream):
    """Stream for corporate guidance."""

    name = "corporate_guidance"
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

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get corporate guidance records."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing corporate guidance for ticker: {ticker}")

        def transform(df):
            df = df.reset_index().rename(columns={"symbol": "ticker"})
            df.columns = clean_strings(df.columns)
            if "significance" in df.columns:
                df["significance"] = df["significance"].astype(int)
            df = fix_empty_values(df)
            yield from df.to_dict("records")

        yield from self._get_dataframe_records(
            ticker, "corporate_guidance", transform, is_callable=False
        )


class CompanyOfficersStream(BaseFinancialStream):
    """Stream for company officers."""

    name = "company_officers"
    primary_keys = ["surrogate_key"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]
    _surrogate_key_cols = [
        "ticker",
        "officers",
        "name",
        "age",
        "title",
        "year_born",
        "fiscal_year",
        "total_pay",
        "exercised_value",
        "unexercised_value",
    ]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("officers", th.StringType),
        th.Property("name", th.StringType),
        th.Property("age", th.IntegerType),
        th.Property("title", th.StringType),
        th.Property("year_born", th.IntegerType),
        th.Property("fiscal_year", th.IntegerType),
        th.Property("total_pay", th.NumberType),
        th.Property("exercised_value", th.NumberType),
        th.Property("unexercised_value", th.NumberType),
        th.Property("max_age", th.IntegerType),
        th.Property("surrogate_key", th.StringType),
    ).to_dict()

    def post_process(self, row: dict, context: Context | None = None) -> dict | None:
        """Convert decimal values to integers for integer fields."""
        int_fields = ["age", "year_born", "fiscal_year"]
        for field in int_fields:
            if field in row and row[field] is not None:
                row[field] = int(row[field])
        return row

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get company officers records."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing company officers for ticker: {ticker}")

        def transform(df):
            df = df.reset_index(level=0).rename(columns={"symbol": "ticker"})
            df.columns = clean_strings(df.columns)
            df = self._add_surrogate_key_to_dataframe(df)
            df = fix_empty_values(df)
            # Convert integer fields from Decimal to int in the records
            int_fields = ["age", "year_born", "fiscal_year", "max_age"]
            for record in df.to_dict("records"):
                for field in int_fields:
                    if field in record and record[field] is not None:
                        try:
                            record[field] = int(float(record[field]))
                        except (ValueError, TypeError):
                            pass
                yield record

        yield from self._get_dataframe_records(
            ticker, "company_officers", transform, is_callable=False
        )


class InsiderTransactionsStream(BaseFinancialStream):
    """Stream for insider transactions."""

    name = "insider_transactions"
    primary_keys = ["surrogate_key"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]
    _surrogate_key_cols = [
        "ticker",
        "filer_name",
        "start_date",
        "shares",
        "transaction_text",
    ]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("filer_name", th.StringType),
        th.Property("filer_relation", th.StringType),
        th.Property("filer_url", th.StringType),
        th.Property("transaction_text", th.StringType),
        th.Property("money_text", th.StringType),
        th.Property("ownership", th.StringType),
        th.Property("start_date", th.DateType),
        th.Property("shares", th.IntegerType),
        th.Property("value", th.NumberType),
        th.Property("max_age", th.IntegerType),
        th.Property("surrogate_key", th.StringType),
    ).to_dict()

    def post_process(self, row: dict, context: Context | None = None) -> dict | None:
        """Convert decimal values to integers for integer fields."""
        int_fields = ["shares", "max_age"]
        for field in int_fields:
            if field in row and row[field] is not None:
                row[field] = int(row[field])
        return row

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get insider transactions records."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing insider transactions for ticker: {ticker}")

        def transform(df):
            df = df.reset_index(level=0).rename(columns={"symbol": "ticker"})
            df = df.reset_index(drop=True)
            df.columns = clean_strings(df.columns)
            df = self._add_surrogate_key_to_dataframe(df)
            df = fix_empty_values(df)
            # Convert integer fields from Decimal to int in the records
            int_fields = ["shares", "max_age"]
            for record in df.to_dict("records"):
                for field in int_fields:
                    if field in record and record[field] is not None:
                        try:
                            record[field] = int(float(record[field]))
                        except (ValueError, TypeError):
                            pass
                yield record

        yield from self._get_dataframe_records(
            ticker, "insider_transactions", transform, is_callable=False
        )


class RecommendationTrendStream(BaseFinancialStream):
    """Stream for analyst recommendation trends."""

    name = "recommendation_trend"
    primary_keys = ["ticker", "period"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("period", th.StringType, required=True),
        th.Property("strong_buy", th.IntegerType),
        th.Property("buy", th.IntegerType),
        th.Property("hold", th.IntegerType),
        th.Property("sell", th.IntegerType),
        th.Property("strong_sell", th.IntegerType),
    ).to_dict()

    def post_process(self, row: dict, context: Context | None = None) -> dict | None:
        """Convert decimal values to integers for integer fields."""
        int_fields = ["strong_buy", "buy", "hold", "sell", "strong_sell"]
        for field in int_fields:
            if field in row and row[field] is not None:
                row[field] = int(row[field])
        return row

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get recommendation trend records."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing recommendation trend for ticker: {ticker}")

        def transform(df):
            df = df.reset_index(level=0).rename(columns={"symbol": "ticker"})
            df = df.reset_index(drop=True)
            df.columns = clean_strings(df.columns)
            df = fix_empty_values(df)
            yield from df.to_dict("records")

        yield from self._get_dataframe_records(
            ticker, "recommendation_trend", transform, is_callable=False
        )


class KeyStatsStream(BaseFinancialStream):
    """Stream for key statistics."""

    name = "key_stats"
    primary_keys = ["ticker"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("max_age", th.IntegerType),
        th.Property("price_hint", th.IntegerType),
        th.Property("enterprise_value", th.NumberType),
        th.Property("forward_pe", th.NumberType),
        th.Property("profit_margins", th.NumberType),
        th.Property("float_shares", th.NumberType),
        th.Property("shares_outstanding", th.NumberType),
        th.Property("shares_short", th.NumberType),
        th.Property("shares_short_prior_month", th.NumberType),
        th.Property("shares_short_previous_month_date", th.DateTimeType),
        th.Property("date_short_interest", th.DateTimeType),
        th.Property("shares_percent_shares_out", th.NumberType),
        th.Property("held_percent_insiders", th.NumberType),
        th.Property("held_percent_institutions", th.NumberType),
        th.Property("short_ratio", th.NumberType),
        th.Property("short_percent_of_float", th.NumberType),
        th.Property("beta", th.NumberType),
        th.Property("implied_shares_outstanding", th.NumberType),
        th.Property("category", th.StringType),
        th.Property("book_value", th.NumberType),
        th.Property("price_to_book", th.NumberType),
        th.Property("fund_family", th.StringType),
        th.Property("legal_type", th.StringType),
        th.Property("last_fiscal_year_end", th.DateTimeType),
        th.Property("next_fiscal_year_end", th.DateTimeType),
        th.Property("most_recent_quarter", th.DateTimeType),
        th.Property("earnings_quarterly_growth", th.NumberType),
        th.Property("net_income_to_common", th.NumberType),
        th.Property("trailing_eps", th.NumberType),
        th.Property("forward_eps", th.NumberType),
        th.Property("last_split_factor", th.StringType),
        th.Property("last_split_date", th.DateTimeType),
        th.Property("enterprise_to_revenue", th.NumberType),
        th.Property("enterprise_to_ebitda", th.NumberType),
        th.Property("52_week_change", th.NumberType),
        th.Property("sand_p52_week_change", th.NumberType),
        th.Property("last_dividend_value", th.NumberType),
        th.Property("last_dividend_date", th.IntegerType),
        th.Property("forward_p_e", th.NumberType),
        th.Property("latest_share_class", th.StringType),
        th.Property("lead_investor", th.StringType),
    ).to_dict()

    def post_process(self, row: dict, context: Context | None = None) -> dict | None:
        """Convert decimal values to integers for integer fields."""
        int_fields = ["max_age", "price_hint", "last_dividend_date"]
        for field in int_fields:
            if field in row and row[field] is not None:
                row[field] = int(row[field])
        return row

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get key stats records."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing key stats for ticker: {ticker}")

        def transform(data):
            stats = data[ticker]
            stats["ticker"] = ticker
            cleaned_stats = {}
            for key, value in stats.items():
                cleaned_key = clean_strings([key])[0]
                cleaned_stats[cleaned_key] = value
            cleaned_stats = fix_empty_values(pd.DataFrame([cleaned_stats])).to_dict(
                "records"
            )[0]
            yield cleaned_stats

        yield from self._get_dict_records(
            ticker, "key_stats", transform, is_callable=False
        )


class BalanceSheetStream(BaseFinancialStream):
    """Stream for balance sheets."""

    name = "balance_sheet"
    primary_keys = ["ticker", "as_of_date", "period_type"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("as_of_date", th.DateType, required=True),
        th.Property("period_type", th.StringType, required=True),
        th.Property("currency_code", th.StringType),
        # Add common balance sheet fields - there are many, using ObjectType for flexibility
        th.Property("accounts_payable", th.NumberType),
        th.Property("accounts_receivable", th.NumberType),
        th.Property("accumulated_depreciation", th.NumberType),
        th.Property("cash_and_cash_equivalents", th.NumberType),
        th.Property("cash_cash_equivalents_and_short_term_investments", th.NumberType),
        th.Property("cash_equivalents", th.NumberType),
        th.Property("cash_financial", th.NumberType),
        th.Property("commercial_paper", th.NumberType),
        th.Property("common_stock", th.NumberType),
        th.Property("current_assets", th.NumberType),
        th.Property("current_liabilities", th.NumberType),
        th.Property("goodwill", th.NumberType),
        th.Property("gross_ppe", th.NumberType),
        th.Property("intangible_assets", th.NumberType),
        th.Property("inventory", th.NumberType),
        th.Property("invested_capital", th.NumberType),
        th.Property("long_term_debt", th.NumberType),
        th.Property("net_debt", th.NumberType),
        th.Property("net_ppe", th.NumberType),
        th.Property("net_tangible_assets", th.NumberType),
        th.Property("ordinary_shares_number", th.NumberType),
        th.Property("other_current_assets", th.NumberType),
        th.Property("other_current_liabilities", th.NumberType),
        th.Property("retained_earnings", th.NumberType),
        th.Property("stockholders_equity", th.NumberType),
        th.Property("tangible_book_value", th.NumberType),
        th.Property("total_assets", th.NumberType),
        th.Property("total_debt", th.NumberType),
        th.Property("total_equity_gross_minority_interest", th.NumberType),
        th.Property("total_liabilities_net_minority_interest", th.NumberType),
        th.Property("total_non_current_assets", th.NumberType),
        th.Property(
            "total_non_current_liabilities_net_minority_interest", th.NumberType
        ),
        th.Property("working_capital", th.NumberType),
        # Additional properties found in various tickers
        th.Property("additional_paid_in_capital", th.NumberType),
        th.Property("allowance_for_doubtful_accounts_receivable", th.NumberType),
        th.Property("available_for_sale_securities", th.NumberType),
        th.Property("buildings_and_improvements", th.NumberType),
        th.Property("capital_lease_obligations", th.NumberType),
        th.Property("capital_stock", th.NumberType),
        th.Property("common_stock_equity", th.NumberType),
        th.Property("construction_in_progress", th.NumberType),
        th.Property("current_accrued_expenses", th.NumberType),
        th.Property("current_capital_lease_obligation", th.NumberType),
        th.Property("current_debt", th.NumberType),
        th.Property("current_debt_and_capital_lease_obligation", th.NumberType),
        th.Property("current_deferred_liabilities", th.NumberType),
        th.Property("current_deferred_revenue", th.NumberType),
        th.Property("current_provisions", th.NumberType),
        th.Property("employee_benefits", th.NumberType),
        th.Property("finished_goods", th.NumberType),
        th.Property("gains_losses_not_affecting_retained_earnings", th.NumberType),
        th.Property("goodwill_and_other_intangible_assets", th.NumberType),
        th.Property("gross_accounts_receivable", th.NumberType),
        th.Property("gross_p_p_e", th.NumberType),
        th.Property("income_tax_payable", th.NumberType),
        th.Property("interest_payable", th.NumberType),
        th.Property("investmentin_financial_assets", th.NumberType),
        th.Property("investments_and_advances", th.NumberType),
        th.Property("land_and_improvements", th.NumberType),
        th.Property("leases", th.NumberType),
        th.Property("long_term_capital_lease_obligation", th.NumberType),
        th.Property("long_term_debt_and_capital_lease_obligation", th.NumberType),
        th.Property("machinery_furniture_equipment", th.NumberType),
        th.Property("net_p_p_e", th.NumberType),
        th.Property("non_current_accounts_receivable", th.NumberType),
        th.Property("non_current_deferred_assets", th.NumberType),
        th.Property("non_current_deferred_liabilities", th.NumberType),
        th.Property("non_current_deferred_revenue", th.NumberType),
        th.Property("non_current_deferred_taxes_assets", th.NumberType),
        th.Property("non_current_deferred_taxes_liabilities", th.NumberType),
        th.Property("non_current_prepaid_assets", th.NumberType),
        th.Property("other_current_borrowings", th.NumberType),
        th.Property("other_equity_adjustments", th.NumberType),
        th.Property("other_intangible_assets", th.NumberType),
        th.Property("other_investments", th.NumberType),
        th.Property("other_non_current_assets", th.NumberType),
        th.Property("other_non_current_liabilities", th.NumberType),
        th.Property("other_payable", th.NumberType),
        th.Property("other_properties", th.NumberType),
        th.Property("other_receivables", th.NumberType),
        th.Property("other_short_term_investments", th.NumberType),
        th.Property("payables", th.NumberType),
        th.Property("payables_and_accrued_expenses", th.NumberType),
        th.Property(
            "pensionand_other_post_retirement_benefit_plans_current", th.NumberType
        ),
        th.Property("preferred_stock", th.NumberType),
        th.Property("prepaid_assets", th.NumberType),
        th.Property("properties", th.NumberType),
        th.Property("raw_materials", th.NumberType),
        th.Property("receivables", th.NumberType),
        th.Property("share_issued", th.NumberType),
        th.Property("taxes_receivable", th.NumberType),
        th.Property("total_capitalization", th.NumberType),
        th.Property("total_tax_payable", th.NumberType),
        th.Property("tradeand_other_payables_non_current", th.NumberType),
        th.Property("treasury_shares_number", th.NumberType),
        th.Property("treasury_stock", th.NumberType),
        th.Property("work_in_process", th.NumberType),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get balance sheet records."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing balance sheet for ticker: {ticker}")

        def transform(df):
            df = df.reset_index().rename(columns={"symbol": "ticker"})
            df.columns = clean_strings(df.columns)
            df = fix_empty_values(df)
            yield from df.to_dict("records")

        yield from self._get_dataframe_records(ticker, "balance_sheet", transform)


class InstitutionOwnershipStream(BaseFinancialStream):
    """Stream for institutional ownership."""

    name = "institution_ownership"
    primary_keys = ["ticker", "organization", "report_date"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("organization", th.StringType, required=True),
        th.Property("report_date", th.DateTimeType, required=True),
        th.Property("max_age", th.IntegerType),
        th.Property("pct_held", th.NumberType),
        th.Property("position", th.NumberType),
        th.Property("value", th.NumberType),
        th.Property("pct_change", th.NumberType),
    ).to_dict()

    def post_process(self, row: dict, context: Context | None = None) -> dict | None:
        """Convert decimal values to integers for integer fields."""
        if "max_age" in row and row["max_age"] is not None:
            row["max_age"] = int(row["max_age"])
        return row

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get institution ownership records."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing institution ownership for ticker: {ticker}")

        def transform(df):
            df = df.reset_index(level=0).rename(columns={"symbol": "ticker"})
            df = df.reset_index(drop=True)
            df.columns = clean_strings(df.columns)
            df = fix_empty_values(df)
            yield from df.to_dict("records")

        yield from self._get_dataframe_records(
            ticker, "institution_ownership", transform, is_callable=False
        )


class MajorHoldersStream(BaseFinancialStream):
    """Stream for major holders summary."""

    name = "major_holders"
    primary_keys = ["ticker"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("max_age", th.IntegerType),
        th.Property("insiders_percent_held", th.NumberType),
        th.Property("institutions_percent_held", th.NumberType),
        th.Property("institutions_float_percent_held", th.NumberType),
        th.Property("institutions_count", th.IntegerType),
    ).to_dict()

    def post_process(self, row: dict, context: Context | None = None) -> dict | None:
        """Convert decimal values to integers for integer fields."""
        int_fields = ["max_age", "institutions_count"]
        for field in int_fields:
            if field in row and row[field] is not None:
                row[field] = int(row[field])
        return row

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get major holders records."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing major holders for ticker: {ticker}")

        def transform(data):
            holders = data[ticker]
            holders["ticker"] = ticker
            cleaned_holders = {}
            for key, value in holders.items():
                cleaned_key = clean_strings([key])[0]
                cleaned_holders[cleaned_key] = value
            cleaned_holders = fix_empty_values(pd.DataFrame([cleaned_holders])).to_dict(
                "records"
            )[0]
            yield cleaned_holders

        yield from self._get_dict_records(
            ticker, "major_holders", transform, is_callable=False
        )


class EsgScoresStream(BaseFinancialStream):
    """Stream for ESG scores."""

    name = "esg_scores"
    primary_keys = ["ticker"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("max_age", th.IntegerType),
        th.Property("total_esg", th.NumberType),
        th.Property("environment_score", th.NumberType),
        th.Property("social_score", th.NumberType),
        th.Property("governance_score", th.NumberType),
        th.Property("rating_year", th.IntegerType),
        th.Property("rating_month", th.IntegerType),
        th.Property("highest_controversy", th.NumberType),
        th.Property("peer_count", th.IntegerType),
        th.Property("esg_performance", th.StringType),
        th.Property("peer_group", th.StringType),
        # Flattened peer performance metrics
        th.Property("peer_esg_score_performance_min", th.NumberType),
        th.Property("peer_esg_score_performance_avg", th.NumberType),
        th.Property("peer_esg_score_performance_max", th.NumberType),
        th.Property("peer_governance_performance_min", th.NumberType),
        th.Property("peer_governance_performance_avg", th.NumberType),
        th.Property("peer_governance_performance_max", th.NumberType),
        th.Property("peer_social_performance_min", th.NumberType),
        th.Property("peer_social_performance_avg", th.NumberType),
        th.Property("peer_social_performance_max", th.NumberType),
        th.Property("peer_environment_performance_min", th.NumberType),
        th.Property("peer_environment_performance_avg", th.NumberType),
        th.Property("peer_environment_performance_max", th.NumberType),
        th.Property("peer_highest_controversy_performance_min", th.NumberType),
        th.Property("peer_highest_controversy_performance_avg", th.NumberType),
        th.Property("peer_highest_controversy_performance_max", th.NumberType),
        th.Property("percentile", th.NumberType),
        th.Property("environment_percentile", th.NumberType),
        th.Property("social_percentile", th.NumberType),
        th.Property("governance_percentile", th.NumberType),
        th.Property("adult", th.BooleanType),
        th.Property("alcoholic", th.BooleanType),
        th.Property("animal_testing", th.BooleanType),
        th.Property("catholic", th.BooleanType),
        th.Property("controversial_weapons", th.BooleanType),
        th.Property("small_arms", th.BooleanType),
        th.Property("fur_leather", th.BooleanType),
        th.Property("gambling", th.BooleanType),
        th.Property("gmo", th.BooleanType),
        th.Property("military_contract", th.BooleanType),
        th.Property("nuclear", th.BooleanType),
        th.Property("pesticides", th.BooleanType),
        th.Property("palm_oil", th.BooleanType),
        th.Property("coal", th.BooleanType),
        th.Property("tobacco", th.BooleanType),
    ).to_dict()

    def post_process(self, row: dict, context: Context | None = None) -> dict | None:
        """Convert decimal values to integers for integer fields."""
        int_fields = ["max_age", "rating_year", "rating_month", "peer_count"]
        for field in int_fields:
            if field in row and row[field] is not None:
                row[field] = int(row[field])
        return row

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get ESG scores records."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing ESG scores for ticker: {ticker}")

        def transform(data):
            esg = data[ticker]
            esg["ticker"] = ticker
            peer_fields = [
                "peerEsgScorePerformance",
                "peerGovernancePerformance",
                "peerSocialPerformance",
                "peerEnvironmentPerformance",
                "peerHighestControversyPerformance",
            ]
            for field in peer_fields:
                if field in esg and isinstance(esg[field], dict):
                    peer_data = esg.pop(field)
                    for sub_key, sub_value in peer_data.items():
                        esg[f"{field}_{sub_key}"] = sub_value
            cleaned_esg = {}
            for key, value in esg.items():
                cleaned_key = clean_strings([key])[0]
                cleaned_esg[cleaned_key] = value
            cleaned_esg = fix_empty_values(pd.DataFrame([cleaned_esg])).to_dict(
                "records"
            )[0]
            yield cleaned_esg

        yield from self._get_dict_records(
            ticker, "esg_scores", transform, is_callable=False
        )


class EarningsStream(YahooQueryStream):
    """Stream for fully flattened earnings data."""

    name = "earnings"
    primary_keys = ["ticker", "date", "type"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("date", th.StringType, required=True),
        th.Property(
            "type", th.StringType, required=True
        ),  # 'eps', 'quarterly', 'yearly', 'summary', 'earnings_date'
        th.Property("actual", th.NumberType),
        th.Property("estimate", th.NumberType),
        th.Property("revenue", th.NumberType),
        th.Property("earnings", th.NumberType),
        th.Property("currency", th.StringType),
        th.Property("max_age", th.IntegerType),
        th.Property("current_quarter_estimate", th.NumberType),
        th.Property("current_quarter_estimate_date", th.StringType),
        th.Property("current_quarter_estimate_year", th.NumberType),
        th.Property("earnings_date", th.StringType),
        th.Property("is_earnings_date_estimate", th.BooleanType),
    ).to_dict()

    def _fetch_earnings(self, ticker: str) -> pd.DataFrame:
        data = self._fetch_with_crumb_retry(ticker, "earnings", is_callable=False)
        if not isinstance(data, dict) or ticker not in data:
            self.logger.warning(f"No earnings data found for ticker: {ticker}")
            return pd.DataFrame()

        d = data[ticker]
        if not isinstance(d, dict):
            self.logger.warning(f"Invalid earnings data for ticker {ticker}: {d}")
            return pd.DataFrame()

        max_age = d.get("maxAge")
        currency = d.get("financialCurrency")
        earnings_chart = d.get("earningsChart", {})
        financials_chart = d.get("financialsChart", {})

        records = []

        # EPS rows (only include actual/estimate)
        for row in earnings_chart.get("quarterly", []):
            records.append(
                {
                    "ticker": ticker,
                    "date": row.get("date"),
                    "type": "eps",
                    "actual": row.get("actual"),
                    "estimate": row.get("estimate"),
                    "currency": currency,
                    "max_age": max_age,
                    "current_quarter_estimate": None,
                    "current_quarter_estimate_date": None,
                    "current_quarter_estimate_year": None,
                    "earnings_date": None,
                    "is_earnings_date_estimate": None,
                    "revenue": None,
                    "earnings": None,
                }
            )

        # Financials rows (quarterly/yearly, only revenue/earnings)
        for period, ptype in [("quarterly", "quarterly"), ("yearly", "yearly")]:
            for row in financials_chart.get(period, []):
                records.append(
                    {
                        "ticker": ticker,
                        "date": str(row.get("date")),
                        "type": ptype,
                        "actual": None,
                        "estimate": None,
                        "currency": currency,
                        "max_age": max_age,
                        "current_quarter_estimate": None,
                        "current_quarter_estimate_date": None,
                        "current_quarter_estimate_year": None,
                        "earnings_date": None,
                        "is_earnings_date_estimate": None,
                        "revenue": row.get("revenue"),
                        "earnings": row.get("earnings"),
                    }
                )

        # Current quarter estimate row
        if "currentQuarterEstimate" in earnings_chart:
            records.append(
                {
                    "ticker": ticker,
                    "date": earnings_chart.get("currentQuarterEstimateDate"),
                    "type": "current_quarter_estimate",
                    "actual": earnings_chart.get("currentQuarterEstimate"),
                    "estimate": None,
                    "currency": currency,
                    "max_age": max_age,
                    "current_quarter_estimate": earnings_chart.get(
                        "currentQuarterEstimate"
                    ),
                    "current_quarter_estimate_date": earnings_chart.get(
                        "currentQuarterEstimateDate"
                    ),
                    "current_quarter_estimate_year": earnings_chart.get(
                        "currentQuarterEstimateYear"
                    ),
                    "earnings_date": None,
                    "is_earnings_date_estimate": None,
                    "revenue": None,
                    "earnings": None,
                }
            )

        # Earnings date rows (metadata)
        for e_date in earnings_chart.get("earningsDate", []):
            records.append(
                {
                    "ticker": ticker,
                    "date": e_date,
                    "type": "earnings_date",
                    "actual": None,
                    "estimate": None,
                    "currency": currency,
                    "max_age": max_age,
                    "current_quarter_estimate": None,
                    "current_quarter_estimate_date": None,
                    "current_quarter_estimate_year": None,
                    "earnings_date": e_date,
                    "is_earnings_date_estimate": earnings_chart.get(
                        "isEarningsDateEstimate"
                    ),
                    "revenue": None,
                    "earnings": None,
                }
            )

        return pd.DataFrame.from_records(records)

    def get_records(self, context):
        """Yield earnings records for a given context."""
        ticker = self._get_ticker_from_context(context)
        df = self._fetch_earnings(ticker)
        df = fix_empty_values(df)
        df.columns = clean_strings(df.columns)
        yield from df.to_dict("records")


class EarningsHistoryStream(YahooQueryStream):
    """Stream for earnings history."""

    name = "earnings_history"
    primary_keys = ["ticker", "quarter"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]
    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("quarter", th.StringType, required=True),
        th.Property("max_age", th.NumberType),
        th.Property("eps_actual", th.NumberType),
        th.Property("eps_estimate", th.NumberType),
        th.Property("eps_difference", th.NumberType),
        th.Property("surprise_percent", th.NumberType),
        th.Property("currency", th.StringType),
        th.Property("period", th.StringType),
    ).to_dict()

    def _fetch_earnings_history(self, ticker: str):
        """Fetch earnings history."""
        df = self._fetch_with_crumb_retry(ticker, "earning_history", is_callable=False)
        df = fix_empty_values(df)
        df.columns = clean_strings(df.columns)
        return df

    def get_records(self, context):
        ticker = self._get_ticker_from_context(context)
        df = self._fetch_earnings_history(ticker)
        yield from df.to_dict("records")


class EarningsTrendStream(YahooQueryStream):
    """Stream for earnings trend."""

    name = "earnings_trend"
    primary_keys = ["ticker", "period", "end_date"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]
    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("period", th.StringType, required=True),
        th.Property("end_date", th.StringType),
        th.Property("growth", th.AnyType()),
        th.Property("earnings_avg", th.AnyType()),
        th.Property("earnings_low", th.AnyType()),
        th.Property("earnings_high", th.AnyType()),
        th.Property("earnings_year_ago_eps", th.AnyType()),
        th.Property("earnings_num_analysts", th.AnyType()),
        th.Property("earnings_growth", th.AnyType()),
        th.Property("earnings_currency", th.StringType),
        th.Property("revenue_avg", th.AnyType()),
        th.Property("revenue_low", th.AnyType()),
        th.Property("revenue_high", th.AnyType()),
        th.Property("revenue_num_analysts", th.AnyType()),
        th.Property("revenue_year_ago", th.AnyType()),
        th.Property("revenue_growth", th.AnyType()),
        th.Property("revenue_currency", th.StringType),
        th.Property("eps_trend_current", th.AnyType()),
        th.Property("eps_trend_7days_ago", th.AnyType()),
        th.Property("eps_trend_30days_ago", th.AnyType()),
        th.Property("eps_trend_60days_ago", th.AnyType()),
        th.Property("eps_trend_90days_ago", th.AnyType()),
        th.Property("eps_trend_currency", th.StringType),
        th.Property("eps_up_last_7days", th.AnyType()),
        th.Property("eps_up_last_30days", th.AnyType()),
        th.Property("eps_down_last_7days", th.AnyType()),
        th.Property("eps_down_last_30days", th.AnyType()),
        th.Property("eps_down_last_90days", th.ObjectType()),
        th.Property("eps_revisions_currency", th.StringType),
    ).to_dict()

    def _fetch_earnings_trend(self, ticker: str):
        """Fetch earnings trend data."""
        data = self._fetch_with_crumb_retry(ticker, "earnings_trend", is_callable=False)
        records = []
        for ticker, ticker_data in data.items():
            if not isinstance(ticker_data, dict):
                self.logger.warning(
                    f"Invalid earnings_trend data for ticker {ticker}: {ticker_data}"
                )
                continue
            for trend in ticker_data.get("trend", []):
                record = {
                    "symbol": ticker,
                    "period": trend["period"],
                    "end_date": trend["endDate"],
                    "growth": trend["growth"],
                    # Earnings Estimate
                    "earnings_avg": trend["earningsEstimate"].get("avg"),
                    "earnings_low": trend["earningsEstimate"].get("low"),
                    "earnings_high": trend["earningsEstimate"].get("high"),
                    "earnings_year_ago_eps": trend["earningsEstimate"].get(
                        "yearAgoEps"
                    ),
                    "earnings_num_analysts": trend["earningsEstimate"].get(
                        "numberOfAnalysts"
                    ),
                    "earnings_growth": trend["earningsEstimate"].get("growth"),
                    "earnings_currency": trend["earningsEstimate"].get(
                        "earningsCurrency"
                    ),
                    # Revenue Estimate
                    "revenue_avg": trend["revenueEstimate"].get("avg"),
                    "revenue_low": trend["revenueEstimate"].get("low"),
                    "revenue_high": trend["revenueEstimate"].get("high"),
                    "revenue_num_analysts": trend["revenueEstimate"].get(
                        "numberOfAnalysts"
                    ),
                    "revenue_year_ago": trend["revenueEstimate"].get("yearAgoRevenue"),
                    "revenue_growth": trend["revenueEstimate"].get("growth"),
                    "revenue_currency": trend["revenueEstimate"].get("revenueCurrency"),
                    # EPS Trend
                    "eps_trend_current": trend["epsTrend"].get("current"),
                    "eps_trend_7days_ago": trend["epsTrend"].get("7daysAgo"),
                    "eps_trend_30days_ago": trend["epsTrend"].get("30daysAgo"),
                    "eps_trend_60days_ago": trend["epsTrend"].get("60daysAgo"),
                    "eps_trend_90days_ago": trend["epsTrend"].get("90daysAgo"),
                    "eps_trend_currency": trend["epsTrend"].get("epsTrendCurrency"),
                    # EPS Revisions
                    "eps_up_last_7days": trend["epsRevisions"].get("upLast7days"),
                    "eps_up_last_30days": trend["epsRevisions"].get("upLast30days"),
                    "eps_down_last_7days": trend["epsRevisions"].get("downLast7Days"),
                    "eps_down_last_30days": trend["epsRevisions"].get("downLast30days"),
                    "eps_down_last_90days": trend["epsRevisions"].get("downLast90days"),
                    "eps_revisions_currency": trend["epsRevisions"].get(
                        "epsRevisionsCurrency"
                    ),
                }
                records.append(record)
        df = fix_empty_values(pd.DataFrame(records))
        df = df.rename(columns={"symbol": "ticker"})
        if "end_date" not in df.columns:
            df["end_date"] = None
        df.columns = clean_strings(df.columns)
        return df

    def get_records(self, context):
        ticker = self._get_ticker_from_context(context)
        df = self._fetch_earnings_trend(ticker)
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


class CashFlowStream(BaseFinancialStream):
    """Stream for cash flow statements."""

    name = "cash_flow"
    primary_keys = ["ticker", "as_of_date", "period_type"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]

    schema = CASH_FLOW_SCHEMA

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get cash flow records."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing cash_flow for ticker: {ticker}")
        extra_renames = {
            "NetPPEPurchaseAndSale": "net_ppe_purchase_and_sale",
            "PurchaseOfPPE": "purchase_of_ppe",
        }
        yield from self._get_dataframe_records(
            ticker,
            "cash_flow",
            self._make_df_transform(
                extra_renames=extra_renames, date_columns={"as_of_date": "%Y-%m-%d"}
            ),
        )


class PriceHistoryStream(BaseFinancialStream):
    """Stream for historical OHLCV price data."""

    name = "price_history"
    primary_keys = ["ticker", "date"]
    _valid_segments = None  # All ticker types

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("date", th.DateType, required=True),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("adjclose", th.NumberType),
        th.Property("dividends", th.NumberType),
        th.Property("splits", th.NumberType),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get historical price records."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing price_history for ticker: {ticker}")
        start_date = self.config.get("start_date", "1900-01-01")
        yield from self._get_dataframe_records(
            ticker,
            "history",
            self._make_df_transform(date_columns={"date": "%Y-%m-%d"}),
            start=start_date,
        )


class SummaryDetailStream(BaseFinancialStream):
    """Stream for summary detail data (market cap, PE, dividend yield, etc.)."""

    name = "summary_detail"
    primary_keys = ["ticker"]
    _valid_segments = None  # All ticker types

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("max_age", th.IntegerType),
        th.Property("price_hint", th.IntegerType),
        th.Property("previous_close", th.NumberType),
        th.Property("open", th.NumberType),
        th.Property("day_low", th.NumberType),
        th.Property("day_high", th.NumberType),
        th.Property("regular_market_previous_close", th.NumberType),
        th.Property("regular_market_open", th.NumberType),
        th.Property("regular_market_day_low", th.NumberType),
        th.Property("regular_market_day_high", th.NumberType),
        th.Property("dividend_rate", th.NumberType),
        th.Property("dividend_yield", th.NumberType),
        th.Property("ex_dividend_date", th.StringType),
        th.Property("payout_ratio", th.NumberType),
        th.Property("five_year_avg_dividend_yield", th.NumberType),
        th.Property("beta", th.NumberType),
        th.Property("trailing_pe", th.NumberType),
        th.Property("forward_pe", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("regular_market_volume", th.NumberType),
        th.Property("average_volume", th.NumberType),
        th.Property("average_volume10days", th.NumberType),
        th.Property("average_daily_volume10_day", th.NumberType),
        th.Property("bid", th.NumberType),
        th.Property("ask", th.NumberType),
        th.Property("bid_size", th.NumberType),
        th.Property("ask_size", th.NumberType),
        th.Property("market_cap", th.NumberType),
        th.Property("non_diluted_market_cap", th.NumberType),
        th.Property("fifty_two_week_low", th.NumberType),
        th.Property("fifty_two_week_high", th.NumberType),
        th.Property("all_time_high", th.NumberType),
        th.Property("all_time_low", th.NumberType),
        th.Property("price_to_sales_trailing12_months", th.NumberType),
        th.Property("fifty_day_average", th.NumberType),
        th.Property("two_hundred_day_average", th.NumberType),
        th.Property("trailing_annual_dividend_rate", th.NumberType),
        th.Property("trailing_annual_dividend_yield", th.NumberType),
        th.Property("currency", th.StringType),
        th.Property("from_currency", th.StringType),
        th.Property("to_currency", th.StringType),
        th.Property("last_market", th.StringType),
        th.Property("coin_market_cap_link", th.StringType),
        th.Property("algorithm", th.StringType),
        th.Property("tradeable", th.BooleanType),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing summary_detail for ticker: {ticker}")
        yield from self._get_dict_records(
            ticker,
            "summary_detail",
            self._make_dict_transform(
                ticker,
                extra_renames={
                    "trailingPE": "trailing_pe",
                    "forwardPE": "forward_pe",
                },
            ),
            is_callable=False,
        )


class AssetProfileStream(BaseFinancialStream):
    """Stream for company profile (sector, industry, description, etc.)."""

    name = "asset_profile"
    primary_keys = ["ticker"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("address1", th.StringType),
        th.Property("address2", th.StringType),
        th.Property("city", th.StringType),
        th.Property("state", th.StringType),
        th.Property("zip", th.StringType),
        th.Property("country", th.StringType),
        th.Property("phone", th.StringType),
        th.Property("website", th.StringType),
        th.Property("industry", th.StringType),
        th.Property("industry_key", th.StringType),
        th.Property("industry_disp", th.StringType),
        th.Property("sector", th.StringType),
        th.Property("sector_key", th.StringType),
        th.Property("sector_disp", th.StringType),
        th.Property("long_business_summary", th.StringType),
        th.Property("full_time_employees", th.IntegerType),
        th.Property("audit_risk", th.IntegerType),
        th.Property("board_risk", th.IntegerType),
        th.Property("compensation_risk", th.IntegerType),
        th.Property("share_holder_rights_risk", th.IntegerType),
        th.Property("overall_risk", th.IntegerType),
        th.Property("governance_epoch_date", th.StringType),
        th.Property("compensation_as_of_epoch_date", th.StringType),
        th.Property("ir_website", th.StringType),
        th.Property("max_age", th.IntegerType),
    ).to_dict()

    # Fields to exclude (nested objects handled by other streams)
    _exclude_fields = {"companyOfficers", "executiveTeam"}

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing asset_profile for ticker: {ticker}")
        yield from self._get_dict_records(
            ticker,
            "asset_profile",
            self._make_dict_transform(
                ticker, exclude_nested=True, exclude_fields=self._exclude_fields
            ),
            is_callable=False,
        )


class FinancialDataStream(BaseFinancialStream):
    """Stream for current financial data snapshot (revenue, margins, cash, debt)."""

    name = "financial_data"
    primary_keys = ["ticker"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("max_age", th.IntegerType),
        th.Property("current_price", th.NumberType),
        th.Property("target_high_price", th.NumberType),
        th.Property("target_low_price", th.NumberType),
        th.Property("target_mean_price", th.NumberType),
        th.Property("target_median_price", th.NumberType),
        th.Property("recommendation_mean", th.NumberType),
        th.Property("recommendation_key", th.StringType),
        th.Property("number_of_analyst_opinions", th.IntegerType),
        th.Property("total_cash", th.NumberType),
        th.Property("total_cash_per_share", th.NumberType),
        th.Property("ebitda", th.NumberType),
        th.Property("total_debt", th.NumberType),
        th.Property("quick_ratio", th.NumberType),
        th.Property("current_ratio", th.NumberType),
        th.Property("total_revenue", th.NumberType),
        th.Property("debt_to_equity", th.NumberType),
        th.Property("revenue_per_share", th.NumberType),
        th.Property("return_on_assets", th.NumberType),
        th.Property("return_on_equity", th.NumberType),
        th.Property("gross_profits", th.NumberType),
        th.Property("free_cashflow", th.NumberType),
        th.Property("operating_cashflow", th.NumberType),
        th.Property("earnings_growth", th.NumberType),
        th.Property("revenue_growth", th.NumberType),
        th.Property("gross_margins", th.NumberType),
        th.Property("ebitda_margins", th.NumberType),
        th.Property("operating_margins", th.NumberType),
        th.Property("profit_margins", th.NumberType),
        th.Property("financial_currency", th.StringType),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing financial_data for ticker: {ticker}")
        yield from self._get_dict_records(
            ticker,
            "financial_data",
            self._make_dict_transform(ticker, exclude_nested=True),
            is_callable=False,
        )


class GradingHistoryStream(BaseFinancialStream):
    """Stream for analyst upgrades/downgrades history."""

    name = "grading_history"
    primary_keys = ["ticker", "epoch_grade_date", "firm"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("epoch_grade_date", th.StringType, required=True),
        th.Property("firm", th.StringType, required=True),
        th.Property("to_grade", th.StringType),
        th.Property("from_grade", th.StringType),
        th.Property("action", th.StringType),
        th.Property("price_target_action", th.StringType),
        th.Property("current_price_target", th.NumberType),
        th.Property("prior_price_target", th.NumberType),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get grading history records."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing grading_history for ticker: {ticker}")
        yield from self._get_dataframe_records(
            ticker,
            "grading_history",
            self._make_df_transform(drop_columns=["row"]),
            is_callable=False,
        )


class ValuationMeasuresStream(BaseFinancialStream):
    """Stream for valuation measures over time (PE, PB, PS, EV/EBITDA)."""

    name = "valuation_measures"
    primary_keys = ["ticker", "as_of_date"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("as_of_date", th.DateType, required=True),
        th.Property("period_type", th.StringType),
        th.Property("enterprise_value", th.NumberType),
        th.Property("enterprises_value_ebitda_ratio", th.NumberType),
        th.Property("enterprises_value_revenue_ratio", th.NumberType),
        th.Property("forward_pe_ratio", th.NumberType),
        th.Property("market_cap", th.NumberType),
        th.Property("pb_ratio", th.NumberType),
        th.Property("pe_ratio", th.NumberType),
        th.Property("peg_ratio", th.NumberType),
        th.Property("ps_ratio", th.NumberType),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get valuation measures records."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing valuation_measures for ticker: {ticker}")
        extra_renames = {
            "EnterprisesValueEBITDARatio": "enterprises_value_ebitda_ratio",
            "EnterprisesValueRevenueRatio": "enterprises_value_revenue_ratio",
        }
        yield from self._get_dataframe_records(
            ticker,
            "valuation_measures",
            self._make_df_transform(
                extra_renames=extra_renames, date_columns={"as_of_date": "%Y-%m-%d"}
            ),
            is_callable=False,
        )


class InsiderHoldersStream(BaseFinancialStream):
    """Stream for insider holders data."""

    name = "insider_holders"
    primary_keys = ["ticker", "name", "latest_trans_date", "transaction_description"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("name", th.StringType, required=True),
        th.Property("relation", th.StringType),
        th.Property("url", th.StringType),
        th.Property("transaction_description", th.StringType),
        th.Property("latest_trans_date", th.StringType, required=True),
        th.Property("position_direct", th.NumberType),
        th.Property("position_direct_date", th.StringType),
        th.Property("max_age", th.IntegerType),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get insider holders records."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing insider_holders for ticker: {ticker}")
        yield from self._get_dataframe_records(
            ticker,
            "insider_holders",
            self._make_df_transform(drop_columns=["row"]),
            is_callable=False,
        )


class OptionChainStream(BaseFinancialStream):
    """Stream for options chain data."""

    name = "option_chain"
    primary_keys = ["ticker", "expiration", "option_type", "contract_symbol"]
    _valid_segments = [
        "stock_tickers",
    ]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("expiration", th.StringType, required=True),
        th.Property("option_type", th.StringType, required=True),
        th.Property("contract_symbol", th.StringType, required=True),
        th.Property("strike", th.NumberType),
        th.Property("currency", th.StringType),
        th.Property("last_price", th.NumberType),
        th.Property("change", th.NumberType),
        th.Property("percent_change", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("open_interest", th.NumberType),
        th.Property("bid", th.NumberType),
        th.Property("ask", th.NumberType),
        th.Property("contract_size", th.StringType),
        th.Property("last_trade_date", th.DateTimeType),
        th.Property("implied_volatility", th.NumberType),
        th.Property("in_the_money", th.BooleanType),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get option chain records."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing option_chain for ticker: {ticker}")

        yield from self._get_dataframe_records(
            ticker,
            "option_chain",
            self._make_df_transform(
                extra_renames={"optionType": "option_type"},
                date_columns={"last_trade_date": "%Y-%m-%dT%H:%M:%S"},
            ),
            is_callable=False,
        )


class PriceStream(BaseFinancialStream):
    """Stream for current price and market data."""

    name = "price"
    primary_keys = ["ticker"]
    _valid_segments = None  # All ticker types

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("max_age", th.IntegerType),
        th.Property("pre_market_change_percent", th.NumberType),
        th.Property("pre_market_change", th.NumberType),
        th.Property("pre_market_time", th.StringType),
        th.Property("pre_market_price", th.NumberType),
        th.Property("pre_market_source", th.StringType),
        th.Property("post_market_change_percent", th.NumberType),
        th.Property("post_market_change", th.NumberType),
        th.Property("post_market_time", th.StringType),
        th.Property("post_market_price", th.NumberType),
        th.Property("post_market_source", th.StringType),
        th.Property("regular_market_change_percent", th.NumberType),
        th.Property("regular_market_change", th.NumberType),
        th.Property("regular_market_time", th.StringType),
        th.Property("price_hint", th.IntegerType),
        th.Property("regular_market_price", th.NumberType),
        th.Property("regular_market_day_high", th.NumberType),
        th.Property("regular_market_day_low", th.NumberType),
        th.Property("regular_market_volume", th.NumberType),
        th.Property("regular_market_previous_close", th.NumberType),
        th.Property("regular_market_source", th.StringType),
        th.Property("regular_market_open", th.NumberType),
        th.Property("exchange", th.StringType),
        th.Property("exchange_name", th.StringType),
        th.Property("exchange_data_delayed_by", th.IntegerType),
        th.Property("market_state", th.StringType),
        th.Property("quote_type", th.StringType),
        th.Property("short_name", th.StringType),
        th.Property("long_name", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("quote_source_name", th.StringType),
        th.Property("currency_symbol", th.StringType),
        th.Property("from_currency", th.StringType),
        th.Property("to_currency", th.StringType),
        th.Property("last_market", th.StringType),
        th.Property("market_cap", th.NumberType),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get price records."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing price for ticker: {ticker}")
        yield from self._get_dict_records(
            ticker,
            "price",
            self._make_dict_transform(
                ticker,
                exclude_nested=True,
                exclude_fields={"symbol", "underlyingSymbol"},
            ),
            is_callable=False,
        )


class SharePurchaseActivityStream(BaseFinancialStream):
    """Stream for share purchase/buyback activity."""

    name = "share_purchase_activity"
    primary_keys = ["ticker"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("max_age", th.IntegerType),
        th.Property("period", th.StringType),
        th.Property("buy_info_count", th.IntegerType),
        th.Property("buy_info_shares", th.IntegerType),
        th.Property("buy_percent_insider_shares", th.NumberType),
        th.Property("sell_info_count", th.IntegerType),
        th.Property("sell_info_shares", th.IntegerType),
        th.Property("sell_percent_insider_shares", th.NumberType),
        th.Property("net_info_count", th.IntegerType),
        th.Property("net_info_shares", th.IntegerType),
        th.Property("net_percent_insider_shares", th.NumberType),
        th.Property("total_insider_shares", th.IntegerType),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing share_purchase_activity for ticker: {ticker}")
        yield from self._get_dict_records(
            ticker,
            "share_purchase_activity",
            self._make_dict_transform(ticker, exclude_nested=True),
            is_callable=False,
        )


class SummaryProfileStream(BaseFinancialStream):
    """Stream for company summary profile."""

    name = "summary_profile"
    primary_keys = ["ticker"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("address1", th.StringType),
        th.Property("address2", th.StringType),
        th.Property("city", th.StringType),
        th.Property("state", th.StringType),
        th.Property("zip", th.StringType),
        th.Property("country", th.StringType),
        th.Property("phone", th.StringType),
        th.Property("website", th.StringType),
        th.Property("industry", th.StringType),
        th.Property("industry_key", th.StringType),
        th.Property("industry_disp", th.StringType),
        th.Property("sector", th.StringType),
        th.Property("sector_key", th.StringType),
        th.Property("sector_disp", th.StringType),
        th.Property("long_business_summary", th.StringType),
        th.Property("full_time_employees", th.IntegerType),
        th.Property("ir_website", th.StringType),
        th.Property("max_age", th.IntegerType),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing summary_profile for ticker: {ticker}")
        yield from self._get_dict_records(
            ticker,
            "summary_profile",
            self._make_dict_transform(ticker, exclude_nested=True),
            is_callable=False,
        )


class TechnicalEventsStream(BaseFinancialStream):
    """Stream for technical trading events (support/resistance, patterns, signals)."""

    name = "technical_events"
    primary_keys = ["ticker", "id"]
    _valid_segments = [
        "stock_tickers",
        "private_companies_tickers",
    ]

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("id", th.StringType, required=True),
        # Summary fields (denormalized onto each event row)
        th.Property("short_term_outlook", th.StringType),
        th.Property("mid_term_outlook", th.StringType),
        th.Property("long_term_outlook", th.StringType),
        th.Property("support", th.NumberType),
        th.Property("resistance", th.NumberType),
        th.Property("stop_loss", th.NumberType),
        # Event fields
        th.Property("trading_horizon", th.StringType),
        th.Property("start_date", th.NumberType),
        th.Property("end_date", th.NumberType),
        th.Property("event_type", th.StringType),
        th.Property("duration", th.IntegerType),
        th.Property("inbound_trend_duration", th.IntegerType),
        th.Property("price_period", th.StringType),
        th.Property("price_open", th.NumberType),
        th.Property("price_close", th.NumberType),
        th.Property("price_high", th.NumberType),
        th.Property("price_low", th.NumberType),
        th.Property("price_volume", th.NumberType),
        th.Property("price_date", th.NumberType),
        th.Property("trade_type", th.StringType),
        th.Property("close_position", th.StringType),
        th.Property("lower", th.NumberType),
        th.Property("upper", th.NumberType),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get technical events records."""
        ticker = self._get_ticker_from_context(context)
        self.logger.info(f"Processing technical_events for ticker: {ticker}")

        def transform(data):
            ticker_data = data[ticker]
            if not isinstance(ticker_data, dict):
                return

            summary = ticker_data.get("technicalEvents", {})
            events = ticker_data.get("events", [])

            for event in events:
                record = {
                    "ticker": ticker,
                    "id": event.get("id"),
                    # Summary (denormalized)
                    "short_term_outlook": summary.get("shortTerm"),
                    "mid_term_outlook": summary.get("midTerm"),
                    "long_term_outlook": summary.get("longTerm"),
                    "support": summary.get("support"),
                    "resistance": summary.get("resistance"),
                    "stop_loss": summary.get("stopLoss"),
                    # Event
                    "trading_horizon": event.get("tradingHorizon"),
                    "start_date": event.get("startDate"),
                    "end_date": event.get("endDate"),
                    "event_type": event.get("eventType"),
                    "duration": event.get("duration"),
                    "inbound_trend_duration": event.get("inboundTrendDuration"),
                    "price_period": event.get("pricePeriod"),
                    "price_open": event.get("priceOpen"),
                    "price_close": event.get("priceClose"),
                    "price_high": event.get("priceHigh"),
                    "price_low": event.get("priceLow"),
                    "price_volume": event.get("priceVolume"),
                    "price_date": event.get("priceDate"),
                    "trade_type": event.get("tradeType"),
                    "close_position": event.get("closePosition"),
                    "lower": event.get("lower"),
                    "upper": event.get("upper"),
                }
                yield record

        yield from self._get_dict_records(
            ticker, "p_technical_events", transform, is_callable=False
        )
