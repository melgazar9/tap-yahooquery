"""YahooQuery tap class."""

from __future__ import annotations

import pathlib
import warnings

from singer_sdk import Tap
from singer_sdk import typing as th
from singer_sdk.helpers._util import read_json_file

import typing as t
from tap_yahooquery.streams import (
    TickersStream,
    SecFilingsStream,
    IncomeStmtStream,
    AllFinancialDataStream,
    CorporateEventsStream,
    CalendarEventsStream,
    DividendHistoryStream,
    CorporateGuidanceStream,
    CompanyOfficersStream,
    InsiderTransactionsStream,
    RecommendationTrendStream,
    KeyStatsStream,
    BalanceSheetStream,
    InstitutionOwnershipStream,
    MajorHoldersStream,
    EsgScoresStream,
    EarningsHistoryStream,
    EarningsTrendStream,
    EarningsStream,
    # NewsStream,
    CashFlowStream,
    PriceHistoryStream,
    SummaryDetailStream,
    AssetProfileStream,
    FinancialDataStream,
    GradingHistoryStream,
    ValuationMeasuresStream,
    InsiderHoldersStream,
    OptionChainStream,
    PriceStream,
    SharePurchaseActivityStream,
    SummaryProfileStream,
    TechnicalEventsStream,
)


class TapYahooQuery(Tap):
    """YahooQuery tap class."""

    name = "tap-yahooquery"

    _cached_tickers: t.List[dict] | None = None
    _tickers_stream_instance: TickersStream | None = None

    def __init__(self, *args, catalog=None, **kwargs):
        """Convert catalog file path to dict to avoid SDK deprecation warning."""
        if isinstance(catalog, (str, pathlib.PurePath)):
            catalog = read_json_file(catalog)
        super().__init__(*args, catalog=catalog, **kwargs)

    config_jsonschema = th.PropertiesList(
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Start date for data extraction",
        ),
        th.Property(
            "tickers",
            th.ObjectType(
                th.Property(
                    "select_tickers",
                    th.OneOf(th.StringType, th.ArrayType(th.StringType)),
                ),
            ),
            description="Ticker configuration including selection and query params",
            required=True,
        ),
        th.Property(
            "proxies",
            th.OneOf(th.StringType, th.ArrayType(th.StringType)),
            description=(
                "Proxy URL(s) for rotating IPs. "
                "Single string for one proxy or auto-rotating provider, "
                "list for round-robin rotation. "
                "Format: http://user:pass@host:port or socks5://host:port"
            ),
        ),
        th.Property(
            "sec_filings",
            th.ObjectType(
                th.Property("use_cached_tickers", th.BooleanType),
            ),
            description="SEC filings stream configuration",
        ),
        th.Property(
            "income_stmt",
            th.ObjectType(
                th.Property("use_cached_tickers", th.BooleanType),
            ),
            description="Income Statement stream configuration",
        ),
        th.Property(
            "all_financial_data",
            th.ObjectType(
                th.Property("use_cached_tickers", th.BooleanType),
            ),
            description="All Financial Data stream configuration",
        ),
    ).to_dict()

    def get_cached_tickers(self) -> t.List[dict]:
        if self._cached_tickers is None:
            self.logger.info("Fetching and caching tickers...")
            tickers_stream = self.get_tickers_stream()
            self._cached_tickers = list(tickers_stream.get_records(context=None))
            self.logger.info(f"Cached {len(self._cached_tickers)} tickers.")
        return self._cached_tickers

    def get_tickers_stream(self) -> TickersStream:
        if self._tickers_stream_instance is None:
            self.logger.info("Creating TickersStream instance...")
            self._tickers_stream_instance = TickersStream(self)
        return self._tickers_stream_instance

    def discover_streams(self) -> list:
        """Return a list of discovered streams."""
        return [
            TickersStream(self),
            SecFilingsStream(self),
            IncomeStmtStream(self),
            AllFinancialDataStream(self),
            CorporateEventsStream(self),
            CalendarEventsStream(self),
            DividendHistoryStream(self),
            CorporateGuidanceStream(self),
            CompanyOfficersStream(self),
            InsiderTransactionsStream(self),
            RecommendationTrendStream(self),
            KeyStatsStream(self),
            BalanceSheetStream(self),
            InstitutionOwnershipStream(self),
            MajorHoldersStream(self),
            EsgScoresStream(self),
            EarningsHistoryStream(self),
            EarningsTrendStream(self),
            EarningsStream(self),
            # NewsStream(self),
            CashFlowStream(self),
            PriceHistoryStream(self),
            SummaryDetailStream(self),
            AssetProfileStream(self),
            FinancialDataStream(self),
            GradingHistoryStream(self),
            ValuationMeasuresStream(self),
            InsiderHoldersStream(self),
            OptionChainStream(self),
            PriceStream(self),
            SharePurchaseActivityStream(self),
            SummaryProfileStream(self),
            TechnicalEventsStream(self),
        ]


if __name__ == "__main__":
    TapYahooQuery.cli()
