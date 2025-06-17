"""Stream type classes for tap-yahooquery."""

from __future__ import annotations

import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_yahooquery.client import YahooQueryStream
import yahooquery as yq
from tap_yahooquery.helpers import fix_empty_values, clean_strings
from datetime import datetime


class SecFilingsStream(YahooQueryStream):
    """Stream for SEC filings data."""

    name = "sec_filings"
    method_name = "get_sec_filings"
    primary_keys = ["ticker", "date", "type", "title"]
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

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get records for the current partition (ticker)."""
        ticker = 'MSFT'
        # ticker = context.get("ticker") if context else None
        self.logger.info(f"Processing ticker: {ticker} for stream {self.name}")

        df = yq.Ticker(ticker).sec_filings.reset_index(level=0).rename(columns={"symbol": "ticker"})
        df["timestamp_extracted"] = datetime.utcnow()
        df = fix_empty_values(df)
        df.columns = clean_strings(df.columns)
        str_cols = ["ticker", "type", "title", "edgar_url", "exhibits"]
        df[str_cols] = df[str_cols].astype(str)
        for record in df.to_dict(orient="records"):
            yield record