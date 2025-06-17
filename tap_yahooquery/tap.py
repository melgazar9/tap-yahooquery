"""YahooQuery tap class."""

from __future__ import annotations

from singer_sdk import Tap

from tap_yahooquery.client import YahooQueryStream
from tap_yahooquery.streams import SecFilingsStream

STREAMS = [SecFilingsStream]

class TapYahooQuery(Tap):
    """YahooQuery tap class."""

    name = "tap-yahooquery"
    def discover_streams(self) -> list[YahooQueryStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [SecFilingsStream(tap=self)]


if __name__ == "__main__":
    TapYahooQuery.cli()
