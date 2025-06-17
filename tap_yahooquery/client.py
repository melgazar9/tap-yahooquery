"""REST client handling, including YahooQueryStream base class."""

from __future__ import annotations

from abc import ABC

import typing as t
from singer_sdk.streams import Stream

from singer_sdk.helpers.types import Context


class YahooQueryStream(Stream, ABC):
    """YahooQuery stream class."""

    def get_records(self, state: dict[str, t.Any], context: Context) -> list[dict[str, t.Any]]:
        x = 1
        pass