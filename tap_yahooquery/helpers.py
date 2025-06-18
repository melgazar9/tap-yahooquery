import pandas as pd
import numpy as np
import re
import hashlib
from uuid import uuid4
from datetime import datetime, timedelta
from pytickersymbols import PyTickerSymbols
from requests_html import HTMLSession
import threading
import logging
import backoff
import functools
import time

pd.set_option("future.no_silent_downcasting", True)


class EmptyDataRetryException(Exception):
    """Raised when data is unexpectedly empty and should be retried."""

    pass


def create_yahoo_retry_decorator(max_tries=5, max_time=120, base_delay=0.003):
    """Create a Yahoo API retry decorator using backoff library."""

    def decorator(func):
        @functools.wraps(func)
        def wrapped_func(*args, **kwargs):
            time.sleep(base_delay)

            # Extract ticker context for better logging
            ticker = None
            if args and hasattr(args[0], '__class__'):
                # If called as a method, args[0] is self
                if len(args) > 1:
                    ticker = args[1]
            elif args:
                ticker = args[0]

            try:
                result = func(*args, **kwargs)

                # Check if result is an empty DataFrame - raise exception to trigger retry
                if isinstance(result, pd.DataFrame) and result.empty:
                    raise EmptyDataRetryException(
                        f"Empty DataFrame returned from {func.__name__} for ticker: {ticker}"
                    )
                return result
            except EmptyDataRetryException:
                # Re-raise to trigger backoff
                raise
            except Exception as e:
                # Handle other exceptions with context
                raise Exception(f"Error in {func.__name__} for ticker {ticker}: {e}") from e

        def backoff_handler(details):
            # Extract ticker from exception message if available
            exception_str = str(details['exception'])
            ticker_match = re.search(r'ticker: (\w+)', exception_str)
            ticker_info = f" [{ticker_match.group(1)}]" if ticker_match else ""

            logging.info(
                f"🔄 Retrying {func.__name__}{ticker_info} - "  # ✅ Use original func.__name__
                f"attempt {details['tries']}/{max_tries}, waiting {details['wait']:.1f}s"
            )

        def giveup_handler(details):
            # Extract ticker from exception message if available
            exception_str = str(details['exception'])
            ticker_match = re.search(r'ticker: (\w+)', exception_str)
            ticker_info = f" [{ticker_match.group(1)}]" if ticker_match else ""

            logging.warning(
                f"⚠️ Giving up on {func.__name__}{ticker_info} after {details['tries']} attempts - "  # ✅ Use original func.__name__
                f"continuing with empty result"
            )

        # ✅ Create wrapper that CATCHES the final exception
        @functools.wraps(func)  # ✅ Preserve function name here too
        def safe_wrapper(*args, **kwargs):
            try:
                return backoff.on_exception(
                    backoff.expo,
                    (EmptyDataRetryException,),
                    max_tries=max_tries,
                    max_time=max_time,
                    base=2,
                    max_value=30,
                    jitter=backoff.random_jitter,
                    on_backoff=backoff_handler,
                    on_giveup=giveup_handler,
                )(wrapped_func)(*args, **kwargs)
            except EmptyDataRetryException:
                # ✅ After all retries failed, return empty DataFrame instead of crashing
                logging.warning(f"🔄 All retries exhausted for {func.__name__} - returning empty DataFrame")
                return pd.DataFrame()
        return safe_wrapper

    return decorator

yahoo_api_retry = create_yahoo_retry_decorator()


def clean_strings(lst):
    cleaned_list = [
        re.sub(r"[^a-zA-Z0-9_]", "_", s) for s in lst
    ]  # remove special characters
    cleaned_list = [
        re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower() for s in cleaned_list
    ]  # camel case -> snake case
    cleaned_list = [
        re.sub(r"_+", "_", s).strip("_").lower() for s in cleaned_list
    ]  # clean leading and trailing underscores
    return cleaned_list


def fix_empty_values(df, exclude_columns=None, to_value=None):
    """
    Replaces np.nan, inf, -inf, None, and string versions of 'nan', 'none', 'infinity'
    recursively with a specified value (default None), except for columns listed in exclude_columns.

    Args:
        df (pd.DataFrame): The input DataFrame.
        exclude_columns (list, optional): Columns to skip. Defaults to None.
        to_value: What to replace missing values with (None or np.nan). Defaults to None.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    if exclude_columns is None:
        exclude_columns = []
    if to_value is None:
        to_value = None

    regex_pattern = r"(?i)^(nan|none|infinity)$"

    def clean_obj(val):
        if isinstance(val, dict):
            return {k: clean_obj(v) for k, v in val.items()}
        if isinstance(val, list):
            return [clean_obj(x) for x in val]
        if isinstance(val, str) and re.match(regex_pattern, val):
            return to_value
        if val in [None, np.nan] or (isinstance(val, float) and not np.isfinite(val)):
            return to_value
        return val

    def replace_col(col):
        if col.name in exclude_columns:
            return col
        if pd.api.types.is_numeric_dtype(col) or pd.api.types.is_datetime64_any_dtype(
            col
        ):
            return col.replace([np.nan, np.inf, -np.inf, None], to_value)

        uuid = str(uuid4())
        return (
            col.replace([np.nan, np.inf, -np.inf, None], to_value)
            .replace(regex_pattern, to_value, regex=True)
            .apply(clean_obj)
            .fillna(uuid)
            .replace(uuid, to_value)
        )

    return df.apply(replace_col)


def flatten_list(lst):
    return [v for item in lst for v in (item if isinstance(item, list) else [item])]


class TickerFetcher:
    """
    Fetch and caches Yahoo tickers in memory for the duration of a Meltano tap run.
    ENSURES no duplicates and stops pagination when tickers repeat.
    """

    _memory_cache = {}
    _cache_lock = threading.Lock()

    def fetch_all_tickers(self) -> list[dict]:
        """
        Fetch all tickers from both Yahoo segments and PyTickerSymbols.
        Returns a list of ticker dictionaries - matches tap-yfinance behavior.
        """
        all_dfs = []

        yahoo_segments = [
            "stock_tickers",
            "crypto_tickers",
            "forex_tickers",
            "etf_tickers",
            "mutual_fund_tickers",
            "world_indices_tickers",
            "futures_tickers",
            "bonds_tickers",
            "options_tickers",
            "private_companies_tickers",
        ]

        for segment in yahoo_segments:
            logging.info(f"Pulling {segment} tickers for all_tickers stream.")
            try:
                df = self.fetch_yahoo_tickers(segment)
                if "segment" not in df.columns:
                    df["segment"] = segment
                df = df[["ticker", "name", "segment"]].drop_duplicates()

                df = fix_empty_values(df, exclude_columns=["ticker"])
                df["ticker"] = df["ticker"].astype(str)
                all_dfs.append(df)
            except Exception as e:
                logging.warning(f"Could not download {segment}: {e}")
                continue

        logging.info("Pulling pts_tickers for all_tickers stream.")
        try:
            df = self.fetch_pts_tickers()
            df["segment"] = "pts_tickers"

            if "ticker" not in df.columns and "yahoo_ticker" in df.columns:
                df = df.rename(columns={"yahoo_ticker": "ticker"})

            df = df[["ticker", "name", "segment"]].drop_duplicates(
                subset=["ticker", "segment"]
            )
            df = fix_empty_values(df, exclude_columns=["ticker"])
            df["ticker"] = df["ticker"].astype(str)
            all_dfs.append(df)
        except Exception as e:
            logging.warning(f"Could not download pts_tickers: {e}")

        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=["ticker"])
            return combined_df.to_dict("records")
        else:
            logging.warning("No tickers were successfully fetched from any source")
            return []

    def fetch_specific_tickers(self, ticker_list: list[str]) -> list[dict]:
        """
        Create ticker records for a specific list of tickers.
        """
        return [
            {
                "ticker": ticker.upper(),
                "name": None,
                "segment": self._guess_segment(ticker),
            }
            for ticker in ticker_list
        ]

    @staticmethod
    def _guess_segment(ticker: str) -> str:
        """Guess the segment for a ticker based on its format."""
        if isinstance(ticker, str):
            if ticker.endswith("=X"):
                return "forex_tickers"
            elif "-" in ticker and ticker.endswith("USD"):
                return "crypto_tickers"
            elif ticker.isupper() and len(ticker) <= 5:
                return "stock_tickers"
            elif ".PVT" in ticker:
                return "private_companies_tickers"
            elif ticker.startswith("^"):
                return "world_indices_tickers"
        return "unknown"

    @classmethod
    def fetch_yahoo_tickers(cls, segment, paginate_records=200, max_pages=50000):
        url_map = {
            "world_indices_tickers": "https://finance.yahoo.com/markets/world-indices/",
            "futures_tickers": "https://finance.yahoo.com/markets/commodities/",
            "bonds_tickers": "https://finance.yahoo.com/markets/bonds/",
            "forex_tickers": "https://finance.yahoo.com/markets/currencies/",
            "options_tickers": "https://finance.yahoo.com/markets/options/most-active/?start={start}&count={count}",
            "stock_tickers": "https://finance.yahoo.com/markets/stocks/most-active/?start={start}&count={count}",
            "crypto_tickers": "https://finance.yahoo.com/markets/crypto/all/?start={start}&count={count}",
            "private_companies_tickers": "https://finance.yahoo.com/markets/private-companies/highest-valuation/",
            "etf_tickers": "https://finance.yahoo.com/markets/etfs/most-active/",
            "mutual_fund_tickers": "https://finance.yahoo.com/markets/mutualfunds/gainers/?start={start}&count={count}",
        }
        default_url_map = {
            "world_indices_tickers": "https://finance.yahoo.com/markets/world-indices/",
            "futures_tickers": "https://finance.yahoo.com/markets/commodities/",
            "bonds_tickers": "https://finance.yahoo.com/markets/bonds/",
            "forex_tickers": "https://finance.yahoo.com/markets/currencies/",
            "options_tickers": "https://finance.yahoo.com/markets/options/most-active/",
            "stock_tickers": "https://finance.yahoo.com/markets/stocks/most-active/",
            "crypto_tickers": "https://finance.yahoo.com/markets/crypto/all/",
            "private_companies_tickers": "https://finance.yahoo.com/markets/private-companies/highest-valuation/",
            "etf_tickers": "https://finance.yahoo.com/markets/etfs/most-active/?start={start}&count={count}",
            "mutual_fund_tickers": "https://finance.yahoo.com/markets/mutualfunds/",
        }

        if segment not in url_map or segment not in default_url_map:
            raise Exception(f"Unknown segment: {segment}")

        with cls._cache_lock:
            if segment in cls._memory_cache:
                return cls._memory_cache[segment]

        base_url = url_map[segment]
        first_url = default_url_map[segment]
        paginate = "{start}" in base_url
        key_columns = ["symbol", "name"]

        session = HTMLSession()
        session.timeout = 60

        if not paginate:
            resp = session.get(first_url)
            tables = pd.read_html(resp.html.raw_html)
            if not tables:
                session.close()
                raise Exception(f"No tables found for {segment}")
            df = tables[0]
            df.columns = [str(x).strip().lower() for x in df.columns]
            if segment == "private_companies_tickers":
                df = df.rename(columns={"company": "name"})
            if not all(col in df.columns for col in key_columns):
                session.close()
                raise Exception(
                    f"Expected columns {key_columns} not found for tickers {segment}"
                )
            df = df[key_columns].rename(columns={"symbol": "ticker"})
            df = df.drop_duplicates(subset=["ticker"])
            df = df.reset_index(drop=True)

            mask_nan_both = df["ticker"].isna() & df["name"].isna()
            if mask_nan_both.any():
                df.loc[mask_nan_both, :] = fix_empty_values(df.loc[mask_nan_both, :])
            df = df.dropna(how="all", axis=1)
            df = df.dropna(how="all", axis=0)
            session.close()
            df[["ticker", "name"]] = df[["ticker", "name"]].astype(str)
            with cls._cache_lock:
                cls._memory_cache[segment] = df
            return df

        # --- PAGINATION ---
        all_dfs = []
        seen_tickers = set()
        start = 0
        page = 0
        while page < max_pages:
            url = base_url.format(start=start, count=paginate_records)
            resp = session.get(url)
            tables = pd.read_html(resp.html.raw_html)
            if not tables:
                break
            df = tables[0]
            df.columns = [str(x).strip().lower() for x in df.columns]
            if not all(col in df.columns for col in key_columns):
                break
            df = df[key_columns]
            # Filter out tickers already seen
            df = df[~df["symbol"].astype(str).isin(seen_tickers)]
            if df.empty:
                break
            all_dfs.append(df)
            # Add the tickers from this page to seen_tickers
            seen_tickers.update(df["symbol"].astype(str))
            # If the number of unique tickers on this page is less than paginate_records, stop (last page)
            if len(df) < paginate_records:
                break
            start += paginate_records
            page += 1
        session.close()
        if not all_dfs:
            raise Exception(f"No data found for segment: {segment}")

        df_final = pd.concat(all_dfs, ignore_index=True)
        df_final = df_final.drop_duplicates(subset=["symbol"])
        df_final = df_final.rename(columns={"symbol": "ticker"})
        df_final = df_final.reset_index(drop=True)
        mask_nan_both = df_final["ticker"].isna() & df_final["name"].isna()
        if mask_nan_both.any():
            df_final.loc[mask_nan_both, :] = fix_empty_values(
                df_final.loc[mask_nan_both, :]
            )
        df_final = df_final.dropna(how="all", axis=1)
        df_final = df_final.dropna(how="all", axis=0)
        df_final[["ticker", "name"]] = df_final[["ticker", "name"]].astype(str)
        with cls._cache_lock:
            cls._memory_cache[segment] = df_final
        return df_final

    @staticmethod
    def fetch_pts_tickers():
        """
        Description
        -----------
        Fetch py-ticker-symbols tickers
        """
        logging.info("Pulling PTS tickers.")
        pts = PyTickerSymbols()
        all_getters = list(
            filter(
                lambda x: (
                    x.endswith("_yahoo_tickers") or x.endswith("_google_tickers")
                ),
                dir(pts),
            )
        )

        all_tickers = {"yahoo_tickers": [], "google_tickers": []}
        for t in all_getters:
            if t.endswith("google_tickers"):
                all_tickers["google_tickers"].append((getattr(pts, t)()))
            elif t.endswith("yahoo_tickers"):
                all_tickers["yahoo_tickers"].append((getattr(pts, t)()))
        all_tickers["google_tickers"] = flatten_list(all_tickers["google_tickers"])
        all_tickers["yahoo_tickers"] = flatten_list(all_tickers["yahoo_tickers"])
        if len(all_tickers["yahoo_tickers"]) == len(all_tickers["google_tickers"]):
            all_tickers = pd.DataFrame(all_tickers)
        else:
            all_tickers = pd.DataFrame(
                dict([(k, pd.Series(v)) for k, v in all_tickers.items()])
            )

        all_tickers = (
            all_tickers.rename(
                columns={
                    "yahoo_tickers": "yahoo_ticker",
                    "google_tickers": "google_ticker",
                }
            )
            .sort_values(by=["yahoo_ticker", "google_ticker"])
            .drop_duplicates()
        )
        all_tickers = fix_empty_values(all_tickers)
        all_tickers = all_tickers.replace([-np.inf, np.inf, np.nan], None)
        all_tickers.columns = ["yahoo_ticker", "google_ticker"]

        all_stocks = pts.get_all_stocks()
        df_all_stocks = pd.json_normalize(
            all_stocks,
            record_path=["symbols"],
            meta=[
                "name",
                "symbol",
                "country",
                "indices",
                "industries",
                "isins",
                "akas",
                ["metadata", "founded"],
                ["metadata", "employees"],
            ],
            errors="ignore",
        )
        df_all_stocks = df_all_stocks.rename(
            columns={"metadata.founded": "founded", "metadata.employees": "employees"}
        ).rename(columns={"yahoo": "yahoo_ticker", "google": "google_ticker"})
        df_all_stocks["segment"] = "stocks"

        all_indices = pts.get_all_indices()
        df_all_indices = pd.DataFrame({"ticker": all_indices, "name": None})
        df_all_indices["segment"] = "indices"

        industries = pts.get_all_industries()
        df_all_industries = pd.DataFrame({"ticker": None, "name": industries})
        df_all_industries["segment"] = "industries"

        countries = pts.get_all_countries()
        df_countries = pd.DataFrame({"ticker": None, "name": countries})
        df_countries["segment"] = "countries"

        df_final = pd.concat(
            [
                all_tickers,
                df_all_stocks,
                df_all_indices,
                df_all_industries,
                df_countries,
            ],
            ignore_index=True,
        )

        df_final["ticker"] = (
            df_final["ticker"]
            .fillna(df_final["yahoo_ticker"])
            .fillna(df_final["google_ticker"])
        )
        df_final = df_final.dropna(how="all", axis=1)
        df_final = df_final.dropna(how="all", axis=0)
        df_final = fix_empty_values(df_final)
        list_cols = ["indices", "industries", "isins", "akas"]
        for col in list_cols:
            if col in df_final.columns:
                df_final[col] = df_final[col].apply(
                    lambda x: tuple(x) if isinstance(x, list) else x
                )

        df_final["surrogate_key"] = df_final.apply(
            lambda x: hashlib.sha256(
                "".join(str(x) for x in x.values).encode("utf-8")
            ).hexdigest(),
            axis=1,
        )

        df_final[["employees", "founded"]] = df_final[["employees", "founded"]].astype(
            str
        )  # ensure no issues with singer schema
        return df_final


def get_valid_yfinance_start_timestamp(interval, start="1950-01-01 00:00:00"):
    """
    Description
    -----------
    Get a valid yfinance date to lookback.
    Valid intervals with maximum lookback period
    1m: 7 days
    2m: 60 days
    5m: 60 days
    15m: 60 days
    30m: 60 days
    60m: 730 days
    90m: 60 days
    1h: 730 days
    1d: 50+ years
    5d: 50+ years
    1wk: 50+ years
    1mo: 50+ years --- Buggy!
    3mo: 50+ years --- Buggy!

    Note: Often times yfinance returns an error even when looking back maximum number of days - 1,
        by default, return a date 2 days closer to the current date than the maximum specified in the yfinance docs

    """

    valid_intervals = [
        "1m",
        "2m",
        "5m",
        "15m",
        "30m",
        "60m",
        "1h",
        "90m",
        "1d",
        "5d",
        "1wk",
        "1mo",
        "3mo",
    ]
    assert interval in valid_intervals, f"must pass a valid interval {valid_intervals}"

    if interval == "1m":
        updated_start = max(
            (datetime.today() - timedelta(days=5)).date(), pd.to_datetime(start).date()
        )
    elif interval in ["2m", "5m", "15m", "30m", "90m"]:
        updated_start = max(
            (datetime.today() - timedelta(days=58)).date(), pd.to_datetime(start).date()
        )
    elif interval in ["60m", "1h"]:
        updated_start = max(
            (datetime.today() - timedelta(days=728)).date(),
            pd.to_datetime(start).date(),
        )
    else:
        updated_start = pd.to_datetime(start)

    updated_start = updated_start.strftime(
        "%Y-%m-%d"
    )  # yfinance doesn't like strftime with hours, minutes, or seconds

    return updated_start
