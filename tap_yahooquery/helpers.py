import pandas as pd
import numpy as np
import re

from uuid import uuid4

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