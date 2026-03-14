"""
Feature engineering for the MLOps batch pipeline.
Computes rolling mean on the close price column.
"""

import pandas as pd


def compute_rolling_mean(df: pd.DataFrame, window: int) -> pd.DataFrame:
    """
    Compute the rolling mean of the 'close' column.

    The first (window - 1) rows will have NaN values for the rolling_mean
    column, as there are insufficient data points to compute the full window
    average. These rows are explicitly handled downstream in signal generation.

    Args:
        df: DataFrame with a 'close' column.
        window: Rolling window size (positive integer).

    Returns:
        DataFrame with an added 'rolling_mean' column.
    """
    df = df.copy()
    df["rolling_mean"] = df["close"].rolling(window=window, min_periods=window).mean()
    return df
