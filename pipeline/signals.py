"""
Signal generation for the MLOps batch pipeline.
Produces binary trading signals based on close price vs rolling mean.
"""

import pandas as pd


def generate_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate binary signals: 1 if close > rolling_mean, else 0.

    Rows where rolling_mean is NaN (boundary rows from insufficient window data)
    receive signal = 0 but are excluded from signal_rate computation downstream.

    Args:
        df: DataFrame with 'close' and 'rolling_mean' columns.

    Returns:
        DataFrame with an added 'signal' column.
    """
    df = df.copy()
    df["signal"] = 0

    # Only assign signal=1 where rolling_mean is valid and close exceeds it
    valid_mask = df["rolling_mean"].notna()
    df.loc[valid_mask & (df["close"] > df["rolling_mean"]), "signal"] = 1

    return df
