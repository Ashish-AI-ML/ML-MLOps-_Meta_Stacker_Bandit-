"""
Metrics computation for the MLOps batch pipeline.
Assembles the output metrics dictionary with all required fields.
"""

import pandas as pd


def compute_metrics(
    df: pd.DataFrame,
    latency_ms: int,
    seed: int,
    version: str,
) -> dict:
    """
    Compute pipeline output metrics.

    Args:
        df: DataFrame with 'signal' and 'rolling_mean' columns.
        latency_ms: Pipeline execution time in milliseconds.
        seed: Random seed used for the run.
        version: Pipeline version string.

    Returns:
        Dictionary with the complete metrics schema.
    """
    rows_processed = len(df)

    # signal_rate computed only over rows where rolling_mean is valid
    valid_signals = df.loc[df["rolling_mean"].notna(), "signal"]
    signal_rate = round(float(valid_signals.mean()), 4) if len(valid_signals) > 0 else 0.0

    return {
        "status": "success",
        "rows_processed": rows_processed,
        "signal_rate": signal_rate,
        "latency_ms": latency_ms,
        "version": version,
        "seed": seed,
    }
