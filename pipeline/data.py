"""
Data ingestion and validation for the MLOps batch pipeline.
Handles CSV loading with defensive checks for all failure modes.
"""

import os
import pandas as pd


def load_data(path: str) -> pd.DataFrame:
    """
    Load and validate the input CSV dataset.

    Validates: file existence, non-empty content, presence of 'close' column,
    and that 'close' is numeric.

    Args:
        path: Path to the input CSV file.

    Returns:
        Validated pandas DataFrame.

    Raises:
        FileNotFoundError: If the input file does not exist.
        ValueError: If the file is empty, malformed, or missing required columns.
    """
    # Check file exists
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input file not found: {path}")

    # Attempt to read CSV
    try:
        df = pd.read_csv(path)
    except pd.errors.ParserError as e:
        raise ValueError(f"Failed to parse CSV: {e}")
    except Exception as e:
        raise ValueError(f"Error reading input file: {e}")

    # Check non-empty
    if len(df) == 0:
        raise ValueError(f"Dataset is empty: 0 rows loaded from {path}")

    # Check required column
    if "close" not in df.columns:
        raise ValueError(
            f"Required column 'close' not found in dataset. "
            f"Available columns: {list(df.columns)}"
        )

    # Check numeric dtype
    if not pd.api.types.is_numeric_dtype(df["close"]):
        raise ValueError(
            f"Column 'close' must be numeric, got dtype: {df['close'].dtype}"
        )

    return df
