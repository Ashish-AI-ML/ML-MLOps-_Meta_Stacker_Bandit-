"""
MLOps Batch Pipeline — CLI Entrypoint & Orchestrator

Executes a linear batch pipeline: config → seed → data → features → signals → metrics → output.
All stages are validated. Errors produce a structured error metrics.json and exit code 1.
"""

import argparse
import json
import sys
import time
import traceback

import numpy as np

from pipeline.logger import setup_logger
from pipeline.config import load_config
from pipeline.data import load_data
from pipeline.features import compute_rolling_mean
from pipeline.signals import generate_signals
from pipeline.metrics import compute_metrics


def write_output(metrics: dict, output_path: str) -> None:
    """Write metrics dict to JSON file and print to stdout."""
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
    # Print to stdout (required for Docker --rm visibility)
    print(json.dumps(metrics, indent=2))


def write_error(error_message: str, output_path: str) -> None:
    """Write error metrics JSON and print to stdout."""
    error_metrics = {
        "status": "error",
        "error_message": error_message,
    }
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(error_metrics, f, indent=2)
    except Exception:
        pass  # If we can't write the file, still print to stdout
    print(json.dumps(error_metrics, indent=2))


def parse_args() -> argparse.Namespace:
    """Parse and return CLI arguments."""
    parser = argparse.ArgumentParser(
        description="MLOps Batch Pipeline — Rolling Mean Signal Generator"
    )
    parser.add_argument(
        "--input", required=True, help="Path to input CSV data file"
    )
    parser.add_argument(
        "--config", required=True, help="Path to YAML configuration file"
    )
    parser.add_argument(
        "--output", required=True, help="Path to output metrics JSON file"
    )
    parser.add_argument(
        "--log-file", required=True, help="Path to output log file"
    )
    return parser.parse_args()


def main() -> int:
    """
    Main pipeline orchestrator.

    Returns:
        Exit code: 0 on success, 1 on failure.
    """
    args = parse_args()
    start_time = time.perf_counter()
    logger = None

    try:
        # ── Step 01: Initialize logger ──────────────────────────────────
        logger = setup_logger(args.log_file)
        logger.info(
            f"Pipeline started | input={args.input} | config={args.config} | "
            f"output={args.output} | log_file={args.log_file}"
        )

        # ── Step 02-03: Load and validate configuration ─────────────────
        config = load_config(args.config)
        logger.info(
            f"Config loaded | seed={config.seed} | window={config.window} | "
            f"version={config.version}"
        )

        # ── Step 04: Set random seed for reproducibility ────────────────
        np.random.seed(config.seed)
        logger.info(f"Random seed set: {config.seed}")

        # ── Step 05-06: Load and validate dataset ───────────────────────
        df = load_data(args.input)
        logger.info(
            f"Dataset loaded | rows={len(df)} | path={args.input}"
        )
        logger.info(
            f"Dataset validated | columns confirmed | "
            f"close dtype={df['close'].dtype}"
        )

        # ── Step 07: Compute rolling mean ───────────────────────────────
        if config.window >= len(df):
            logger.warning(
                f"Window size ({config.window}) >= dataset rows ({len(df)}). "
                f"All rolling_mean values will be NaN."
            )
        df = compute_rolling_mean(df, config.window)
        nan_count = int(df["rolling_mean"].isna().sum())
        logger.info(
            f"Rolling mean computed | window={config.window} | "
            f"NaN rows excluded: {nan_count}"
        )

        # ── Step 08: Generate signals ───────────────────────────────────
        df = generate_signals(df)
        valid_mask = df["rolling_mean"].notna()
        valid_rows = int(valid_mask.sum())
        signal_1_count = int(df.loc[valid_mask, "signal"].sum())
        logger.info(
            f"Signals generated | valid rows: {valid_rows} | "
            f"signal=1 count: {signal_1_count}"
        )

        # ── Step 09: Compute metrics ────────────────────────────────────
        latency_ms = int((time.perf_counter() - start_time) * 1000)
        metrics = compute_metrics(
            df=df,
            latency_ms=latency_ms,
            seed=config.seed,
            version=config.version,
        )
        logger.info(
            f"Metrics assembled | rows_processed={metrics['rows_processed']} | "
            f"signal_rate={metrics['signal_rate']} | latency_ms={metrics['latency_ms']}"
        )

        # ── Step 10: Write output ───────────────────────────────────────
        write_output(metrics, args.output)
        logger.info(f"Output written | path={args.output}")
        logger.info(
            f"Job completed successfully | status=success | "
            f"duration={latency_ms}ms"
        )
        return 0

    except Exception as e:
        error_message = str(e)
        if logger:
            logger.error(f"Pipeline failed: {error_message}")
            logger.debug(traceback.format_exc())
        write_error(error_message, args.output)
        return 1


if __name__ == "__main__":
    sys.exit(main())
