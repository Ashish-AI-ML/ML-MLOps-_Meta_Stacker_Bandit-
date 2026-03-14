"""
Structured logging setup for the MLOps batch pipeline.
Configures file + console handlers with timestamped, leveled output.
"""

import logging
import sys


def setup_logger(log_file: str, name: str = "mlops_pipeline") -> logging.Logger:
    """
    Configure and return a structured logger.

    Args:
        log_file: Path to the log output file.
        name: Logger name (default: 'mlops_pipeline').

    Returns:
        Configured logging.Logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Prevent duplicate handlers on repeated calls
    if logger.handlers:
        logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File handler — captures all log levels
    file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler — INFO and above
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
