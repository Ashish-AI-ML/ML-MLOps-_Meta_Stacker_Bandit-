# MLOps Batch Pipeline — Rolling Mean Signal Generator

A production-grade batch pipeline that ingests OHLCV market data, computes a rolling mean feature, generates binary trading signals, and outputs structured metrics — all config-driven, fully logged, and Dockerized for single-command execution.

---

## Project Structure

```
├── run.py               # CLI entrypoint & orchestrator
├── config.yaml          # Pipeline config (seed, window, version)
├── data.csv             # 10,000-row OHLCV dataset
├── requirements.txt     # Pinned Python dependencies
├── Dockerfile           # Container definition
├── README.md            # This file
└── pipeline/
    ├── __init__.py      # Package initializer
    ├── logger.py        # Structured logging (file + console)
    ├── config.py        # YAML config loading & validation
    ├── data.py          # CSV ingestion & validation
    ├── features.py      # Rolling mean computation
    ├── signals.py       # Binary signal generation
    └── metrics.py       # Metrics assembly
```

---

## Local Run Instructions

### Prerequisites

- Python 3.9+
- pip

### Step 1 — Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 2 — Run the Pipeline

```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

| Flag         | Purpose                            |
|--------------|------------------------------------|
| `--input`    | Path to input CSV data file        |
| `--config`   | Path to YAML configuration file    |
| `--output`   | Path to output metrics JSON file   |
| `--log-file` | Path to output log file            |

All four flags are **required** — no hardcoded paths.

---

## Docker Build / Run Commands

### Build the Image

```bash
docker build -t mlops-task .
```

### Run the Pipeline

```bash
docker run --rm mlops-task
```

The container runs the full pipeline and prints the metrics JSON to **stdout**. Since `--rm` destroys the container after exit, stdout is the primary output channel.

---

## Example `metrics.json`

### Success Output

```json
{
  "status": "success",
  "rows_processed": 10000,
  "signal_rate": 0.4991,
  "latency_ms": 197,
  "version": "v1",
  "seed": 42
}
```

| Field             | Description                                              |
|-------------------|----------------------------------------------------------|
| `status`          | `"success"` or `"error"`                                 |
| `rows_processed`  | Total rows in the input CSV                              |
| `signal_rate`     | Mean of signal column over valid (non-NaN) rows          |
| `latency_ms`      | Pipeline execution time in milliseconds                  |
| `version`         | Pipeline version from `config.yaml`                      |
| `seed`            | Random seed from `config.yaml`                           |

### Error Output

```json
{
  "status": "error",
  "error_message": "Input file not found: missing.csv"
}
```

The pipeline writes `metrics.json` in **both success and error cases** — it never crashes without output. All errors produce a structured JSON with `status: "error"` and a descriptive `error_message`, followed by a non-zero exit code.

---

## Pipeline Flow

```
CLI args → Config load → Seed set → Data ingest → Validate →
Rolling mean → Signal generation → Metrics → JSON output
```

1. **Config** — Loads `config.yaml` (seed, window, version) with schema validation
2. **Seed** — Sets `numpy.random.seed` immediately for reproducibility
3. **Data** — Ingests CSV with defensive checks (file exists, non-empty, column validation)
4. **Features** — Computes rolling mean on `close` with configurable window
5. **Signals** — Generates binary signal (`1` if close > rolling_mean, else `0`)
6. **Metrics** — Computes `rows_processed`, `signal_rate`, `latency_ms`
7. **Output** — Writes `metrics.json` and prints to stdout

---

## Configuration

`config.yaml`:

```yaml
seed: 42       # NumPy random seed for reproducibility
window: 5      # Rolling mean window size
version: "v1"  # Pipeline version tag
```

---

## Logging

All pipeline events are written to the log file (`run.log`) with timestamps and severity levels:

```
2026-03-14 13:31:19 | INFO     | Pipeline started | input=data.csv | config=config.yaml
2026-03-14 13:31:19 | INFO     | Config loaded | seed=42 | window=5 | version=v1
2026-03-14 13:31:19 | INFO     | Random seed set: 42
2026-03-14 13:31:19 | INFO     | Dataset loaded | rows=10000 | path=data.csv
2026-03-14 13:31:19 | INFO     | Dataset validated | columns confirmed | close dtype=float64
2026-03-14 13:31:19 | INFO     | Rolling mean computed | window=5 | NaN rows excluded: 4
2026-03-14 13:31:19 | INFO     | Signals generated | valid rows: 9996 | signal=1 count: 4989
2026-03-14 13:31:19 | INFO     | Metrics assembled | rows_processed=10000 | signal_rate=0.4991
2026-03-14 13:31:19 | INFO     | Output written | path=metrics.json
2026-03-14 13:31:19 | INFO     | Job completed successfully | status=success | duration=51ms
```

---

## Reproducibility

- **Seeded RNG** — `numpy.random.seed` set from config before any computation
- **Pinned deps** — Exact versions in `requirements.txt` (`pandas==2.0.3`, `numpy==1.24.3`, `PyYAML==6.0.1`)
- **Config-driven** — All parameters in `config.yaml`, no hardcoded values
- **Deterministic ops** — Rolling mean and threshold comparison are fully deterministic
- **Audit trail** — `metrics.json` includes `seed` and `version` for run traceability
