FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy and install dependencies first (layer caching optimization)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code and data
COPY run.py .
COPY config.yaml .
COPY data.csv .
COPY pipeline/ pipeline/

# Run the pipeline with all required CLI arguments
ENTRYPOINT ["python", "run.py", "--input", "data.csv", "--config", "config.yaml", "--output", "metrics.json", "--log-file", "run.log"]
