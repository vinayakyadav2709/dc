# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    openmpi-bin \
    libopenmpi-dev \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv

# Copy dependency files
COPY pyproject.toml .
COPY README.md .

# Install dependencies only (cache layer)
RUN uv sync --no-install-project

# Copy project files
COPY src/ ./src/
COPY test_node.py .
COPY test_client.py .

# Install the project
RUN uv pip install -e .

# No default CMD - let docker-compose specify commands