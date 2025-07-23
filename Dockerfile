FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies for Mac compatibility
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better Docker caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create data directory with proper permissions
RUN mkdir -p /app/data && chmod 755 /app/data

# Create dagster home directory
RUN mkdir -p /app/.dagster

# Expose Dagster ports
EXPOSE 3000 3001

# Default command
CMD ["dagster", "dev", "--host", "0.0.0.0", "--port", "3000", "--module-name", "dagster_project"]