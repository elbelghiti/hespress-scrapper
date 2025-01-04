# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set a working directory (inside the container)
WORKDIR /app

# Install system dependencies (if needed for psycopg2)
RUN apt-get update && apt-get install -y libpq-dev gcc

# Copy requirements.txt into the container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your code into the container
COPY . .

# (Optional) Create a logs folder in the container
RUN mkdir -p logs

# Expose any ports if necessary (not strictly required for a scraper)
# EXPOSE 5000

# Set the entrypoint or default command
# We'll run the Python script directly, but you can override this in docker-compose
CMD ["python", "scraper.py"]
