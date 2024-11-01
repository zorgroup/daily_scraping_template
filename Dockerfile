# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements and the script files to the container
COPY requirements.txt /app/
COPY daily_scraping_module.py /app/
COPY daily_scraping_test.py /app/
COPY daily_scraping_fetch_and_parse.py /app/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Default command to run the scraper module script
CMD ["python", "/app/daily_scraping_module.py"]
