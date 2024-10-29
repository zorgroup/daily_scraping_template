
# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements and the script
COPY requirements.txt ./
COPY daily_scraping.py ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Command to run the Python script with environment variables loaded
CMD ["python", "daily_scraping.py"]
