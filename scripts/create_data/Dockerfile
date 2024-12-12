# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy all files to the container
COPY . .

# Check if the .env file exists

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port 80 (optional)
EXPOSE 80

# Run main.py when the container launches
CMD ["python", "main.py"]