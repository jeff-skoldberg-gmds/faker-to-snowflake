# Use an official Python 3.11 runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy only the requirements first to leverage Docker cache
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the specific project directory
COPY src/faker_to_snowflake_project ./faker_to_snowflake_project

# Make port 80 available to the world outside this container
EXPOSE 80

# Define environment variable
ENV NAME World

# Run generator.py when the container launches
CMD ["python", "./faker_to_snowflake_project/generator.py"]
