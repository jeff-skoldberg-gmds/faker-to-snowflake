# Lambda Parent Image
FROM public.ecr.aws/lambda/python:3.11-arm64


# Set the working directory in the container
WORKDIR /usr/src/app

# Copy only the requirements first to leverage Docker cache
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the specific project directory
COPY src/faker_to_snowflake_project/ ${LAMBDA_TASK_ROOT}

# Make port 80 available to the world outside this container
EXPOSE 80

# Run generator.py when the container launches
CMD ["app.lambda_handler"]
