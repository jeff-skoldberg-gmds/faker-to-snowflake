# Lambda Parent Image
FROM public.ecr.aws/lambda/python:3.11-arm64


# Set the working directory in the container
WORKDIR /usr/src/app

# Define arguments for build-time variables
ARG SECRET_NAME
ARG REGION

# Set environment variables
ENV SECRET_NAME=$SECRET_NAME
ENV REGION=$REGION

# Copy only the requirements first to leverage Docker cache
COPY requirements.txt ./
RUN pip install -r requirements.txt

# Copy the specific project directory
COPY src/faker_to_snowflake/ ${LAMBDA_TASK_ROOT}

# Make port 80 available to the world outside this container
EXPOSE 80

# Run generator.py when the container launches
CMD ["app.lambda_handler"]
