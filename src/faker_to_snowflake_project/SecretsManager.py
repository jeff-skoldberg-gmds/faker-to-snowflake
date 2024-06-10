import boto3
import os
import json
import logging
import dotenv

logger = logging.getLogger("faker_lambda_logger")

class SecretsManager:
    def __init__(self, secret_name, region):
        logger.info(f"Initializing SecretsManager object for secret: {secret_name}")
        self.secret_name = secret_name
        self.region = region

        
        self.session = boto3.Session()  # relies on environment credentials or IAM roles
        self.client = self.session.client('secretsmanager',region)
        self.set_env_vars()

    def set_env_vars(self):
        """
        Set environment variables from a .env file if it exists.
        This matters locally when creating the secret.

        Returns:
            None
        """
        try:
            dotenv.load_dotenv()
            logger.info("Environment variables loaded from .env file.")
        except Exception as e:
            logger.error(
                f"Could not load .env file. Continuing with existing environment variables. Error: {e}"
            )    
    
    def create_or_update_secret(self):
        """Create or update a secret in AWS Secrets Manager from existing environment variables."""
        logger.info(f"Creating or updating secret: {self.secret_name}")
        secret_data = {
            "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER"),
            "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
            "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
            "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE"),
            "SNOWFLAKE_SCHEMA": os.getenv("SNOWFLAKE_SCHEMA"),
            "SNOWFLAKE_ROLE": os.getenv("SNOWFLAKE_ROLE")
        }
        # Convert dictionary to JSON string
        secret_string = json.dumps(secret_data)

        try:
            # Check if the secret exists
            self.client.describe_secret(SecretId=self.secret_name)
            # If it exists, update the secret
            response = self.client.update_secret(
                SecretId=self.secret_name,
                SecretString=secret_string
            )
            logger.info(f"Updated secret: {self.secret_name}")
        except self.client.exceptions.ResourceNotFoundException:
            # If it doesn't exist, create the secret
            response = self.client.create_secret(
                Name=self.secret_name,
                Description='Snowflake credentials and configuration',
                SecretString=secret_string
            )
            logger.info(f"Created secret: {self.secret_name}")

        return response

    def get_secret(self):
        """Retrieve and set environment variables from a secret in AWS Secrets Manager."""
        # Fetch the secret
        logger.info(f"Retrieving secret: {self.secret_name}")
        response = self.client.get_secret_value(SecretId=self.secret_name)
        secret_dict = json.loads(response['SecretString'])
        # Set environment variables
        for key, value in secret_dict.items():
            os.environ[key] = value
        logger.info(f"Retrieved secret: {self.secret_name}")


if __name__ == "__main__":
    secret_manager = SecretsManager(secret_name="gmdata_snowflake", region="us-east-1")
    secret_manager.create_or_update_secret()