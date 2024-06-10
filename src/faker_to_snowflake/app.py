from logging_config import logger

logger.info("Beginning app.py")

from SecretsManager import SecretsManager
from config import secret_name, region
from fake_data_to_snowflake import main

# Initialize the SecretsManager object
secret_manager = SecretsManager(secret_name=secret_name, region=region)

# set system environment variables from Secrets Manager
secret_manager.get_secret()

def lambda_handler(event, context):
    logger.info("Lambda handler started")
    rows = event.get("rows", 2000)
    main(rows)
    logger.info("Lambda handler finished")
    return {
    'statusCode': 200,
    "body": f"{rows} Records loaded to Snowflake"
    }
