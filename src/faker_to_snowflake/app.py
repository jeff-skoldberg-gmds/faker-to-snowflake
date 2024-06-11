import importlib
from logging_config import logger

logger.info("Beginning app.py")

from SecretsManager import SecretsManager
import config
from fake_data_to_snowflake import main

# Initialize the SecretsManager object and set the environment variables
secret_manager = SecretsManager(secret_name=config.secret_name, region=config.region)
secret_manager.get_secret() # sets variables
#after the env vars are set, to re-import the config.
importlib.reload(config)
# todo: ^ make config a class that handles this.


def lambda_handler(event, context):
    logger.info("Lambda handler started")
    rows = event.get("rows", 2000)
    main(rows,
        user=config.user,
        password=config.password,
        account=config.account,
        warehouse=config.warehouse,
        database=config.database,
        schema=config.schema,
        role=config.role,
        rsa_key=config.rsa_key)
    logger.info("Lambda handler finished")
    return {"statusCode": 200, "body": f"{rows} Records loaded to Snowflake"}
