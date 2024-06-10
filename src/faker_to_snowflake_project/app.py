import logging
# log format: timestamp | module name | log level - message
logger = logging.getLogger("faker_lambda_logger")
logger.setLevel(logging.INFO)

# Create a console handler with a specific log level
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)

# Create a formatter and add it to the handler
formatter = logging.Formatter('%(asctime)s | %(module)s | %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)

logger.info("Beginning app.py")

from SecretsManager import SecretsManager
from config import secret_name, region, profile_name
from fake_data_to_snowflake import main

# I have too many aws profiles and something was conflicting
# You probably don't need to do this:
# if os.getenv("WHERE_AM_I") == "LOCAL":
#     secret_manager = SecretsManager(secret_name=secret_name,region=region, profile_name=profile_name)
# else:
secret_manager = SecretsManager(secret_name=secret_name, region=region)

secret_manager.get_secret() # this sets env vars needed

def lambda_handler(event, context):
    logger.info("Lambda handler started")
    rows = event.get("rows", 2000)
    main(rows)
    logger.info("Lambda handler finished")
    return {
    'statusCode': 200,
    "body": "Records loaded to Snowflake"
    }
