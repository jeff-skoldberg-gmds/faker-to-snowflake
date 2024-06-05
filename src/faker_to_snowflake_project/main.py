import logging
# log format: timestamp | module name | log level - message
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(module)s | %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

import os
from config import SecretsManager
from fake_data_to_snowflake import main

# I have too many aws profiles and something was conflicting
# You probably don't need to do this:
if os.getenv("WHERE_AM_I") == "LOCAL":
    secret_manager = SecretsManager('gmdata_snowflake', 'snowpipe-tst')
else:
    secret_manager = SecretsManager(secret_name='gmdata_snowflake')

secret_manager.get_secret() # this sets env vars needed

main(2000)