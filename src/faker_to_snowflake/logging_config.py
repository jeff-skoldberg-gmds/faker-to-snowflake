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