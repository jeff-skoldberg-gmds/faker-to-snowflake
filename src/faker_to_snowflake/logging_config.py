import logging

# log format: timestamp | module name | log level - message
logger = logging.getLogger("faker_lambda_logger")
logger.setLevel(logging.INFO)

# Check if the logger already has handlers, if not, add a new StreamHandler
if not logger.handlers:
    # in AWS, they have their own logger.
    # This logger is used locally.
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s | %(module)s | %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
