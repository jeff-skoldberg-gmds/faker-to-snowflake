import os
from faker import Faker
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import uuid
import random
from datetime import datetime, timedelta
from snowflake.connector import connect
import dotenv
import logging


# log format: timestamp | module name | log level - message
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(module)s | %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)




def set_env_vars():
    """
    Set environment variables from a .env file if it exists.

    Returns:
        None
    """
    try:
        dotenv.load_dotenv()
    except Exception as e:
        logger.error(
            f"Could not load .env file. Continuing with existing environment variables. Error: {e}"
        )


def generate_data(number_of_rows: int) -> pd.DataFrame:
    """
    Generate fake data for testing purposes.

    Args:
        number_of_rows (int): The number of rows of fake data to generate.

    Returns:
        pandas.DataFrame: A DataFrame containing the generated fake data.
    """
    logger.info(f"Generating {number_of_rows} rows of fake data")
    fake = Faker()
    now = datetime.now()
    data = {
        "name": [fake.name() for _ in range(number_of_rows)],
        "email": [fake.email() for _ in range(number_of_rows)],
        "address": [fake.address() for _ in range(number_of_rows)],
        "ordered_at_utc": [
            now - timedelta(minutes=random.randint(0, 120)) for _ in range(number_of_rows)
        ],
        "extracted_at_utc": [now for _ in range(number_of_rows)],
        "sales_order_id": [
            str(uuid.uuid4()) for _ in range(number_of_rows)
        ],  # Using UUID4 for unique identifiers
    }
    # make sales_order_id the left most column
    df = pd.DataFrame(data)
    logger.info(f"Generated {number_of_rows} rows of fake data")
    return df


def upload_to_snowflake(df):
    """
    Uploads the given DataFrame to Snowflake.

    Args:
        df (pandas.DataFrame): The DataFrame to be uploaded.

    Returns:
        None
    """
    # Load the environment variables from dotenv file if it exists
    logger.info("Uploading data to Snowflake")

    conn = connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )
    # Use the write_pandas method for efficient data upload
    write_pandas(conn, df, "fake_sales_orders",auto_create_table=True, quote_identifiers=False)
    conn.close()
    logger.info("Data uploaded to Snowflake")


def main(number_of_rows: int = 1000) -> None:
    """
    Entry point of the script.

    Args:
        number_of_rows (int): The number of rows to generate in the fake data.
    """
    set_env_vars()
    df = generate_data(number_of_rows)
    upload_to_snowflake(df)


if __name__ == "__main__":
    main()
