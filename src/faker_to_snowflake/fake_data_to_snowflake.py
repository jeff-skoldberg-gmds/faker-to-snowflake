import os
from faker import Faker
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import uuid
import random
from datetime import datetime, timedelta
from snowflake.connector import connect
from logging_config import logger
from snowpipe_loader import SnowpipeLoader
from config import (
    user,
    password,
    account,
    warehouse,
    database,
    schema,
    role,
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
            now - timedelta(minutes=random.randint(0, 120))
            for _ in range(number_of_rows)
        ],
        "extracted_at_utc": [now for _ in range(number_of_rows)],
        "sales_order_id": [str(uuid.uuid4()) for _ in range(number_of_rows)],
    }

    df = pd.DataFrame(data)
    logger.info(f"Generated {number_of_rows} rows of fake data")
    return df


def upload_to_snowflake(
    df: pd.DataFrame,
    user: str,
    password: str,
    account: str,
    warehouse: str,
    database: str,
    schema: str,
    role: str,
) -> None:
    """
    Uploads the given DataFrame to Snowflake.
    This is an extremely simple way to load data so Snowflake, but it can be expensive if you run it frequently.
    The copy command utilizes a virtual warehouse, so you pay for a full minute of usage.

    Args:
        df (pandas.DataFrame): The DataFrame to be uploaded.

    Returns:
        None
    """
    # Load the environment variables from dotenv file if it exists
    logger.info("Uploading data to Snowflake")

    conn = connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role,
    )
    # Use the write_pandas method for efficient data upload
    write_pandas(
        conn, df, "fake_sales_orders", auto_create_table=True, quote_identifiers=False
    )
    conn.close()
    logger.info("Data uploaded to Snowflake")


def main(number_of_rows: int = 1000) -> None:
    """
    Entry point of the script.

    Args:
        number_of_rows (int): The number of rows to generate in the fake data.
    """
    df = generate_data(number_of_rows)
    # upload_to_snowflake(df)
    loader = SnowpipeLoader(
        df,
        database=database,
        schema=schema,
        user=user,
        password=password,
        account=account,
        role=role,
        table="fake_sales_orders",
    )
    loader.load_data()
    logger.info("Process complete")


if __name__ == "__main__":
    main(number_of_rows=1000)
