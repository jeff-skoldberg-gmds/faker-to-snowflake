import os
from faker import Faker
import pandas as pd
import uuid
import random
from datetime import datetime, timedelta
from logging_config import logger
from snowflake_loader import SnowflakeDataLoader


def generate_data(number_of_rows: int) -> pd.DataFrame:
    """
    Generate fake data for testing purposes.

    Args:
        number_of_rows (int): The number of rows of fake data to generate.

    Returns:
        pandas.DataFrame: A DataFrame containing the generated fake data.
    """
    # randmize the number of rows
    number_of_rows += random.randint(-100, 100)
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


def data_to_snowflake(
    df: pd.DataFrame,
    database: str,
    schema: str,
    user: str,
    password: str,
    account: str,
    role: str,
    warehouse: str,
    rsa_key: str,
    table: str = "fake_sales_orders",
) -> None:
    """
    Load data from a pandas DataFrame to Snowflake using SnowpipeLoader or SnowflakeDfLoader.

    Args:
        df (pd.DataFrame): The pandas DataFrame containing the data to be loaded.
        database (str, optional): The name of the Snowflake database. Defaults to the value of the `database` variable.
        schema (str, optional): The name of the Snowflake schema. Defaults to the value of the `schema` variable.
        user (str, optional): The Snowflake user name. Defaults to the value of the `user` variable.
        password (str, optional): The Snowflake password. Defaults to the value of the `password` variable.
        account (str, optional): The Snowflake account name. Defaults to the value of the `account` variable.
        role (str, optional): The Snowflake role name. Defaults to the value of the `role` variable.
        warehouse (str, optional): The Snowflake warehouse name. Defaults to the value of the `warehouse` variable.
        table (str, optional): The name of the Snowflake table to load the data into. Defaults to "fake_sales_orders".

    Returns:
        None

    Raises:
        Exception: If an error occurs during the data loading process.

    Notes:
        This function first tries to load the data using the SnowpipeLoader, which does not require a warehouse.
        If the SnowpipeLoader fails, it falls back to using the SnowflakeDfLoader, which uses a warehouse.
        The function does not check if the table exists because the purpose is to avoid using a warehouse.
        TODO: Instead of the current try-except block, use the "show tables" command to check if the table exists.

    """
    loader = SnowflakeDataLoader(
        df,
        database=database,
        schema=schema,
        user=user,
        password=password,
        account=account,
        role=role,
        table=table,
        rsa_key=rsa_key
    )
    try:
        loader.load_using_snowpipe()
    except Exception as e:
        logger.error(f"Error: {e}")
        try:
            loader.load_using_write_pandas(warehouse=warehouse)
        except Exception as e:
            logger.error(f"Error: {e}")


def main(
    number_of_rows: int,
    database: str,
    schema: str,
    user: str,
    password: str,
    account: str,
    role: str,
    warehouse: str,
    rsa_key: str
) -> None:
    """
    Entry point of the script.

    Args:
        number_of_rows (int): The number of rows to generate in the fake data.
    """
    df = generate_data(number_of_rows)
    # upload_to_snowflake(df)

    try:
        data_to_snowflake(
            df,
            database=database,
            schema=schema,
            user=user,
            password=password,
            account=account,
            role=role,
            warehouse=warehouse,
            table="fake_sales_orders",
            rsa_key=rsa_key
        )
        logger.info("Process complete")
    except Exception as e:
        logger.error(f"Error: {e}")
        logger.error("Process failed")
        raise e


if __name__ == "__main__":
    # for testing locally
    from config import (
        user,
        password,
        account,
        warehouse,
        database,
        schema,
        role,
        region,
        secret_name

    )
    from SecretsManager import SecretsManager

    secrets_manager = SecretsManager(secret_name=secret_name, region=region)
    secret = secrets_manager.get_secret()
    rsa_key = os.getenv('rsa_key')

    main(
        number_of_rows=1000,
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role,
        rsa_key=rsa_key
    )
