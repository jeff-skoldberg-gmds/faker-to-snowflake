import os
from faker import Faker
import pandas as pd
import uuid
import random
from datetime import datetime, timedelta
from logging_config import logger
from snowflake_loader import SnowpipeLoader, SnowflakeDfLoader
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

def data_to_snowflake(df,
        database=database,
        schema=schema,
        user=user,
        password=password,
        account=account,
        role=role,
        warehouse=warehouse,
        table="fake_sales_orders",):
    '''
    try using the SnowpipeLoader. (No warehouse required)
    If it fails, the most likely reason is the talbe doesn't exist.
    Upon failure, use the SnowflakeDfLoader instead (uses a warehouse)
    Note: we do not check if the table exists because the whole point is to not use a warehouse
    TODO: Instead of this cheezy try except, use "show tables" and decide based on that. 
    '''
    try:
        loader = SnowpipeLoader(
            df,
            database=database,
            schema=schema,
            user=user,
            password=password,
            account=account,
            role=role,
            table=table,
        )
        loader.load_data()
    except Exception as e:
        logger.error(f"Error: {e}")
        try:
            loader = SnowflakeDfLoader(
                database=database,
                schema=schema,
                user=user,
                password=password,
                account=account,
                role=role,
                table=table,
                warehouse=warehouse,
                df=df,
            )
            loader.upload_to_snowflake()
        except Exception as e:
            logger.error(f"Error: {e}")


def main(number_of_rows: int = 1000) -> None:
    """
    Entry point of the script.

    Args:
        number_of_rows (int): The number of rows to generate in the fake data.
    """
    df = generate_data(number_of_rows)
    # upload_to_snowflake(df)
    
    try:
        data_to_snowflake(df,
                        database=database,
                        schema=schema,
                        user=user,
                        password=password,
                        account=account,
                        role=role,
                        warehouse=warehouse,
                        table="fake_sales_orders",)
        logger.info("Process complete")
    except Exception as e:
        logger.error(f"Error: {e}")
        logger.error("Process failed")


if __name__ == "__main__":
    main(number_of_rows=1000)
