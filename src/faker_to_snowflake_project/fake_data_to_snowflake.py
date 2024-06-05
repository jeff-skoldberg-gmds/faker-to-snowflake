import os
from faker import Faker
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

# Create the connection using the connect method from snowflake.connector
from snowflake.connector import connect

def generate_data(n):
    fake = Faker()
    data = {'Name': [fake.name() for _ in range(n)],
            'Email': [fake.email() for _ in range(n)],
            'Address': [fake.address() for _ in range(n)]}
    return pd.DataFrame(data)

def upload_to_snowflake(df):
    conn = connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    # Use the write_pandas method for efficient data upload
    write_pandas(conn, df, 'MYTABLE')
    conn.close()

def main():
    df = generate_data(1000)
    upload_to_snowflake(df)

if __name__ == "__main__":
    main()
