from datetime import timedelta, timezone, datetime
import jwt
import os
import pandas as pd
import snowflake.connector
import requests
import json
import tempfile
from sql_api_generate_jwt import JWTGenerator
from logging_config import logger


class SnowpipeLoader:
    def __init__(
        self,
        df: pd.DataFrame,
        database: str,
        schema: str,
        user: str,
        password: str,
        account: str,
        role: str,
        table: str,
    ):
        self.df = df
        self.database = database
        self.schema = schema
        self.user = user
        self.password = password
        self.account = account
        self.role = role
        self.table = table
        self.jwt_token = self.generate_jwt_token()

    def generate_jwt_token(self):
        logger.info("Generating JWT token")
        private_key_path = "rsa_key.p8"
        jwt_generator = JWTGenerator(self.account, self.user, private_key_path)
        jwt_token = jwt_generator.get_token()
        if not jwt_token:
            raise ValueError(
                "The JWT token could not be generated. Please check the private key and payload details."
            )
        return jwt_token

    def upload_dataframe_to_stage(self):
        stage_name = f"%{self.table}"
        logger.info(f"Uploading data to Snowflake stage: {stage_name}")
        with tempfile.NamedTemporaryFile(
            suffix=".parquet", delete=False
        ) as temp_parquet:
            self.df.to_parquet(temp_parquet.name, index=False)
            temp_parquet_path = temp_parquet.name

        conn = snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
            schema=self.schema,
            role=self.role,
        )

        try:
            cursor = conn.cursor()
            cursor.execute(f"PUT file://{temp_parquet_path} @{stage_name}")
            logger.info(f"File successfully uploaded to stage: {stage_name}")
        finally:
            cursor.close()
            conn.close()

        file_name = os.path.basename(temp_parquet_path)
        os.remove(temp_parquet_path)
        return file_name

    def trigger_snowpipe(self, file_name: str):
        snowpipe_name = f"pipe_{self.table}".upper()
        url = f"https://{self.account}.snowflakecomputing.com/v1/data/pipes/{self.database}.{self.schema}.{snowpipe_name}/insertFiles"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.jwt_token}",
            "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
        }
        payload = {"files": [{"path": file_name}]}
        logger.info(f"Triggering Snowpipe: {url}")
        logger.info(f"Payload: {payload}")

        response = requests.post(url, headers=headers, data=json.dumps(payload))
        if response.status_code == 200:
            logger.info(f"response: {response.text}")
            logger.info("Data successfully loaded to table using Snowpipe.")
        else:
            logger.info(
                f"Failed to trigger Snowpipe. Status code: {response.status_code}, Response: {response.text}"
            )

    def clean_table_stage(self):
        stage_name = f"%{self.table}"
        logger.info(f"Removing files from stage: {stage_name}")
        conn = snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
            schema=self.schema,
            role=self.role,
        )
        try:
            cursor = conn.cursor()
            cursor.execute(f"REMOVE @{stage_name}")
            logger.info(f"Stage {stage_name} cleaned.")
        finally:
            cursor.close()
            conn.close()

    def load_data(self):
        file_name = self.upload_dataframe_to_stage()
        self.trigger_snowpipe(file_name)
        self.clean_table_stage()


# # Example usage
# df = pd.DataFrame({'column1': [1, 2, 3], 'column2': ['a', 'b', 'c']})
# loader = SnowflakeLoader(df, "your_database", "your_schema", "your_table")
# loader.load_data()
