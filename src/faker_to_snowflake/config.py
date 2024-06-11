import os
import dotenv

dotenv.load_dotenv()

secret_name = os.getenv("SECRET_NAME")
region = os.getenv("REGION")

user = os.getenv("SNOWFLAKE_USER")
password = os.getenv("SNOWFLAKE_PASSWORD")
account = os.getenv("SNOWFLAKE_ACCOUNT")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
database = os.getenv("SNOWFLAKE_DATABASE")
schema = os.getenv("SNOWFLAKE_SCHEMA")
role = os.getenv("SNOWFLAKE_ROLE")
