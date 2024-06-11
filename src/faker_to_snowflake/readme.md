# Files in this directory
1. app.py: this is what is run by aws lambda
2. fake_data_to_snowflake: this is what you run if you want to try to run it locally.  But, there is a lot of behind the scenes setup that doesn't come with this repo (yet), so running locally may be a challenge.
3. snowflake_loader.py: utility for loading data to snowflake via snowpipe API or write_pandas
4. sql_api_generate_jwt: Generates a token to use the snowpipe API
5. SecretesManager.py: reads and writes data from/to AWS secrets manager.
6. config.py: environment variables used for the project.  Many of these variables are set automatically by SecretsManager.py.  (It calls aws secrets manager and applies all snowflake variables)
7. logging_config.py: ensures all modules have the same log format.
