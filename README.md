# faker_to_snowflake
## Purpose
1. To create a sizable amount of fake data in Snowflake in the absolute cheapest way possible.
1. Showing cost difference between write_pandas (copy command) vs Snowpipe.
   - snowpipe does not use a warehouse; it ends up being significantly cheaper.
## Getting Started
### If running locally
1. Install `pdm` globally, if you don't already have it. `pip install pdm`
1. run `pdm install`, which will install of the dependancies for this project.

### .devcontainer and instructions will be added soon!


### Further steps regardless of Local vs devcontainer
1. Prerequisite: aws cli configured and working.
1. Add a .env file to the project root and add these variables.
    ```
    SNOWFLAKE_USER=YOUR_USER
    SNOWFLAKE_PASSWORD=YOUR_PASSWORD
    SNOWFLAKE_ACCOUNT=your-account
    SNOWFLAKE_WAREHOUSE=YOUR_WAREHOSUE
    SNOWFLAKE_DATABASE=YOUR_DATABASE
    SNOWFLAKE_SCHEMA=YOUR_SCHEMA
    SNOWFLAKE_ROLE=YOUR_ROLE
    SECRET_NAME = your-snowflake-secret-name-in-secrets-manager
    REGION = aws-region-of-your-secret
    ```
1. Review config.py to see how variables are set from `.env`
1. Directly run `SecretsManager.py` once.  This will create a new secret in AWS Secrets Manager according to your environment variables set in `.env`.
   - This assumes you have rsa_key.p8 in the root directory of the project.  This is the key file for your Snowflake user.
   - This project does not cover setting up the keypair auth.
1. You can run this locally by running `fake_data_to_snowflake.py` directly, adjusting the `number_of_rows` input.

## Deploying this as AWS Lambda
- I'm hoping to add automated steps using `aws cdk` in the near future.  For now, this is not a "one click build" project.
- For now, follow these manual steps:
1. Build the docker container.
1. Push it to `ECR`
1. Create a lambda that runs docker, select an `arm64` image type.
1. json body should be like this:
    ```
    {
    "rows": 100000
    }
    ```
1. Schedule it using EventBridge.
1. Enjoy your fake streaming table.