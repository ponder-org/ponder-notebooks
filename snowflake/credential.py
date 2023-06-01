params = {
  "account": "<account_identifier>",
  "user": "<username>",
  "password": "<password>",
  "role": "<role_name>",
  "warehouse": "<warehouse_name>",
  "database": "<database_name>",
  "schema": "<schema_name>"
}

def check_credential_not_unpopulated():
    assert params["account"]!="<account_identifier>", "'account' field in credential.py unpopulated. Please add your Snowflake information to credential.py"
    assert params["user"]!="<username>", "'user' field in credential.py unpopulated. Please add your Snowflake information to credential.py"
    assert params["password"]!="<password>", "'password' field in credential.py unpopulated. Please add your Snowflake information to credential.py"
    assert params["role"]!="<role_name>", "'role' field in credential.py unpopulated. Please add your Snowflake information to credential.py"
    assert params["warehouse"]!="<warehouse_name>", "'warehouse' field in credential.py unpopulated. Please add your Snowflake information to credential.py"
    assert params["database"]!="<database_name>", "'database' field in credential.py unpopulated. Please add your Snowflake information to credential.py"
    assert params["schema"]!="<schema_name>", "'schema' field in credential.py unpopulated. Please add your Snowflake information to credential.py"

check_credential_not_unpopulated()
