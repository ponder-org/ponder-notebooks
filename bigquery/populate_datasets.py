import ponder

ponder.init()
from google.cloud import bigquery
from google.cloud.bigquery import dbapi
from google.oauth2 import service_account
import json

db_con = dbapi.Connection(
    bigquery.Client(
        credentials=service_account.Credentials.from_service_account_info(
            json.loads(open("credential.json").read()),
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
    )
)

ponder.configure(bigquery_dataset="TEST", default_connection=db_con)

import modin.pandas as pd


def try_upload_csv(db_con, url, name):
    df = pd.read_csv(url)
    try:
        df.to_sql(name, db_con, index=False)
        print(f"Uploaded dataset to {name}")
    except ValueError as e:
        print(f"PONDER.{name} already exists")


# try_upload_csv(db_con,"https://github.com/ponder-org/ponder-datasets/blob/main/citibike_tutorial.csv?raw=True", "PONDER_CITIBIKE")
try_upload_csv(
    db_con,
    "https://github.com/ponder-org/ponder-datasets/blob/main/books.csv?raw=True",
    "PONDER_BOOKS",
)
# Upload TPC-H
try_upload_csv(
    db_con,
    "https://github.com/ponder-org/ponder-datasets/blob/main/tpch/customer.csv?raw=True",
    "PONDER_CUSTOMER",
)
try_upload_csv(
    db_con,
    "https://github.com/ponder-org/ponder-datasets/blob/main/tpch/orders.csv?raw=True",
    "PONDER_ORDERS",
)
try_upload_csv(
    db_con,
    "https://github.com/ponder-org/ponder-datasets/blob/main/tpch/part.csv?raw=True",
    "PONDER_PART",
)
try_upload_csv(
    db_con,
    "https://github.com/ponder-org/ponder-datasets/blob/main/tpch/supplier.csv?raw=True",
    "PONDER_SUPPLIER",
)
