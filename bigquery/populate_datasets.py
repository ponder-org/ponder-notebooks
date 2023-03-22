import ponder.bigquery
import modin.pandas as pd

# Create a Ponder BigQuery Connections object
bigquery_con = ponder.bigquery.connect(creds, schema = "TEST")

# Initialize the BigQuery connection
ponder.bigquery.init(bigquery_con)

# df = pd.read_csv("https://github.com/ponder-org/ponder-datasets/blob/main/citibike_trial.csv?raw=True", on_bad_lines='skip')
# df.to_sql("PONDER_CITIBIKE",bigquery_con,index=False)
# print("Uploaded dataset to PONDER_CITIBIKE")


# df = pd.read_csv("https://github.com/ponder-org/ponder-datasets/blob/main/books.csv?raw=True", on_bad_lines='skip')
# df = df.dropna() # Causes errors for None writeback
# df.to_sql("PONDER_BOOKS",bigquery_con,index=False)
# print("Uploaded dataset to PONDER_BOOKS")


# df = pd.read_csv("https://github.com/ponder-org/ponder-datasets/blob/main/yellow_tripdata_2015-01.csv?raw=True", on_bad_lines='skip')
# df.to_sql("PONDER_TAXI",bigquery_con,index=False)
# print("Uploaded dataset to PONDER_TAXI")


# Upload TPC-H
df = pd.read_csv("https://github.com/ponder-org/ponder-datasets/blob/main/tpch/customer.csv?raw=True", on_bad_lines='skip')
df.to_sql("PONDER_CUSTOMER",bigquery_con,index=False)
print("Uploaded dataset to PONDER_CUSTOMER")

df = pd.read_csv("https://github.com/ponder-org/ponder-datasets/blob/main/tpch/orders.csv?raw=True", on_bad_lines='skip')
df.to_sql("PONDER_ORDERS",bigquery_con,index=False)
print("Uploaded dataset to PONDER_ORDER")

df = pd.read_csv("https://github.com/ponder-org/ponder-datasets/blob/main/tpch/part.csv?raw=True", on_bad_lines='skip')
df.to_sql("PONDER_PART",bigquery_con,index=False)
print("Uploaded dataset to PONDER_PART")

df = pd.read_csv("https://github.com/ponder-org/ponder-datasets/blob/main/tpch/supplier.csv?raw=True", on_bad_lines='skip')
df.to_sql("PONDER_SUPPLIER",bigquery_con,index=False)
print("Uploaded dataset to PONDER_SUPPLIER")
