import dlt
import requests
import time
import duckdb
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

# Define the API resource for NYC taxi data
@dlt.resource(name="rides")   # <--- The name of the resource (will be used as the table name)
def ny_taxi():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):    # <--- API endpoint for retrieving taxi ride data
        yield page   # <--- yield data to manage memory

 #✅ Step 3: Initialize the pipeline
pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination="duckdb",
    dataset_name="ny_taxi_data",
    full_refresh=False
)

# ✅ Step 4: Run the pipeline and verify data load
load_info = pipeline.run(ny_taxi())  # Run the pipeline
print("Pipeline Load Info:", load_info)

# ✅ Step 5: Connect to DuckDB database
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

# ✅ Step 6: Check if any tables exist in any schema
all_tables = conn.sql("SELECT table_schema, table_name FROM information_schema.tables").fetchall()
print("All Tables in DuckDB:", all_tables)

# ✅ Step 7: Ensure the correct schema is set before querying tables
conn.sql("SET search_path TO ny_taxi_data")

# ✅ Step 8: Check tables again after setting the schema
tables = conn.sql("SHOW TABLES").fetchall()
print("Existing tables:", tables)

# ✅ Step 9: Ensure the `rides` table exists and inspect its data
table_name = "rides"

if any(table_name in t for t in tables):
    print(f"Describing table: {table_name}")

    # Fetch column details (Avoids NumPy)
    description = conn.sql(f"DESCRIBE {table_name}").fetchall()
    print("Table Description for rides:", description)

    # ✅ Step 10: Check if the rides table has data
    record_count = conn.sql(f"SELECT COUNT(*) FROM {table_name}").fetchall()
    print(f"Total records in {table_name}:", record_count[0][0])

    # ✅ Step 11: Preview first 5 rows
    sample_data = conn.sql(f"SELECT * FROM {table_name} LIMIT 5").fetchall()
    print(f"Sample data from {table_name}:", sample_data)

# Count the total number of tables in DuckDB
table_count = conn.sql("SELECT COUNT(*) FROM information_schema.tables").fetchall()
print(f"Total number of tables: {table_count[0][0]}")

# Count the total number of rows in the rides table
record_count = conn.sql("SELECT COUNT(*) FROM rides").fetchall()
print(f"Total rows in rides table: {record_count[0][0]}")

with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT
            AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
            FROM rides;
            """
        )
    # Prints column values of the first row
    print(res)
