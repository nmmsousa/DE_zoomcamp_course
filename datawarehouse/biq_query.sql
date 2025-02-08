-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://data_lake_csv/yellow_tripdata_2024-*.parquet']
);


-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE zoomcamp.yellow_tripdata_non_partitoned AS
SELECT * FROM zoomcamp.external_yellow_tripdata;

--Question 1: What is count of records for the 2024 Yellow Taxi Data?
select count(*) 
from `zoomcamp.yellow_tripdata_non_partitoned`;

--Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
select distinct PULocationID from `zoomcamp.yellow_tripdata_non_partitoned`;
select distinct PULocationID from `zoomcamp.external_yellow_tripdata`;

--Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?
select PULocationID from `zoomcamp.yellow_tripdata_non_partitoned`;
select PULocationID, DOLocationID from `zoomcamp.yellow_tripdata_non_partitoned`;

--How many records have a fare_amount of 0?
select count(*) from `zoomcamp.yellow_tripdata_non_partitoned`
where fare_amount = 0;

--What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)
CREATE OR REPLACE TABLE zoomcamp.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM zoomcamp.external_yellow_tripdata;

--Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
select distinct VendorID from `zoomcamp.yellow_tripdata_non_partitoned`
where cast (tpep_dropoff_datetime as date) between '2024-03-01' and '2024-03-15';
select distinct VendorID from `zoomcamp.yellow_tripdata_partitoned_clustered`
where cast (tpep_dropoff_datetime as date) between '2024-03-01' and '2024-03-15';