-- Query public available table
SELECT station_id, name FROM bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;

--CREACION TABLA EXTERNA
CREATE EXTERNAL TABLE `h-c-123.cd_week3.yellow_week3`
  OPTIONS (
    format ="PARQUET",
    uris = ['gs://de/yellow/yellow_tripdata_2024-*.parquet']
    );

--CONSULTA PRUEBA
SELECT * FROM `h-c-123.cd_week3.yellow_week3` LIMIT 10;

-- 1. NUM ROWS, 20332093
select count(*) FROM `h-c-123.cd_week3.yellow_week3`;

-- TABLA
CREATE OR REPLACE TABLE `h-c-123.cd_week3.yellow_week3_non_partitioned` AS
SELECT * FROM `h-c-123.cd_week3.yellow_week3`;

--VIEW
CREATE OR REPLACE MATERIALIZED VIEW `h-c-123.cd_week3.v_yellow_week3_non_partitioned` AS
SELECT * FROM `h-c-123.cd_week3.yellow_week3`;

-- PRUEBA
select a.tpep_pickup_datetime,a.tpep_dropoff_datetime,a.Airport_fee,a.fare_amount FROM `h-c-123.cd_week3.yellow_week3_non_partitioned` a;

-- 2. distinct number of PULocationIDs
-- 0 MB for the External Table and 155.12 MB for the Materialized Table
select COUNT(PULocationID) FROM `h-c-123.cd_week3.yellow_week3_non_partitioned`
GROUP BY PULocationID;
-- 0 MB
select COUNT(PULocationID) FROM `h-c-123.cd_week3.yellow_week3`
GROUP BY PULocationID;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `h-c-123.cd_week3.yellow_week3_partitioned`
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM `h-c-123.cd_week3.yellow_week3`;

-- 3.
SELECT PULocationID, DOLocationID FROM `h-c-123.cd_week3.yellow_week3_non_partitioned`
;

-- 4, 8333
SELECT COUNT(fare_amount)
FROM `h-c-123.cd_week3.yellow_week3_non_partitioned`
WHERE fare_amount = 0;

--5
-- Create a partitioned table tpep_dropoff_datetime 
CREATE OR REPLACE TABLE `h-c-123.cd_week3.yellow_week3_partitioned_tpep_dropoff_datetime_cluster_VendorID`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS SELECT * FROM `h-c-123.cd_week3.yellow_week3`;

-- Create a partitioned table tpep_dropoff_datetime and VendorID
CREATE OR REPLACE TABLE `h-c-123.cd_week3.yellow_week3_partitioned_tpep_dropoff_datetime_and_VendorID`
PARTITION BY DATE(tpep_dropoff_datetime),vendorId
CLUSTER BY VendorID
AS SELECT * FROM `h-c-123.cd_week3.yellow_week3`;


-- 6. NUM ROWS, 20332093
-- 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
--NO PARTTIONED
select DISTINCT(VendorID) FROM `h-c-123.cd_week3.yellow_week3_non_partitioned`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

--PARTTIONED
select DISTINCT(VendorID) FROM `h-c-123.cd_week3.yellow_week3_partitioned_tpep_dropoff_datetime_cluster_VendorID` 
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'
;