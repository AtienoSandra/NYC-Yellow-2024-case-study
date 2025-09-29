-- PROJECT: NYC Yellow Taxi Trips Case Study
-- TOOLS: DuckDB
-- DATA: NYC Yellow Taxi Trips (Jan–Dec 2024) via Parquet URLs
-- AUTHOR: Atieno Sandra

--- OPEN DATABASE & SET OUTPUT
--------------------------------------------------------------------
-- Create persistent duckdb database file
.open 'C:/Users/HP/OneDrive/Desktop/SANDY/MasterclassSQL/Yellow2024_study/yellow_2024.duckdb'

-- Save all query outputs to a text file for reporting
.output 'C:/Users/HP/OneDrive/Desktop/SANDY/MasterclassSQL/Yellow2024_study/yellow_2024_output.txt'

-- STEP 1: LOAD YELLOW TAXI JAN-DEC 2024 RAW DATA USING URLs
--------------------------------------------------------------------

CREATE VIEW IF NOT EXISTS nyc_taxi_2024_raw AS
SELECT * 
FROM read_parquet(['https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet',
'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet',
'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet', 
'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-04.parquet',
'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-05.parquet',
'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-06.parquet',
'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-07.parquet',
'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-08.parquet',
'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-09.parquet',
'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet',
'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-11.parquet',
'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet' 
]); 

CREATE VIEW IF NOT EXISTS taxi_zones AS
SELECT * FROM read_csv ('https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv');

-- creating a view points to all 2024 monthly Parquet files and the taxi zone lookup table.
-- This allows querying without downloading the dataset.

DESCRIBE nyc_taxi_2024_raw; -- Schema check

-- STEP 2: DATA CLEANING & VALIDATION
---- We perform data cleaning in 4 steps:
------ A. Flag invalid records
------ B. Filter the valid trips
------ C. Generate a summary of the rejected trips & compare the number of rows in raw data and cleaned data

-- STEP 2: DATA CLEANING & VALIDATION
---- We perform data cleaning in 4 steps:
------ A. Flag invalid records
------ B. Filter the valid trips
------ C. Generate a summary of the rejected trips & compare row counts
------ D. Create summary stats for clean dataset
------ E. Build lookup table for payment types

-- A. Flag invalid records
CREATE OR REPLACE TABLE trips_2024_flagged AS
SELECT *,
    CASE 
        WHEN passenger_count IS NULL OR passenger_count <= 0 THEN 'Invalid'
        ELSE 'Valid'
    END AS passenger_flag,

    CASE 
        WHEN trip_distance IS NULL OR trip_distance <= 0 THEN 'Invalid'
        ELSE 'Valid'
    END AS distance_flag,

    CASE 
        WHEN tpep_dropoff_datetime <= tpep_pickup_datetime THEN 'Invalid'
        WHEN DATE_DIFF('second', tpep_pickup_datetime, tpep_dropoff_datetime) > 6*3600 
             THEN 'Too Long' ---- trips > 6 hrs
        ELSE 'Valid'
    END AS duration_flag,

    CASE 
        WHEN trip_distance / NULLIF(DATE_DIFF('second', tpep_pickup_datetime, tpep_dropoff_datetime)/3600.0,0) > 100 
             THEN 'High speed'  -----  speed > 100 mph
        ELSE 'Valid'
    END AS speed_flag
FROM nyc_taxi_2024_raw;

-- B. Create clean dataset by filtering valid trips only
DROP TABLE IF EXISTS trips_2024_clean;
CREATE TABLE trips_2024_clean AS
SELECT *,
       DATE_DIFF('second', tpep_pickup_datetime, tpep_dropoff_datetime)/60.0 AS trip_duration_mins
FROM trips_2024_flagged
WHERE 
EXTRACT(YEAR FROM tpep_pickup_datetime)=2024
  AND  passenger_flag = 'Valid'
  AND distance_flag = 'Valid'
  AND duration_flag = 'Valid'
  AND speed_flag = 'Valid';

-- C. Save rejected trips.
CREATE OR REPLACE TABLE trips_2024_rejected AS
SELECT *
FROM trips_2024_flagged
WHERE passenger_flag <> 'Valid'
   OR distance_flag <> 'Valid'
   OR duration_flag <> 'Valid'
   OR speed_flag <> 'Valid'; --- gives a cross table of the different rejection combinations. Found this interesting

-- Rejected trips breakdown
SELECT passenger_flag, distance_flag, duration_flag, speed_flag, COUNT(*) AS rejected_trips
FROM trips_2024_rejected
GROUP BY passenger_flag, distance_flag, duration_flag, speed_flag
ORDER BY rejected_trips DESC;

-- Trips before vs after cleaning
SELECT 
    (SELECT COUNT(*) FROM nyc_taxi_2024_raw) AS total_raw,
    (SELECT COUNT(*) FROM trips_2024_clean) AS total_clean,
    (SELECT COUNT(*) FROM trips_2024_rejected) AS total_rejected;

-- D. Summary statistics (distance, duration, fare, tip) for clean data
SELECT 
    ROUND(AVG(passenger_count),0) AS avg_passenger_count,
    ROUND(AVG(trip_distance),2) AS avg_distance_miles,
    ROUND(AVG(fare_amount),2) AS avg_fare,
    ROUND(AVG(total_amount),2) AS avg_total_amount,
    ROUND(AVG(trip_duration_mins),2) AS avg_duration_minutes,
    MIN(trip_duration_mins) AS min_duration_minutes,
    MAX(trip_duration_mins) AS max_duration_minutes
FROM trips_2024_clean;


--E. Build lookup table for payment_type codes  present 

-- Check distinct payment_type values in the dataset
SELECT DISTINCT payment_type
FROM trips_2024_clean
ORDER BY payment_type;

--  Count frequency of each payment_type code
SELECT 
    payment_type,
    COUNT(*) AS trips
FROM trips_2024_clean
GROUP BY payment_type
ORDER BY trips DESC;

-- Join with lookup table for human-readable names
CREATE OR REPLACE TABLE payment_lookup AS
SELECT * FROM (VALUES
    (1, 'Credit Card'),
    (2, 'Cash'),
    (3, 'No Charge'),
    (4, 'Dispute')
) AS pl(payment_type, payment_desc); -- our clean data has only this 4. type 4 is dispute bcoz from the 
                                    -- data dictionary, passenger disputed the fare with driver/company.

-- Payment types summary with tips
SELECT 
    COALESCE(pl.payment_desc, 'Unknown') AS payment_method,
    ROUND(AVG(t.tip_amount),2) AS avg_tip,
    COUNT(*) AS trips
FROM trips_2024_clean t
LEFT JOIN payment_lookup pl
    ON t.payment_type = pl.payment_type
GROUP BY payment_method
ORDER BY avg_tip DESC;


-- DATA ANALYSIS
    --- For Data Analysis I wish to introduce a new table with additional/enhanced columns. This will allow
    --- me to analyze the data without interfering with the clean dataset (trips_2024_clean). This is a precaution incase
    --- the business rule changes, I can rebuild the analysis table from trips_2024_clean.

-- Add additional columns for analysis

CREATE OR REPLACE TABLE trips_2024_analysis AS
SELECT
    *,
    DATE(tpep_pickup_datetime) AS trip_date,
    STRFTIME(tpep_pickup_datetime, '%Y-%m') AS trip_month,
    STRFTIME(tpep_pickup_datetime, '%w') AS day_of_week, -- 0=Sunday
    STRFTIME(tpep_pickup_datetime, '%H') AS pickup_hour
FROM trips_2024_clean
;

PRAGMA table_info('trips_2024_analysis');

-- Trips per month
SELECT trip_month, COUNT(*) AS trips
FROM trips_2024_analysis
GROUP BY trip_month
ORDER BY trip_month;

-- Average fare per month
SELECT trip_month, AVG(fare_amount) AS avg_fare
FROM trips_2024_analysis
GROUP BY trip_month
ORDER BY trip_month;

-- Top 10 pickup zones by revenue
SELECT 
    z.Zone AS pickup_zone,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount),2) AS total_revenue
FROM trips_2024_analysis t
JOIN taxi_zones z ON t.PULocationID = z.LocationID
GROUP BY z.Zone
ORDER BY total_revenue DESC
LIMIT 10;

--- Peak Hours

WITH hourly_trips AS (
    SELECT 
        pickup_hour,
        COUNT(*) AS total_trips
    FROM trips_2024_analysis
    GROUP BY pickup_hour
)
SELECT 
    pickup_hour,
    total_trips,
    RANK() OVER (ORDER BY total_trips DESC) AS demand_rank
FROM hourly_trips
ORDER BY demand_rank;


--- Trips analysis by seasons (Winter, Spring, Summer, Fall)

-- Seasonal trips and revenue
SELECT 
    CASE 
        WHEN STRFTIME(tpep_pickup_datetime, '%m') IN ('12','01','02') THEN 'Winter'
        WHEN STRFTIME(tpep_pickup_datetime, '%m') IN ('03','04','05') THEN 'Spring'
        WHEN STRFTIME(tpep_pickup_datetime, '%m') IN ('06','07','08') THEN 'Summer'
        WHEN STRFTIME(tpep_pickup_datetime, '%m') IN ('09','10','11') THEN 'Fall'
    END AS season,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount),2) AS total_revenue
FROM trips_2024_analysis
GROUP BY season
ORDER BY total_revenue DESC;



--- Driver/Vendor analysis

WITH driver_efficiency AS (
    SELECT 
        VendorID,
        trip_distance,
        trip_duration_mins,
        total_amount
    FROM trips_2024_analysis
), --- this CTE groups the efficiency variables

driver_summary AS (
    SELECT 
        VendorID,
        COUNT(*) AS total_trips,
        SUM(total_amount) AS total_revenue,
        AVG(trip_distance) AS avg_distance,
        AVG(trip_duration_mins) AS avg_duration,
        ROUND(AVG((trip_distance / NULLIF(trip_duration_mins,0)) * 60), 2) AS avg_speed_mph
    FROM driver_efficiency
    GROUP BY VendorID
) --- this CTE gives summary statistics per driver. We then rank drivers by revenue and efficiency
SELECT 
    VendorID,
    total_trips,
    ROUND(total_revenue,2) AS total_revenue,
    ROUND(avg_distance,2) AS avg_distance,
    ROUND(avg_duration,2) AS avg_duration,
    avg_speed_mph,
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
    RANK() OVER (ORDER BY avg_speed_mph DESC) AS efficiency_rank
FROM driver_summary
ORDER BY revenue_rank;

