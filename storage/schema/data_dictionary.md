# Data Dictionary

## fact_trips

Grain: one row per completed yellow taxi trip.
Primary key: `trip_id`
Source: NYC TLC Yellow Taxi Trip Records (public S3)
Location: `s3a://nexlab-lake/curated/gold/fact_trips/`

| Column | Type | Description | Source Column |
|--------|------|-------------|---------------|
| trip_id | string | SHA-256 hash of VendorID + pickup_datetime + PULocationID | derived |
| pickup_date_sk | int | Foreign key to dim_date (format YYYYMMDD) | derived |
| pickup_location_sk | int | Foreign key to dim_location for pickup | PULocationID |
| dropoff_location_sk | int | Foreign key to dim_location for dropoff | DOLocationID |
| VendorID | int | Taxi vendor: 1 = Creative Mobile, 2 = VeriFone | VendorID |
| tpep_pickup_datetime | timestamp | Trip start time | tpep_pickup_datetime |
| tpep_dropoff_datetime | timestamp | Trip end time | tpep_dropoff_datetime |
| passenger_count | int | Number of passengers (1–9) | passenger_count |
| trip_distance | double | Miles traveled per taximeter | trip_distance |
| trip_duration_minutes | double | (dropoff - pickup) in minutes | derived |
| fare_amount | double | Base metered fare in USD | fare_amount |
| tip_amount | double | Credit card tip in USD | tip_amount |
| tolls_amount | double | Tolls paid in USD | tolls_amount |
| total_amount | double | Total charged to passenger in USD | total_amount |
| pickup_hour | int | Hour of day for pickup (0–23) | derived |
| year | int | Partition column: year of pickup | derived |
| month | int | Partition column: month of pickup | derived |

---

## dim_location

Grain: one row per NYC taxi zone (265 zones).
Primary key: `location_sk`
Source: NYC TLC Taxi Zone Lookup CSV
Location: `s3a://nexlab-lake/curated/gold/dim_location/`

| Column | Type | Description |
|--------|------|-------------|
| location_sk | int | Surrogate key (same as location_id for this dataset) |
| location_id | int | Original zone ID from source file |
| borough | string | NYC borough (Manhattan, Queens, Brooklyn, Bronx, Staten Island, EWR) |
| zone | string | Neighborhood or landmark name |
| service_zone | string | Service type: Yellow Zone, Boro Zone, Airports |

---

## dim_date

Grain: one row per calendar day for the loaded year.
Primary key: `date_sk`
Location: `s3a://nexlab-lake/curated/gold/dim_date/`

| Column | Type | Description |
|--------|------|-------------|
| date_sk | int | Surrogate key in YYYYMMDD format |
| full_date | date | Calendar date |
| year | int | Calendar year |
| month | int | Calendar month (1–12) |
| day | int | Day of month |
| day_of_week | int | Day of week (1 = Sunday, 7 = Saturday) |
| week_of_year | int | ISO week number |
| is_weekend | boolean | True if Saturday or Sunday |
| quarter | int | Calendar quarter (1–4) |
