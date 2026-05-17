# Data Model — Grain and Key Definitions

## fact_trips

Grain: one row per completed yellow taxi trip.

Primary key: `trip_id`
Construction: SHA-256(VendorID || tpep_pickup_datetime || PULocationID)
Uniqueness guarantee: enforced by DQ check `check_uniqueness(fact, "trip_id")`.

Foreign keys:
- `pickup_date_sk` -> dim_date.date_sk
- `pickup_location_sk` -> dim_location.location_sk
- `dropoff_location_sk` -> dim_location.location_sk

## dim_location

Grain: one row per NYC taxi zone.
Primary key: `location_sk` (natural key, 265 zones, no surrogate needed).

## dim_date

Grain: one row per calendar day within the loaded year.
Primary key: `date_sk` (integer YYYYMMDD, e.g. 20230115).
