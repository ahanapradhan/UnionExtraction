-- Table: public.taxi_trips_5

-- DROP TABLE IF EXISTS public.taxi_trips_5;

CREATE TABLE IF NOT EXISTS public.taxi_trips_5
(
    tax_unique_key_5 text COLLATE pg_catalog."default",
    tax_taxi_id_5 text COLLATE pg_catalog."default",
    tax_trip_start_timestamp_5 text COLLATE pg_catalog."default",
    tax_trip_end_timestamp_5 text COLLATE pg_catalog."default",
    tax_trip_seconds_5 integer,
    tax_trip_miles_5 double precision,
    tax_pickup_census_tract_5 double precision,
    tax_dropoff_census_tract_5 double precision,
    tax_pickup_community_area_5 double precision,
    tax_dropoff_community_area_5 double precision,
    tax_fare_5 double precision,
    tax_tips_5 double precision,
    tax_tolls_5 double precision,
    tax_extras_5 double precision,
    tax_trip_total_5 double precision,
    tax_payment_type_5 text COLLATE pg_catalog."default",
    tax_company_5 text COLLATE pg_catalog."default",
    tax_pickup_latitude_5 double precision,
    tax_pickup_longitude_5 double precision,
    tax_pickup_location_5 double precision,
    tax_dropoff_latitude_5 double precision,
    tax_dropoff_longitude_5 double precision,
    tax_dropoff_location_5 double precision
)
