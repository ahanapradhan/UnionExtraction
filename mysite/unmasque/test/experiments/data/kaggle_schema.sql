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

CREATE TABLE IF NOT EXISTS public.transactions
(
    tra_hash text COLLATE pg_catalog."default",
    tra_size integer,
    tra_virtual_size integer,
    tra_version integer,
    tra_lock_time integer,
    tra_block_hash text COLLATE pg_catalog."default",
    tra_block_number integer,
    tra_block_timestamp date,
    tra_block_timestamp_month date,
    tra_input_count integer,
    tra_output_count integer,
    tra_input_value double precision,
    tra_output_value integer,
    tra_is_coinbase text COLLATE pg_catalog."default",
    tra_fee integer,
    tra_inputs text COLLATE pg_catalog."default",
    tra_outputs text COLLATE pg_catalog."default"
)

CREATE TABLE IF NOT EXISTS public.stories
(
    sto_id integer,
    sto_by text COLLATE pg_catalog."default",
    sto_score integer,
    sto_time integer,
    sto_time_ts date,
    sto_title text COLLATE pg_catalog."default",
    sto_url text COLLATE pg_catalog."default",
    sto_text text COLLATE pg_catalog."default",
    sto_deleted text COLLATE pg_catalog."default",
    sto_dead text COLLATE pg_catalog."default",
    sto_descendants integer,
    sto_author text COLLATE pg_catalog."default"
)

CREATE TABLE IF NOT EXISTS public.comments_5
(
    com_id_5 integer,
    com_by_5 text COLLATE pg_catalog."default",
    com_author_5 text COLLATE pg_catalog."default",
    com_time_5 integer,
    com_time_ts_5 date,
    com_text_5 text COLLATE pg_catalog."default",
    com_parent_5 integer,
    com_deleted_5 text COLLATE pg_catalog."default",
    com_dead_5 text COLLATE pg_catalog."default",
    com_ranking_5 integer
)

