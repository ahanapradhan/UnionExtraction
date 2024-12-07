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

CREATE TABLE IF NOT EXISTS public.posts_answers_4
(
    pos_id_4 integer,
    pos_title_4 text COLLATE pg_catalog."default",
    pos_body_4 text COLLATE pg_catalog."default",
    pos_accepted_answer_id_4 integer,
    pos_answer_count_4 integer,
    pos_comment_count_4 integer,
    pos_community_owned_date_4 date,
    pos_creation_date_4 date,
    pos_favorite_count_4 integer,
    pos_last_activity_date_4 date,
    pos_last_edit_date_4 date,
    pos_last_editor_display_name_4 text COLLATE pg_catalog."default",
    pos_last_editor_user_id_4 integer,
    pos_owner_display_name_4 text COLLATE pg_catalog."default",
    pos_owner_user_id_4 integer,
    pos_parent_id_4 integer,
    pos_post_type_id_4 integer,
    pos_score_4 integer,
    pos_tags_4 text COLLATE pg_catalog."default",
    pos_view_count_4 integer
)

CREATE TABLE IF NOT EXISTS public.posts_questions_3
(
    pq_id_3 integer,
    pq_title_3 text COLLATE pg_catalog."default",
    pq_body_3 text COLLATE pg_catalog."default",
    pq_accepted_answer_id_3 integer,
    pq_answer_count_3 integer,
    pq_comment_count_3 integer,
    pq_community_owned_date_3 date,
    pq_creation_date_3 date,
    pq_favorite_count_3 integer,
    pq_last_activity_date_3 date,
    pq_last_edit_date_3 date,
    pq_last_editor_display_name_3 text COLLATE pg_catalog."default",
    pq_last_editor_user_id_3 integer,
    pq_owner_display_name_3 text COLLATE pg_catalog."default",
    pq_owner_user_id_3 integer,
    pq_parent_id_3 integer,
    pq_post_type_id_3 integer,
    pq_score_3 integer,
    pq_tags_3 text COLLATE pg_catalog."default",
    pq_view_count_3 integer
)

CREATE TABLE IF NOT EXISTS public.posts_questions_4
(
    pq_id_4 integer,
    pq_title_4 text COLLATE pg_catalog."default",
    pq_body_4 text COLLATE pg_catalog."default",
    pq_accepted_answer_id_4 integer,
    pq_answer_count_4 integer,
    pq_comment_count_4 integer,
    pq_community_owned_date_4 date,
    pq_creation_date_4 date,
    pq_favorite_count_4 double precision,
    pq_last_activity_date_4 date,
    pq_last_edit_date_4 date,
    pq_last_editor_display_name_4 text COLLATE pg_catalog."default",
    pq_last_editor_user_id_4 integer,
    pq_owner_display_name_4 text COLLATE pg_catalog."default",
    pq_owner_user_id_4 integer,
    pq_parent_id_4 integer,
    pq_post_type_id_4 integer,
    pq_score_4 integer,
    pq_tags_4 text COLLATE pg_catalog."default",
    pq_view_count_4 integer
)

CREATE TABLE IF NOT EXISTS public.posts_answers_5
(
    pos_id_5 integer,
    pos_title_5 text COLLATE pg_catalog."default",
    pos_body_5 text COLLATE pg_catalog."default",
    pos_accepted_answer_id_5 integer,
    pos_answer_count_5 integer,
    pos_comment_count_5 integer,
    pos_community_owned_date_5 date,
    pos_creation_date_5 date,
    pos_favorite_count_5 integer,
    pos_last_activity_date_5 date,
    pos_last_edit_date_5 date,
    pos_last_editor_display_name_5 text COLLATE pg_catalog."default",
    pos_last_editor_user_id_5 integer,
    pos_owner_display_name_5 text COLLATE pg_catalog."default",
    pos_owner_user_id_5 integer,
    pos_parent_id_5 integer,
    pos_post_type_id_5 integer,
    pos_score_5 integer,
    pos_tags_5 text COLLATE pg_catalog."default",
    pos_view_count_5 integer
)

CREATE TABLE IF NOT EXISTS public.users
(
    use_id integer,
    use_display_name text COLLATE pg_catalog."default",
    use_about_me text COLLATE pg_catalog."default",
    use_age integer,
    use_creation_date date,
    use_last_access_date date,
    use_location text COLLATE pg_catalog."default",
    use_reputation integer,
    use_up_votes integer,
    use_down_votes integer,
    use_views integer,
    use_profile_image_url text COLLATE pg_catalog."default",
    use_website_url text COLLATE pg_catalog."default"
)

CREATE TABLE IF NOT EXISTS public.posts_answers_6
(
    pos_id_6 integer,
    pos_title_6 text COLLATE pg_catalog."default",
    pos_body_6 text COLLATE pg_catalog."default",
    pos_accepted_answer_id_6 integer,
    pos_answer_count_6 integer,
    pos_comment_count_6 integer,
    pos_community_owned_date_6 date,
    pos_creation_date_6 date,
    pos_favorite_count_6 integer,
    pos_last_activity_date_6 date,
    pos_last_edit_date_6 date,
    pos_last_editor_display_name_6 text COLLATE pg_catalog."default",
    pos_last_editor_user_id_6 integer,
    pos_owner_display_name_6 text COLLATE pg_catalog."default",
    pos_owner_user_id_6 integer,
    pos_parent_id_6 integer,
    pos_post_type_id_6 integer,
    pos_score_6 integer,
    pos_tags_6 text COLLATE pg_catalog."default",
    pos_view_count_6 integer
)

CREATE TABLE IF NOT EXISTS public.posts_questions_5
(
    pq_id_5 integer,
    pq_title_5 text COLLATE pg_catalog."default",
    pq_body_5 text COLLATE pg_catalog."default",
    pq_accepted_answer_id_5 integer,
    pq_answer_count_5 integer,
    pq_comment_count_5 integer,
    pq_community_owned_date_5 date,
    pq_creation_date_5 date,
    pq_favorite_count_5 integer,
    pq_last_activity_date_5 date,
    pq_last_edit_date_5 date,
    pq_last_editor_display_name_5 text COLLATE pg_catalog."default",
    pq_last_editor_user_id_5 integer,
    pq_owner_display_name_5 text COLLATE pg_catalog."default",
    pq_owner_user_id_5 integer,
    pq_parent_id_5 integer,
    pq_post_type_id_5 integer,
    pq_score_5 integer,
    pq_tags_5 text COLLATE pg_catalog."default",
    pq_view_count_5 integer
)
