Q_2_2_X_2	= '''SELECT tax_pickup_community_area_5,  tax_trip_start_timestamp_5,  tax_trip_end_timestamp_5,  RANK()  
OVER (  PARTITION BY tax_pickup_community_area_5  ORDER BY tax_trip_start_timestamp_5  ) AS trip_number 
FROM taxi_trips_5 
WHERE DATE(tax_trip_start_timestamp_5) = '2017-05-01';'''

