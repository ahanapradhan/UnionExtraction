Q_2_2_X_2	= '''SELECT pickup_community_area,  trip_start_timestamp,  trip_end_timestamp,  RANK()  
OVER (  PARTITION BY pickup_community_area  ORDER BY trip_start_timestamp  ) AS trip_number 
FROM taxi_trips_5 
WHERE DATE(trip_start_timestamp) = '2017-05-01';'''

