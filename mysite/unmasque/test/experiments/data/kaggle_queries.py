Q_2_2_X_2 = '''SELECT tax_pickup_community_area_5,  tax_trip_start_timestamp_5,  tax_trip_end_timestamp_5,  RANK()  
OVER (  PARTITION BY tax_pickup_community_area_5  ORDER BY tax_trip_start_timestamp_5  ) AS trip_number 
FROM taxi_trips_5 
WHERE DATE(tax_trip_start_timestamp_5) = '2017-05-01';'''

Q_2_1_T_1 = '''WITH c AS
  (
  SELECT com_parent_5, COUNT(*) as num_comments
  FROM comments_5
  GROUP BY com_parent_5
  )
  SELECT s.sto_id as story_id, s.sto_by, s.sto_title, c.num_comments
  FROM stories AS s
  LEFT JOIN c
  ON s.sto_id = c.com_parent_5
  WHERE s.sto_time_ts = '2012-01-01'
  ORDER BY c.num_comments DESC;'''

Q_1_5_T_1 = '''WITH time AS
  (
  SELECT DATE(tra_block_timestamp) AS trans_date
  FROM transactions
  )
  SELECT COUNT(1) AS transactions,
  trans_date
  FROM time
  GROUP BY trans_date
  ORDER BY trans_date;'''



