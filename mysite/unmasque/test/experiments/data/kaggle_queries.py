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

Q_2_1_X_2 = '''SELECT q.pq_owner_user_id_3 AS owner_user_id,
  MIN(q.pq_creation_date_3) AS q_creation_date,
  MIN(a.pos_creation_date_4) AS a_creation_date
  FROM posts_questions_3 AS q
  JOIN posts_answers_4 AS a
  ON q.pq_owner_user_id_3 = a.pos_owner_user_id_4
  WHERE q.pq_creation_date_3 >= '2019-01-01'
  AND q.pq_creation_date_3 < '2019-02-01'
  AND a.pos_creation_date_4 >= '2019-01-01'
  AND a.pos_creation_date_4 < '2019-02-01'
  GROUP BY q.pq_owner_user_id_3;'''

Q_2_1_X_3 = '''SELECT u.use_id AS id,
  MIN(q.pq_creation_date_4) AS q_creation_date,
  MIN(a.pos_creation_date_5) AS a_creation_date
  FROM posts_questions_4 AS q
  JOIN posts_answers_5 AS a
  ON q.pq_owner_user_id_4 = a.pos_owner_user_id_5
  RIGHT JOIN users AS u
  ON q.pq_owner_user_id_4 = u.use_id
  WHERE u.use_creation_date >= '2019-01-01'
  and u.use_creation_date < '2019-02-01'
  GROUP BY u.use_id;'''

Q_2_1_X_4 = '''SELECT q.pq_owner_user_id_5
  FROM posts_questions_5 AS q
  WHERE DATE(q.pq_creation_date_5) = '2019-01-01'
  UNION
  SELECT a.pos_owner_user_id_6
  FROM posts_answers_6 AS a
  WHERE DATE(a.pos_creation_date_6) = '2019-01-01';'''

