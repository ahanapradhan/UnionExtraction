Select l_shipmode, sum(l_extendedprice * l_discount) as revenue From lineitem  
     Where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and  
     l_quantity < 24 Group By l_shipmode Limit 100;