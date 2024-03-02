Select l_shipmode, Count(*) as count
 From lineitem, orders 
 Where l_orderkey = o_orderkey
 and l_extendedprice <= o_totalprice
 and 60000.005 <= o_totalprice
 and l_extendedprice <= 70000.004
 and l_receiptdate <= '1994-12-31'
 and '1994-01-01' <= l_receiptdate
 and l_commitdate <= '1994-12-30'
 and l_shipdate <= '1994-12-29'
 and l_shipdate < l_commitdate
 and l_commitdate < l_receiptdate 
 Group By l_shipmode 
 Order By l_shipmode asc;