Select l_shipmode, Sum(l_extendedprice*l_discount) as revenue
From lineitem
Where l_quantity  <= 23.0 and l_shipdate  >= '1994-01-01' and l_shipdate <= '1994-12-31'
Group By l_shipmode
Order By l_shipmode asc
Limit 100;