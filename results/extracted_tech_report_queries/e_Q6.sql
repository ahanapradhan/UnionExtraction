Select l_shipmode, Sum(l_discount*l_extendedprice) as revenue
 From lineitem 
 Where lineitem.l_quantity  <= 23.99
 and lineitem.l_shipdate  >= '1994-01-01' and lineitem.l_shipdate <= '1994-12-31' 
 Group By l_shipmode 
 Order By l_shipmode asc 
 Limit 100;