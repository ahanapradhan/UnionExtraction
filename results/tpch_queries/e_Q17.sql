(Select Avg(l_extendedprice) as avgtotal
From part, lineitem
Where p_partkey = l_partkey and p_brand  = 'Brand#52' and p_container  = 'LG CAN'
Limit 1);