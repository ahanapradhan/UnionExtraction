Select Avg(l_extendedprice) as avgtotal
From lineitem, part
Where l_partkey = p_partkey and p_brand  = 'Brand#52' and p_container  = 'LG CAN'
Limit 1;