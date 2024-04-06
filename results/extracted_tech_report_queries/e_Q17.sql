Select Avg(l_extendedprice) as avgtotal
 From lineitem, part 
 Where lineitem.l_partkey = part.p_partkey
 and part.p_brand = 'Brand#52'
 and part.p_container = 'LG CAN';