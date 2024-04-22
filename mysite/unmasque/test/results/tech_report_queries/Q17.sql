Select AVG(l_extendedprice) as avgTOTAL From lineitem, part  
      Where p_partkey = l_partkey and p_brand = 'Brand#52' and p_container = 'LG CAN' ;
