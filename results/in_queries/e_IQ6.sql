(Select p_container, p_retailprice, ps_availqty
From part, partsupp, supplier
Where ps_suppkey = s_suppkey and p_partkey = ps_partkey and p_brand  = 'Brand#15')
 INTERSECT 
(Select p_container, p_retailprice, ps_availqty
From part, partsupp, supplier
Where ps_suppkey = s_suppkey and p_partkey = ps_partkey and p_brand  = 'Brand#45');