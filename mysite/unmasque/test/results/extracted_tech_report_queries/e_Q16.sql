Select p_brand, p_type, p_size, Count(*) as supplier_cnt
 From part, partsupp 
 Where part.p_partkey = partsupp.ps_partkey
 and part.p_size  >= 4 
 Group By p_brand, p_type, p_size 
 Order By supplier_cnt desc, p_brand asc, p_type asc, p_size asc;