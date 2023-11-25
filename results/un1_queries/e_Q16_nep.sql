Select p_brand, p_type, p_size, Count(*) as supplier_cnt
From part, partsupp
Where p_partkey = ps_partkey and p_type LIKE 'SMALL PLATED%' and p_size  >= 4 and p_brand <> 'Brand#45' 
Group By p_brand, p_type, p_size
Order By p_brand asc, p_type asc, p_size asc;