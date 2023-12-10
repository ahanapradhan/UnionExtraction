Select p_brand, p_type, p_size, Count(*) as supplier_cnt
From part, partsupp
Where p_partkey = ps_partkey and p_brand  = 'Brand#45' and p_type LIKE 'SMALL PLATED%' and p_size  >= 4
Group By p_type, p_size, p_brand
Order By p_type asc, p_size asc;