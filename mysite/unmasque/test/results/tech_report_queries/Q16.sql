Select p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt From partsupp, part  
            Where p_partkey = ps_partkey and p_brand <> 'Brand#45' and p_type NOT Like 'SMALL PLATED%' and p_size >=  
            4 Group By p_brand, p_type, p_size Order by supplier_cnt desc, p_brand, p_type, p_size;
