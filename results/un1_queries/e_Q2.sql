Select s_acctbal, s_name, n_name, ps_partkey as p_partkey, p_mfgr, s_address, s_phone, s_comment
From nation, part, partsupp, region, supplier
Where p_partkey = ps_partkey and ps_suppkey = s_suppkey and n_nationkey = s_nationkey and n_regionkey = r_regionkey and p_type LIKE '%TIN' and p_size = 38 and r_name  = 'MIDDLE EAST'
Order By s_acctbal desc, n_name asc, s_name asc, p_partkey asc
Limit 100;