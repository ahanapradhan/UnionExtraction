Select s_acctbal, s_name, n_name, ps_partkey as p_partkey, p_mfgr, s_address, s_phone, s_comment
From partsupp, region, supplier, part, nation
Where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and r_regionkey = n_regionkey and r_name  = 'MIDDLE EAST' and p_type LIKE '%TIN' and p_size = 38
Order By s_acctbal desc, n_name asc, s_name asc
Limit 100;