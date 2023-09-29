Select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
From part, partsupp, supplier, nation, region
Where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and r_regionkey = n_regionkey and p_type LIKE '%TIN' and p_size = 38 and r_name  = 'MIDDLE EAST'
Order By s_acctbal desc, n_name asc, s_name asc
Limit 100;