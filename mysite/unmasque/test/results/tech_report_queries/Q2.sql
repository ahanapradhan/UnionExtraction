Select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment  
     From part, supplier, partsupp, nation, region  
     Where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 38 and p_type like  
     '%TIN' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST'  
     Order by s_acctbal desc, n_name, s_name, p_partkey Limit 100;