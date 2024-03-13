Select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
 From nation, part, partsupp, region, supplier 
 Where p_partkey = ps_partkey
 and ps_suppkey = s_suppkey
 and n_nationkey = s_nationkey
 and n_regionkey = r_regionkey
 and p_size = 38
 and r_name = 'MIDDLE EAST'
 and p_type LIKE '%TIN'
 and p_size  >= 38 and p_size <= 38 
 Limit 100;