Select s_acctbal, s_name, n_name, ps_partkey as p_partkey, p_mfgr, s_address, s_phone, s_comment
 From nation, part, partsupp, region, supplier 
 Where part.p_partkey = partsupp.ps_partkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and part.p_size = 38
 and region.r_name = 'MIDDLE EAST'
 and part.p_type LIKE '%TIN' 
 Order By s_acctbal desc, n_name asc, s_name asc, p_partkey asc 
 Limit 100;