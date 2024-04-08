Select ps_comment, Sum(ps_availqty*ps_supplycost) as value
 From nation, partsupp, supplier 
 Where partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_nationkey = supplier.s_nationkey
 and nation.n_name = 'ARGENTINA' 
 Group By ps_comment 
 Order By value desc, ps_comment asc 
 Limit 100;