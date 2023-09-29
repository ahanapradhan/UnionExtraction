Select ps_comment, Sum(ps_availqty) as value
From supplier, nation, partsupp
Where s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_name  = 'ARGENTINA'
Group By ps_comment
Order By value desc, ps_comment asc
Limit 100;