Select s_name, count(*) as numwait
From supplier, lineitem, orders, nation
Where s_suppkey = l_suppkey and o_orderkey = l_orderkey
and o_orderstatus = 'F' and
l_receiptdate >= l_commitdate and s_nationkey = n_nationkey
Group By s_name
Order By numwait desc
Limit 100;