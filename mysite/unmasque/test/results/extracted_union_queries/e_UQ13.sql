Select o_orderkey as l_orderkey, ps_availqty as l_linenumber
 From lineitem, orders, partsupp 
 Where l_partkey = ps_partkey
 and l_suppkey = ps_suppkey
 and l_linenumber = ps_availqty
 and l_orderkey = o_orderkey
 and o_orderdate <= l_shipdate
 and l_shipdate <= l_commitdate
 and l_commitdate <= l_receiptdate
 and '1990-01-01' <= o_orderdate
 and '1994-01-02' <= l_receiptdate 
 Order By l_orderkey asc 
 Limit 7;