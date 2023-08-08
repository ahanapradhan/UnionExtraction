tpch_query1 = "select count(*) as count_order from lineitem where l_shipdate <= date '1998-12-01' - interval ':1' " \
              "day group by l_returnflag, l_linestatus;"

tpch_query3 = "select c_mktsegment, l_orderkey, sum(l_extendedprice) as revenue, " \
              "o_orderdate, o_shippriority from customer, orders, lineitem where c_custkey = o_custkey " \
              "and l_orderkey = o_orderkey and o_orderdate > date '1995-10-11' " \
              "group by l_orderkey, o_orderdate, o_shippriority, c_mktsegment limit 4;"

Q1 = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, " \
     "sum(l_discount) as sum_disc_price, sum(l_tax) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) " \
     "as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date " \
     "'1998-12-01' - interval '71 days' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;"
Q2 = "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part, supplier, " \
     "partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 38 and p_type " \
     "like '%TIN' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST' order by " \
     "s_acctbal desc, n_name, s_name limit 100;"
Q3 = "select l_orderkey as orderkey, sum(l_discount) as revenue, o_orderdate as orderdate, o_shippriority as " \
     "shippriority from customer, orders, " \
     "lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate " \
     "< '1995-03-15' and l_shipdate > '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue " \
     "desc, o_orderdate limit 10;"
Q3_1 = "select l_orderkey as orderkey, sum(l_extendedprice * (1-l_discount)) as revenue, o_orderdate as orderdate, " \
       "o_shippriority as " \
       "shippriority from customer, orders, " \
       "lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate " \
       "< '1995-03-15' and l_shipdate > '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue " \
       "desc, o_orderdate, l_orderkey limit 10;"

Q4 = "Select o_orderdate, o_orderpriority, count(*) as order_count From orders Where o_orderdate >= date '1997-07-01' " \
     "and o_orderdate < date '1997-07-01' + interval '3' month Group By o_orderdate, o_orderpriority Order By " \
     "o_orderpriority Limit 10;"
Q5 = "Select  n_name, sum(l_extendedprice) as revenue From  customer, orders, lineitem, supplier, nation, " \
     "region Where  c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = " \
     "s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST' and " \
     "o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '1' year Group By n_name Order " \
     "By  revenue desc Limit  100;"
Q6 = "Select  l_shipmode, sum(l_extendedprice) as revenue From  lineitem Where  l_shipdate >= date '1994-01-01' and " \
     "l_shipdate < date '1994-01-01' + interval '1' year and l_quantity < 24 Group By  l_shipmode Limit  100;"
Q7 = "Select  c_name, sum(l_extendedprice) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment From  " \
     "customer, orders, lineitem, nation Where  c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= " \
     "date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '3' month and l_returnflag = 'R' and " \
     "c_nationkey = n_nationkey Group By  c_name, c_acctbal, c_phone, n_name, c_address, c_comment Order By  revenue " \
     "desc Limit  20;"
Q11 = "Select  ps_COMMENT, sum(ps_availqty) as value From  partsupp, supplier, nation Where  ps_suppkey = s_suppkey " \
      "and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By  ps_COMMENT Order By  value desc Limit  100;"
Q16 = "Select  p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt From  partsupp, part Where  p_partkey = " \
      "ps_partkey and p_type like 'SMALL PLATED%' and p_size >= 4 Group By  p_brand, p_type, p_size Order By  " \
      "supplier_cnt desc, p_brand, p_type, p_size;"

Q17 = "Select  AVG(l_extendedprice) as avgTOTAL From  lineitem, part Where  p_partkey = l_partkey and p_brand = " \
      "'Brand#52' and p_container = 'LG CAN';"
Q18 = "Select  p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt From  partsupp, part Where  p_partkey = " \
      "ps_partkey and p_type like 'SMALL PLATED%' and p_size >= 4 Group By  p_brand, p_type, p_size Order By  " \
      "supplier_cnt desc, p_brand, p_type, p_size;"
Q21 = "Select  s_name, count(*) as numwait From  supplier, lineitem l1, orders, nation Where  s_suppkey = " \
      "l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and s_nationkey = n_nationkey and n_name = " \
      "'GERMANY' Group By  s_name Order By  numwait desc, s_name Limit  100;"
Q23_1 = "Select  min(ps_supplycost) From  partsupp, supplier, nation, region Where  s_suppkey = ps_suppkey and " \
        "s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST';"

Q18_test = "Select  p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt From  partsupp, part Where  p_partkey = " \
           "ps_partkey and p_size >= 4 Group By  p_brand, p_type, p_size Order By  " \
           "supplier_cnt desc, p_brand, p_type, p_size;"

Q18_test1 = "Select  p_retailprice, p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt From  partsupp, " \
            "part Where  p_partkey = ps_partkey and p_retailprice < 1000 and p_retailprice > 800 and p_size >= 4 " \
            "Group By p_retailprice, p_brand, p_type, p_size Order By  supplier_cnt desc, p_brand, p_type, p_size;"

Q9_simple = "select n_name as nation, o_orderdate as o_year, " \
            "l_quantity as amount from part, supplier,lineitem,partsupp,orders,nation " \
            "where s_suppkey = l_suppkey and ps_suppkey = l_suppkey " \
            "and ps_partkey = l_partkey and p_partkey = l_partkey " \
            "and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%green%';"

Q10_simple = "select c_custkey, c_name, sum(l_extendedprice) as revenue, " \
             "c_acctbal,n_name,c_address,c_phone,c_comment " \
             "from customer,orders,lineitem,nation " \
             "where c_custkey = o_custkey and l_orderkey = o_orderkey " \
             "and o_orderdate >= '1993-10-01' and o_orderdate < '1994-01-01' " \
             "and l_returnflag = 'R' and c_nationkey = n_nationkey group by " \
             "c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment;"

queries_dict = {'tpch_query1': tpch_query1,
                'tpch_query3': tpch_query3,
                'Q1': Q1,
                'Q3': Q3,
                'Q4': Q4,
                'Q5': Q5,
                'Q6': Q6,
                'Q7': Q7,
                'Q11': Q11,
                'Q16': Q16,
                'Q17': Q17,
                'Q18': Q18,
                'Q21': Q21,
                'Q23_1': Q23_1,
                'Q9_simple': Q9_simple,
                'Q10_simple': Q10_simple
                }
'''
(select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905 limit 1)
union
all(select
l_orderkey as key
from lineitem, orders
where
l_orderkey = o_orderkey and o_totalprice <= 905 limit 1) union
all(select
o_orderkey as key
from customer, orders
where
c_custkey = o_custkey and o_totalprice <= 890 limit 1);

TPCDS Q14: INTERSECT, UNION ALL
Q38: INTERSECT

'''
