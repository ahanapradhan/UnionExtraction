Q1 = "Select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as " \
     "sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice *" \
     "(1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as " \
     "avg_price, avg(l_discount) as avg_disc, count(*) as count_order " \
     "From lineitem Where l_shipdate <= date '1998-12-01' - interval '71 days' " \
     "Group By l_returnflag, l_linestatus Order by l_returnflag, l_linestatus;"

Q2 = "Select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment " \
     "From part, supplier, partsupp, nation, region " \
     "Where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 38 and p_type like " \
     "'%TIN' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST' " \
     "Order by s_acctbal desc, n_name, s_name, p_partkey Limit 100;"

Q3 = "Select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority " \
     "From customer, orders, lineitem " \
     "Where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and " \
     "o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15' " \
     "Group By l_orderkey, o_orderdate, o_shippriority " \
     "Order by revenue desc, o_orderdate Limit 10;"

Q3_1 = "Select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority " \
       "From customer, orders, lineitem " \
       "Where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and " \
       "o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15' " \
       "Group By l_orderkey, o_shippriority, o_orderdate " \
       "Order by revenue desc, o_orderdate Limit 10;"

Q4 = "Select o_orderdate, o_orderpriority, count(*) as order_count " \
     "From orders " \
     "Where o_orderdate >= date '1997-07-01' and o_orderdate < date '1997-07-01' + interval '3' month " \
     "Group By o_orderdate, o_orderpriority Order by o_orderpriority Limit 10;"

Q5 = "Select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue " \
     "From customer, orders, lineitem, supplier, nation, region " \
     "Where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and " \
     "c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and " \
     "r_name = 'MIDDLE EAST' and o_orderdate >= date '1994-01-01' and o_orderdate < date " \
     "'1994-01-01' + interval '1' year " \
     "Group By n_name " \
     "Order by revenue desc Limit 100;"

Q6 = "Select l_shipmode, sum(l_extendedprice * l_discount) as revenue From lineitem " \
     "Where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and " \
     "l_quantity < 24 Group By l_shipmode Limit 100;"

Q10 = "Select c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address, " \
      "c_phone, c_comment From customer, orders, lineitem, nation " \
      "Where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date '1994-01-01' " \
      "and o_orderdate < date '1994-01-01' + interval '3' month and l_returnflag = 'R' and c_nationkey = n_nationkey " \
      "Group By c_name, c_acctbal, c_phone, n_name, c_address, c_comment Order by revenue desc Limit 20;"

Q11 = "Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation " \
      "Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT " \
      "Order by value desc Limit 100;"

Q16 = "Select p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt From partsupp, part " \
      "Where p_partkey = ps_partkey and p_brand = 'Brand#45' and p_type Like 'SMALL PLATED%' and p_size >= 4 " \
      "Group By p_brand, p_type, p_size Order by supplier_cnt desc, p_brand, p_type, p_size;"

Q16_nep = "Select p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt From partsupp, part " \
          "Where p_partkey = ps_partkey and p_brand <> 'Brand#45' and p_type Like 'SMALL PLATED%' and p_size >= 4 " \
          "Group By p_brand, p_type, p_size Order by supplier_cnt desc, p_brand, p_type, p_size;"

Q16_nep_2 = "Select p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt From partsupp, part " \
            "Where p_partkey = ps_partkey and p_brand <> 'Brand#45' and p_type NOT Like 'SMALL PLATED%' and p_size >= " \
            "4 Group By p_brand, p_type, p_size Order by supplier_cnt desc, p_brand, p_type, p_size;"

Q17 = "Select AVG(l_extendedprice) as avgTOTAL From lineitem, part " \
      "Where p_partkey = l_partkey and p_brand = 'Brand#52' and p_container = 'LG CAN' ;"

Q18 = "Select c_name, o_orderdate, o_totalprice, sum(l_quantity) From customer, orders, lineitem " \
      "Where c_phone Like '27-_%' and c_custkey = o_custkey and o_orderkey = l_orderkey " \
      "Group By c_name, o_orderdate, o_totalprice Order by o_orderdate, o_totalprice desc Limit 100;"

Q21 = "Select s_name, count(*) as numwait From supplier, lineitem l1, orders, nation " \
      "Where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and " \
      "s_nationkey = n_nationkey and n_name = 'GERMANY' " \
      "Group By s_name Order by numwait desc, s_name Limit 100;"

Q_r = "select c_mktsegment as segment from customer,nation,orders where " \
      "c_acctbal between 1000 and 5000 and c_nationkey = n_nationkey and c_custkey = o_custkey " \
      "and n_name not LIKE 'B%';"

Q_dt = "select n_comment from nation, region where n_name = 'BRAZIL';"
