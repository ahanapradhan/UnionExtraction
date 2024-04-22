Select c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address,  
      c_phone, c_comment From customer, orders, lineitem, nation  
      Where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date '1994-01-01'  
      and o_orderdate < date '1994-01-01' + interval '3' month and l_returnflag = 'R' and c_nationkey = n_nationkey  
      Group By c_name, c_acctbal, c_phone, n_name, c_address, c_comment Order by revenue desc Limit 20;
