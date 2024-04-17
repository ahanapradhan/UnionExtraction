Select s_name, count(*) as numwait From supplier, lineitem l1, orders, nation  
      Where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and  
      s_nationkey = n_nationkey and n_name = 'GERMANY'  
      Group By s_name Order by numwait desc, s_name Limit 100;
