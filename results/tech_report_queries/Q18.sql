Select c_name, o_orderdate, o_totalprice,  sum(l_quantity)
From customer, orders, lineitem
      Where c_phone Like '27-_%'
      and c_custkey = o_custkey
      and o_orderkey = l_orderkey
      Group By c_name, o_orderdate, o_totalprice
      Order by o_orderdate, o_totalprice desc Limit 100;
