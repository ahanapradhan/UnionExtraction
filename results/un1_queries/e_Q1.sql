Select l_returnflag, l_linestatus, Sum(l_quantity) as sum_qty, Sum(l_extendedprice) as sum_base_price, Sum(l_extendedprice-1.0*l_extendedprice*l_discount) as sum_disc_price, Sum(l_extendedprice-1.0*l_extendedprice*l_discount+l_extendedprice*l_tax-1.0*l_extendedprice*l_discount*l_tax) as sum_charge, Avg(l_quantity) as avg_qty, Avg(l_extendedprice) as avg_price, Avg(l_discount) as avg_disc, Count(*) as count_order
From lineitem
Where l_shipdate  <= '1998-09-21'
Group By l_returnflag, l_linestatus
Order By l_returnflag asc, l_linestatus asc;