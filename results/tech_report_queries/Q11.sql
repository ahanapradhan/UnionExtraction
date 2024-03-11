Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation  
      Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT  
      Order by value desc Limit 100;