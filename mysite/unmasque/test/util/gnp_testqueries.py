###'''FILTER Predicate''' ___ Checking Aggregation too
# Checking Neg extremes of int and numeric using integers for > operator
Q1 = '''select p_partkey, p_name, ps_availqty, 
        from part, partsupp 
        where p_partkey = ps_partkey and 
              p_partkey > -2147483648 and p_size > -2147483647 and
              ps_suppkey > -2147483646 and p_retailprice > -2147483648 and
              ps_supplycost > -2147483647'''
# Checking Neg extremes of int and numeric using integers for < operator
Q2 = '''select p_partkey, p_name, ps_availqty, 
        from part, partsupp 
        where p_partkey = ps_partkey and 
              p_partkey < 2147483647 and p_size < 2147483646 and
              ps_suppkey < 2147483645 and p_retailprice < 2147483647 and
              ps_supplycost < 2147483646'''
# Checking Neg extremes of int and numeric using integers for >= operator
Q3 = '''select p_partkey, p_name, ps_availqty, 
        from part, partsupp 
        where p_partkey = ps_partkey and 
              p_partkey >= -2147483648 and p_size >= -2147483647 and
              ps_suppkey >= -2147483646 and p_retailprice >= -2147483648 and
              ps_supplycost >= -2147483647'''
# Checking Neg extremes of int and numeric using integers for <= operator
Q4 = '''select p_partkey, p_name, ps_availqty, 
        from part, partsupp 
        where p_partkey = ps_partkey and 
              p_partkey <= 2147483647 and p_size <= 2147483646 and
              ps_suppkey <= 2147483645 and p_retailprice <= 2147483647 and
              ps_supplycost <= 2147483646'''
# Checking extremes of numeric and checking between for integers.
Q5 = '''select p_partkey, p_name, ps_availqty, 
        from part, partsupp 
        where p_partkey = ps_partkey and 
              p_partkey between -2147483647 and 2147483647 and 
              p_size between -214746 and 354564 and ps_suppkey between 0 and 3211132
              and p_retailprice <= 2147483647.85 and ps_supplycost <= 2147483646.99'''
# Checking between for numerics.
Q6 = '''select p_partkey, p_name, ps_availqty, 
        from part, partsupp 
        where p_partkey = ps_partkey and 
              p_retailprice between 2147483647.85 and -2147483647.85 and 
              ps_supplycost between 10 and 564541'''
# Checking Projection, Aggregation with constraints over attribute.
Q7 = '''SELECT c_name, avg(c_acctbal) as max_balance, o_clerk 
        FROM customer, orders 
        where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' 
        and o_orderdate <= DATE '1995-10-23' and 
        c_acctbal > 0 and c_acctbal < 1000
        group by c_name, o_clerk 
        order by c_name, o_clerk desc;'''
Q8 = '''SELECT c_name, avg(c_acctbal) as max_balance, o_clerk 
        FROM customer, orders 
        where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' 
        and o_orderdate <= DATE '1995-10-23' and 
        c_acctbal > 0 and c_acctbal < 1
        group by c_name, o_clerk 
        order by c_name, o_clerk desc;'''

Q9 = '''SELECT c_name, avg(c_acctbal) as max_balance, o_clerk 
        FROM customer, orders 
        where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' 
        and o_orderdate <= DATE '1995-10-23' and 
        c_acctbal > 2 and c_acctbal < 3
        group by c_name, o_clerk 
        order by c_name, o_clerk desc;'''

Q10 = '''SELECT c_name, avg(2.24*c_acctbal + 5.48*o_totalprice + 325.6) as max_balance, o_clerk 
        FROM customer, orders 
        where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' 
        and o_orderdate <= DATE '1995-10-23' and 
        c_acctbal > 0 and c_acctbal < 30.04
        group by c_name, o_clerk 
        order by c_name, o_clerk desc;'''

Q11 = '''SELECT c_name, avg(c_acctbal +  o_totalprice) as rolex,o_clerk 
         FROM customer, orders 
         WHERE c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' 
        and o_orderdate <= DATE '1995-10-23' and c_acctbal > 30.04 
        group by c_name, o_clerk order by c_name, o_clerk desc;'''
