# '''FILTER Predicate''' ___ Checking Aggregation too

# Checking Neg extremes of int and numeric using integers for > operator
GQ1 = '''select p_partkey, p_name, ps_availqty, 
        from part, partsupp 
        where p_partkey = ps_partkey and 
              p_partkey > -2147483648 and p_size > -2147483647 and
              ps_suppkey > -2147483646 and p_retailprice > -2147483648 and
              ps_supplycost > -2147483647'''

# Checking Neg extremes of int and numeric using integers for < operator
GQ2 = '''select p_partkey, p_name, ps_availqty, 
        from part, partsupp 
        where p_partkey = ps_partkey and 
              p_partkey < 2147483647 and p_size < 2147483646 and
              ps_suppkey < 2147483645 and p_retailprice < 2147483647 and
              ps_supplycost < 2147483646'''

# Checking Neg extremes of int and numeric using integers for >= operator
GQ3 = '''select p_partkey, p_name, ps_availqty, 
        from part, partsupp 
        where p_partkey = ps_partkey and 
              p_partkey >= -2147483648 and p_size >= -2147483647 and
              ps_suppkey >= -2147483646 and p_retailprice >= -2147483648 and
              ps_supplycost >= -2147483647'''

# Checking Neg extremes of int and numeric using integers for <= operator
GQ4 = '''select p_partkey, p_name, ps_availqty, 
        from part, partsupp 
        where p_partkey = ps_partkey and 
              p_partkey <= 2147483647 and p_size <= 2147483646 and
              ps_suppkey <= 2147483645 and p_retailprice <= 2147483647 and
              ps_supplycost <= 2147483646'''

# Checking extremes of numeric and checking between for integers.
GQ5 = '''select p_partkey, p_name, ps_availqty, 
        from part, partsupp 
        where p_partkey = ps_partkey and 
              p_partkey between -2147483647 and 2147483647 and 
              p_size between -2147483646 and 2147483646 and ps_suppkey between 0 and 3211132
              and p_retailprice <= 2147483647.85 and ps_supplycost <= 2147483646.99'''

# Checking between for numerics.
GQ6 = '''select p_partkey, p_name, ps_availqty, 
        from part, partsupp 
        where p_partkey = ps_partkey and 
              p_retailprice between 2147483647.85 and -2147483647.85 and 
              ps_supplycost between 10 and 564541'''

# Checking Projection, Aggregation with constraints over attribute.
GQ7 = '''SELECT c_name, avg(c_acctbal) as max_balance, o_clerk 
        FROM customer, orders 
        where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' 
        and o_orderdate <= DATE '1995-10-23' and 
        c_acctbal > 0 and c_acctbal < 1000
        group by c_name, o_clerk 
        order by c_name, o_clerk desc;'''

GQ8 = '''SELECT c_name, avg(c_acctbal) as max_balance, o_clerk 
        FROM customer, orders 
        where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' 
        and o_orderdate <= DATE '1995-10-23' and 
        c_acctbal > 0 and c_acctbal < 1
        group by c_name, o_clerk 
        order by c_name, o_clerk desc;'''

# Used to give Avg(51*c_acctbal - 102).
GQ9 = '''SELECT c_name, avg(c_acctbal) as max_balance, o_clerk 
        FROM customer, orders 
        where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' 
        and o_orderdate <= DATE '1995-10-23' and 
        c_acctbal > 2 and c_acctbal < 3
        group by c_name, o_clerk 
        order by c_name, o_clerk desc;'''

# Used to give "Floating point division by 0" error.
GQ10 = '''SELECT c_name, avg(c_acctbal) as max_balance, o_clerk 
        FROM customer, orders 
        where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' 
        and o_orderdate <= DATE '1995-10-23' and c_acctbal > 0
        group by c_name, o_clerk 
        order by c_name, o_clerk desc;'''

# Filter sometimes give 30.06 in output.
GQ11 = '''SELECT c_name, avg(c_acctbal) as rolex,o_clerk 
        FROM customer, orders 
        WHERE c_custkey = o_custkey and o_orderdate > DATE '1993-10-14'
        and o_orderdate <= DATE '1995-10-23' and c_acctbal > 30.04
        group by c_name, o_clerk 
        order by c_name, o_clerk desc;'''

# a, b, c, d values are always integers.
GQ12 = '''SELECT c_name, avg(2.24*c_acctbal + 5.48*o_totalprice + 325.6) as max_balance, o_clerk 
        FROM customer, orders 
        where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' 
        and o_orderdate <= DATE '1995-10-23' and 
        c_acctbal > 30.04;'''

# Constant in projection.
GQ13 = '''SELECT 7
        FROM customer, orders
        where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14'
        and o_orderdate <= DATE '1995-10-23' and
        c_acctbal > 0 and c_acctbal < 30.04;'''

# Constant and count in projection.
GQ14 = '''SELECT count(o_clerk), 7
        FROM customer, orders
        where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14'
        and o_orderdate <= DATE '1995-10-23' and
        c_acctbal > 0 and c_acctbal < 30.04;'''

# Constant = 1 and count in projection.
GQ15 = '''SELECT count(o_clerk), 1
        FROM customer, orders
        where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14'
        and o_orderdate <= DATE '1995-10-23' and
        c_acctbal > 0 and c_acctbal < 30.04;'''

# Constant string in projection.
GQ16 = '''Select p_brand as rolex, p_type as chaka, p_size as napa, count(ps_suppkey) as
            supplier_cnt
        From partsupp, part Where p_partkey = ps_partkey and p_brand = 'Brand#45' and p_type Like
        'SMALL PLATED%' and p_size >= 4 
        Group By p_brand, p_type, p_size 
        Order by supplier_cnt desc, p_brand, p_type, p_size;'''

# Constant Number in projection.
GQ17 = '''SELECT c_name, avg(c_acctbal) as max_balance,o_clerk 
        FROM customer, orders 
        where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14'
        and o_orderdate <= DATE '1995-10-23' and c_acctbal = 121.65
        group by c_name, o_clerk 
        order by c_name, o_clerk desc;'''

# Constant Date in projection.
GQ18 = '''SELECT c_name, c_acctbal as max_balance,o_orderdate 
        FROM customer, orders 
        where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14'
        and o_orderdate = DATE '1995-10-23' and c_acctbal = 121.65;'''

# Constant = 1, 7 and count in projection.
GQ19 = '''SELECT c_name, count(o_clerk), 7, 1
        FROM customer, orders
        where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14'
        and o_orderdate <= DATE '1995-10-23' and
        c_acctbal > 0 and c_acctbal < 30.04
        group by c_name;'''

# Group by attribute doesn't come in projection.
GQ20 = '''SELECT c_name, count(o_clerk), 7
        FROM customer, orders
        where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14'
        and o_orderdate <= DATE '1995-10-23' and
        c_acctbal > 0 and c_acctbal < 30.04
        group by c_name, o_clerk;'''  # group by is not coming fine.

# Sequencing of attributes in group by and order by clause. (1.2.3)
GQ21 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by l_orderkey, o_orderdate, o_shippriority 
        order by revenue desc, o_orderdate, l_orderkey limit 10;'''

GQ22 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by l_orderkey, o_orderdate, o_shippriority 
        order by revenue desc, l_orderkey, o_orderdate limit 10;'''

GQ23 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by l_orderkey, o_orderdate, o_shippriority 
        order by o_orderdate, revenue desc, l_orderkey limit 10;'''

GQ24 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by l_orderkey, o_orderdate, o_shippriority 
        order by o_orderdate, l_orderkey, revenue desc limit 10;'''

GQ25 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by l_orderkey, o_orderdate, o_shippriority 
        order by l_orderkey, revenue desc, o_orderdate,  limit 10;'''

GQ26 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by l_orderkey, o_orderdate, o_shippriority 
        order by l_orderkey, o_orderdate, revenue desc limit 10;'''

# Sequencing of attributes in group by and order by clause. (1.3.2)
GQ27 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by l_orderkey, o_shippriority, o_orderdate
        order by revenue desc, o_orderdate, l_orderkey limit 10;'''

GQ28 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by l_orderkey, o_shippriority, o_orderdate
        order by revenue desc, l_orderkey, o_orderdate limit 10;'''

GQ29 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by l_orderkey, o_shippriority, o_orderdate
        order by o_orderdate, revenue desc, l_orderkey limit 10;'''

GQ30 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by l_orderkey, o_shippriority, o_orderdate
        order by o_orderdate, l_orderkey, revenue desc limit 10;'''

GQ31 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by l_orderkey, o_shippriority, o_orderdate
        order by l_orderkey, revenue desc, o_orderdate,  limit 10;'''

GQ32 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by l_orderkey, o_shippriority, o_orderdate
        order by l_orderkey, o_orderdate, revenue desc limit 10;'''

# Sequencing of attributes in group by and order by clause. (2.1.3)
GQ33 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_orderdate, l_orderkey, o_shippriority 
        order by revenue desc, o_orderdate, l_orderkey limit 10;'''

GQ34 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_orderdate, l_orderkey, o_shippriority
        order by revenue desc, l_orderkey, o_orderdate limit 10;'''

GQ35 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_orderdate, l_orderkey, o_shippriority
        order by o_orderdate, revenue desc, l_orderkey limit 10;'''

GQ36 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_orderdate, l_orderkey, o_shippriority 
        order by o_orderdate, l_orderkey, revenue desc limit 10;'''

GQ37 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_orderdate, l_orderkey, o_shippriority 
        order by l_orderkey, revenue desc, o_orderdate,  limit 10;'''

GQ38 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_orderdate, l_orderkey, o_shippriority 
        order by l_orderkey, o_orderdate, revenue desc limit 10;'''

# Sequencing of attributes in group by and order by clause. (2.3.1)
GQ39 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_orderdate, o_shippriority, l_orderkey
        order by revenue desc, o_orderdate, l_orderkey limit 10;'''

GQ40 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_orderdate, o_shippriority, l_orderkey
        order by revenue desc, l_orderkey, o_orderdate limit 10;'''

GQ41 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_orderdate, o_shippriority, l_orderkey
        order by o_orderdate, revenue desc, l_orderkey limit 10;'''

GQ42 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_orderdate, o_shippriority, l_orderkey
        order by o_orderdate, l_orderkey, revenue desc limit 10;'''

GQ43 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_orderdate, o_shippriority, l_orderkey 
        order by l_orderkey, revenue desc, o_orderdate,  limit 10;'''

GQ44 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_orderdate, o_shippriority, l_orderkey
        order by l_orderkey, o_orderdate, revenue desc limit 10;'''

# Sequencing of attributes in group by and order by clause. (3.1.2)
GQ45 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_shippriority, l_orderkey, o_orderdate
        order by revenue desc, o_orderdate, l_orderkey limit 10;'''

GQ46 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_shippriority, l_orderkey, o_orderdate
        order by revenue desc, l_orderkey, o_orderdate limit 10;'''

GQ47 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_shippriority, l_orderkey, o_orderdate
        order by o_orderdate, revenue desc, l_orderkey limit 10;'''

GQ48 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_shippriority, l_orderkey, o_orderdate 
        order by o_orderdate, l_orderkey, revenue desc limit 10;'''

GQ49 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_shippriority, l_orderkey, o_orderdate
        order by l_orderkey, revenue desc, o_orderdate,  limit 10;'''

GQ50 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_shippriority, l_orderkey, o_orderdate
        order by l_orderkey, o_orderdate, revenue desc limit 10;'''

# Sequencing of attributes in group by and order by clause. (3.2.1)
GQ51 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_shippriority, o_orderdate, l_orderkey
        order by revenue desc, o_orderdate, l_orderkey limit 10;'''

GQ52 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_shippriority, o_orderdate, l_orderkey
        order by revenue desc, l_orderkey, o_orderdate limit 10;'''

GQ53 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_shippriority, o_orderdate, l_orderkey
        order by o_orderdate, revenue desc, l_orderkey limit 10;'''

GQ54 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_shippriority, o_orderdate, l_orderkey
        order by o_orderdate, l_orderkey, revenue desc limit 10;'''

GQ55 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_shippriority, o_orderdate, l_orderkey
        order by l_orderkey, revenue desc, o_orderdate,  limit 10;'''

GQ56 = '''select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, 
            o_shippriority  
        from customer, orders, lineitem 
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  < '1995-03-15' and l_shipdate > '1995-03-15' 
        group by o_shippriority, o_orderdate, l_orderkey
        order by l_orderkey, o_orderdate, revenue desc limit 10;'''

# Only count queries.
GQ57 = '''select count(p_mfgr) 
        from part, partsupp 
        where p_partkey = ps_partkey and p_brand = 'Brand#33' and p_retailprice > 700  
        and p_size > 3 and ps_suppkey <= 1000;'''

GQ58 = '''SELECT count(c_name) as rolex, count(o_clerk) as viola
        FROM customer, orders
        where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14'
        and o_orderdate <= DATE '1995-10-23' and
        c_acctbal > 0 and c_acctbal < 30.04;'''










