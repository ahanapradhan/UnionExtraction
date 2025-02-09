etpch_schema = """
Consider the following schema while formulating the SQL query:

CREATE TABLE nation
(
    n_nationkey  INTEGER not null,
    n_name       CHAR(25) not null,
    n_regionkey  INTEGER not null,
    n_comment    VARCHAR(152)
);

CREATE TABLE region
(
    r_regionkey  INTEGER not null,
    r_name       CHAR(25) not null,
    r_comment    VARCHAR(152)
);

CREATE TABLE part
(
    p_partkey     BIGINT not null,
    p_name        VARCHAR(55) not null,
    p_mfgr        CHAR(25) not null,
    p_brand       CHAR(10) not null,
    p_type        VARCHAR(25) not null,
    p_size        INTEGER not null,
    p_container   CHAR(10) not null,
    p_retailprice DOUBLE PRECISION not null,
    p_comment     VARCHAR(23) not null
);

CREATE TABLE supplier
(
    s_suppkey     BIGINT not null,
    s_name        CHAR(25) not null,
    s_address     VARCHAR(40) not null,
    s_nationkey   INTEGER not null,
    s_phone       CHAR(15) not null,
    s_acctbal     DOUBLE PRECISION not null,
    s_comment     VARCHAR(101) not null
);

CREATE TABLE partsupp
(
    ps_partkey     BIGINT not null,
    ps_suppkey     BIGINT not null,
    ps_availqty    BIGINT not null,
    ps_supplycost  DOUBLE PRECISION  not null,
    ps_comment     VARCHAR(199) not null
);

CREATE TABLE customer
(
    c_custkey     BIGINT not null,
    c_name        VARCHAR(25) not null,
    c_address     VARCHAR(40) not null,
    c_nationkey   INTEGER not null,
    c_phone       CHAR(15) not null,
    c_acctbal     DOUBLE PRECISION   not null,
    c_mktsegment  CHAR(10) not null,
    c_comment     VARCHAR(117) not null
);

CREATE TABLE orders
(
    o_orderkey       BIGINT not null,
    o_custkey        BIGINT not null,
    o_orderstatus    CHAR(1) not null,
    o_totalprice     DOUBLE PRECISION not null,
    o_orderdate      DATE not null,
    o_orderpriority  CHAR(15) not null,  
    o_clerk          CHAR(15) not null, 
    o_shippriority   INTEGER not null,
    o_comment        VARCHAR(79) not null
);

CREATE TABLE web_lineitem
(
    wl_orderkey    BIGINT not null,
    wl_partkey     BIGINT not null,
    wl_suppkey     BIGINT not null,
    wl_linenumber  BIGINT not null,
    wl_quantity    DOUBLE PRECISION not null,
    wl_extendedprice  DOUBLE PRECISION not null,
    wl_discount    DOUBLE PRECISION not null,
    wl_tax         DOUBLE PRECISION not null,
    wl_returnflag  CHAR(1) not null,
    wl_linestatus  CHAR(1) not null,
    wl_shipdate    DATE not null,
    wl_commitdate  DATE not null,
    wl_receiptdate DATE not null,
    wl_shipinstruct CHAR(25) not null,
    wl_shipmode     CHAR(10) not null,
    wl_comment      VARCHAR(44) not null
);

CREATE TABLE store_lineitem
(
    sl_orderkey    BIGINT not null,
    sl_partkey     BIGINT not null,
    sl_suppkey     BIGINT not null,
    sl_linenumber  BIGINT not null,
    sl_quantity    DOUBLE PRECISION not null,
    sl_extendedprice  DOUBLE PRECISION not null,
    sl_discount    DOUBLE PRECISION not null,
    sl_tax         DOUBLE PRECISION not null,
    sl_returnflag  CHAR(1) not null,
    sl_linestatus  CHAR(1) not null,
    sl_shipdate    DATE not null,
    sl_commitdate  DATE not null,
    sl_receiptdate DATE not null,
    sl_shipinstruct CHAR(25) not null,
    sl_shipmode     CHAR(10) not null,
    sl_comment      VARCHAR(44) not null
);

ALTER TABLE PART
  ADD CONSTRAINT part_kpey
     PRIMARY KEY (P_PARTKEY);

ALTER TABLE SUPPLIER
  ADD CONSTRAINT supplier_pkey
     PRIMARY KEY (S_SUPPKEY);

ALTER TABLE PARTSUPP
  ADD CONSTRAINT partsupp_pkey
     PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY);

ALTER TABLE CUSTOMER
  ADD CONSTRAINT customer_pkey
     PRIMARY KEY (C_CUSTKEY);

ALTER TABLE ORDERS
  ADD CONSTRAINT orders_pkey
     PRIMARY KEY (O_ORDERKEY);

ALTER TABLE WEB_LINEITEM
  ADD CONSTRAINT web_lineitem_pkey
     PRIMARY KEY (WL_ORDERKEY, WL_LINENUMBER);
     
ALTER TABLE STORE_LINEITEM
  ADD CONSTRAINT store_lineitem_pkey
     PRIMARY KEY (SL_ORDERKEY, SL_LINENUMBER);

ALTER TABLE NATION
  ADD CONSTRAINT nation_pkey
     PRIMARY KEY (N_NATIONKEY);

ALTER TABLE REGION
  ADD CONSTRAINT region_pkey
     PRIMARY KEY (R_REGIONKEY);

ALTER TABLE SUPPLIER
ADD CONSTRAINT supplier_nation_fkey
   FOREIGN KEY (S_NATIONKEY) REFERENCES NATION(N_NATIONKEY);

   ALTER TABLE PARTSUPP
ADD CONSTRAINT partsupp_part_fkey
   FOREIGN KEY (PS_PARTKEY) REFERENCES PART(P_PARTKEY);
   
   ALTER TABLE PARTSUPP
ADD CONSTRAINT partsupp_supplier_fkey
   FOREIGN KEY (PS_SUPPKEY) REFERENCES SUPPLIER(S_SUPPKEY);

   ALTER TABLE CUSTOMER
ADD CONSTRAINT customer_nation_fkey
   FOREIGN KEY (C_NATIONKEY) REFERENCES NATION(N_NATIONKEY);

   ALTER TABLE ORDERS
ADD CONSTRAINT orders_customer_fkey
   FOREIGN KEY (O_CUSTKEY) REFERENCES CUSTOMER(C_CUSTKEY);

   ALTER TABLE WEB_LINEITEM
ADD CONSTRAINT web_lineitem_orders_fkey
   FOREIGN KEY (WL_ORDERKEY) REFERENCES ORDERS(O_ORDERKEY);

   ALTER TABLE WEB_LINEITEM
ADD CONSTRAINT web_lineitem_partsupp_fkey
   FOREIGN KEY (WL_PARTKEY,WL_SUPPKEY)
    REFERENCES PARTSUPP(PS_PARTKEY,PS_SUPPKEY);
    
    ALTER TABLE STORE_LINEITEM
ADD CONSTRAINT store_lineitem_orders_fkey
   FOREIGN KEY (SL_ORDERKEY) REFERENCES ORDERS(O_ORDERKEY);

   ALTER TABLE STORE_LINEITEM
ADD CONSTRAINT store_lineitem_partsupp_fkey
   FOREIGN KEY (SL_PARTKEY,SL_SUPPKEY)
    REFERENCES PARTSUPP(PS_PARTKEY,PS_SUPPKEY);

   ALTER TABLE NATION
ADD CONSTRAINT nation_region_fkey
   FOREIGN KEY (N_REGIONKEY) REFERENCES REGION(R_REGIONKEY);"""

general_guidelines = """Strictly follow the instructions given below for your SQL output:
1. Strictly use the tables given in the seed query. Do not use any table that is absent in the seed query
2. Do not use redundant join conditions. Do not use CROSS-JOIN.
3. Do not use any predicate with place holder parameter.
4. Do not use window functions, such as RANK() OVER PARTITION BY.
5. Whenever the seed query has projections, strictly re-use their order and aliases.
6. Produce SQL compatible for PostgreSQL Engine.
"""
text_2_sql_question = """Give me SQL for the following text 
(Give only the SQL, do not add any explanation. 
Do not use COALESCE in your SQLs.
Do not use NULLIF in your SQLs.
Put the SQL within Python style comment quotes):"""

seed_query_question = """Refine the following 'seed query' SQL to reach to the final query:"""
refinement_show = "You formulated the following query:"

Q1_text = """
The Query provides a summary pricing report for all lineitems shipped as of a given date.
The date is within 3 days of the greatest ship date contained in the database. The query lists totals for
extended price, discounted extended price, discounted extended price plus tax, average quantity, average extended
price, and average discount. These aggregates are grouped by RETURNFLAG and LINESTATUS, and listed in
ascending order of RETURNFLAG and LINESTATUS. A count of the number of lineitems in each group is
included.  1998-12-01 is the highest possible ship date as defined in the database population.
"""
Q1_seed = """
(select
        wl_returnflag,
        wl_linestatus,
        sum(wl_quantity) as sum_qty,
        sum(wl_extendedprice) as sum_base_price,
        sum(wl_extendedprice * (1 - wl_discount)) as sum_disc_price,
        sum(wl_extendedprice * (1 - wl_discount) * (1 + wl_tax)) as sum_charge,
        avg(wl_quantity) as avg_qty,
        avg(wl_extendedprice) as avg_price,
        avg(wl_discount) as avg_disc,
        count(*) as count_order
from web_lineitem where wl_shipdate <= date '1998-12-01' - interval '3' day
group by
        wl_returnflag,
        wl_linestatus
order by
        wl_returnflag,
        wl_linestatus)
UNION ALL
(select
        sl_returnflag,
        sl_linestatus,
        sum(sl_quantity) as sum_qty,
        sum(sl_extendedprice) as sum_base_price,
        sum(sl_extendedprice * (1 - sl_discount)) as sum_disc_price,
        sum(sl_extendedprice * (1 - sl_discount) * (1 + sl_tax)) as sum_charge,
        avg(sl_quantity) as avg_qty,
        avg(sl_extendedprice) as avg_price,
        avg(sl_discount) as avg_disc,
        count(*) as count_order
from store_lineitem where sl_shipdate <= date '1998-12-01' - interval '3' day
group by
        sl_returnflag,
        sl_linestatus
order by
        sl_returnflag,
        sl_linestatus);
"""
Q1_seed_output = """     
The above seed query gives output as follows:
"A"	"F"	18865717	27356549949.99	25988356900.45	27027321931.296696	25.519179575692974	37004.5151607654	0.05000787256721441	739276
"A"	"F"	18865717	27356549949.99	25988356900.45	27027321931.296696	25.519179575692974	37004.5151607654	0.05000787256721441	739276
"N"	"F"	499596	723782156.17	687811474.1021	715273529.176512	25.562627916496112	37033.4709460704	0.04980607859189521	19544
"N"	"F"	499596	723782156.17	687811474.1021	715273529.176512	25.562627916496112	37033.4709460704	0.04980607859189521	19544
"N"	"O"	38281286	55500406721.32	52724209675.1155	54835316126.84884	25.499760531453646	36969.68489491028	0.05002611173022852	1501241
"N"	"O"	38281286	55500406721.32	52724209675.1155	54835316126.84884	25.499760531453646	36969.68489491028	0.05002611173022852	1501241
"R"	"F"	18872497	27345431033.92	25979800081.9857	27018232810.785515	25.51844400003786	36975.12048861287	0.049998931801617984	739563
"R"	"F"	18872497	27345431033.92	25979800081.9857	27018232810.785515	25.51844400003786	36975.12048861287	0.049998931801617984	739563
"""
Q1_actual_output = """
But my expected output is as follows:
"A"	"F"	37731434	54713099899.98	51976713800.9	54054643862.59339	25.519179575692974	37004.5151607654	0.05000787256721441	1478552
"N"	"F"	999192	1447564312.34	1375622948.2042	1430547058.353024	25.562627916496112	37033.4709460704	0.04980607859189521	39088
"N"	"O"	76562572	111000813442.64	105448419350.231	109670632253.69768	25.499760531453646	36969.68489491028	0.05002611173022852	3002482
"R"	"F"	37744994	54690862067.84	51959600163.9714	54036465621.57103	25.51844400003786	36975.12048861287	0.049998931801617984	1479126

Fix the seed query."""

Q2_text = """"""
Q2_seed = """"""
Q2_seed_output = """Output of the above seed query is as follows:"""
Q2_actual_output = """But the actual output should be as follows:

Fix the seed query."""

Q3_text = """The Query retrieves the shipping priority and potential revenue, defined as the sum of
extendedprice * (1-discount), of the orders having the largest revenue among those that had not been shipped as
of a given date. Orders are listed in decreasing order of revenue. If more than 10 unshipped orders exist, only the 10
orders with the largest revenue are listed.
"""
Q3_seed = """(select
        wl_orderkey,
        sum(wl_extendedprice * (1 - wl_discount)) as revenue,
        o_orderdate,
        o_shippriority
from
        customer,
        orders,
        web_lineitem
where
        c_mktsegment = 'FURNITURE'
        and c_custkey = o_custkey
        and wl_orderkey = o_orderkey
        and o_orderdate < date '1995-01-01'
        and wl_shipdate > date '1995-01-01'
group by
        wl_orderkey,
        o_orderdate,
        o_shippriority
order by
        revenue desc,
        o_orderdate)
UNION ALL
(select
        sl_orderkey,
        sum(sl_extendedprice * (1 - sl_discount)) as revenue,
        o_orderdate,
        o_shippriority
from
        customer,
        orders,
        web_lineitem
where
        c_mktsegment = 'FURNITURE'
        and c_custkey = o_custkey
        and sl_orderkey = o_orderkey
        and o_orderdate < date '1995-01-01'
        and sl_shipdate > date '1995-01-01'
group by
        sl_orderkey,
        o_orderdate,
        o_shippriority
order by
        revenue desc,
        o_orderdate);"""
Q3_seed_output = """The above seed query gives output as follows:
"A"	"F"	18865717	27356549949.99	25988356900.45	27027321931.296696	25.519179575692974	37004.5151607654	0.05000787256721441	739276
"A"	"F"	18865717	27356549949.99	25988356900.45	27027321931.296696	25.519179575692974	37004.5151607654	0.05000787256721441	739276
"N"	"F"	499596	723782156.17	687811474.1021	715273529.176512	25.562627916496112	37033.4709460704	0.04980607859189521	19544
"N"	"F"	499596	723782156.17	687811474.1021	715273529.176512	25.562627916496112	37033.4709460704	0.04980607859189521	19544
"N"	"O"	38281286	55500406721.32	52724209675.1155	54835316126.84884	25.499760531453646	36969.68489491028	0.05002611173022852	1501241
"N"	"O"	38281286	55500406721.32	52724209675.1155	54835316126.84884	25.499760531453646	36969.68489491028	0.05002611173022852	1501241
"R"	"F"	18872497	27345431033.92	25979800081.9857	27018232810.785515	25.51844400003786	36975.12048861287	0.049998931801617984	739563
"R"	"F"	18872497	27345431033.92	25979800081.9857	27018232810.785515	25.51844400003786	36975.12048861287	0.049998931801617984	739563
"""
Q3_actual_output = """But my expected output is as follows:
"A"	"F"	37731434	54713099899.98	51976713800.9	54054643862.59339	25.519179575692974	37004.5151607654	0.05000787256721441	1478552
"N"	"F"	999192	1447564312.34	1375622948.2042	1430547058.353024	25.562627916496112	37033.4709460704	0.04980607859189521	39088
"N"	"O"	76562572	111000813442.64	105448419350.231	109670632253.69768	25.499760531453646	36969.68489491028	0.05002611173022852	3002482
"R"	"F"	37744994	54690862067.84	51959600163.9714	54036465621.57103	25.51844400003786	36975.12048861287	0.049998931801617984	1479126

Fix the seed SQL query."""
Q3_feedback1 = """The seed query looks like to have duplicated groups. 
Keep the UNION ALL operator, but merge the duplicated groups."""

Q4_text = """The Query counts the number of orders ordered in a given quarter of 1995 in which
at least one lineitem was received by the customer later than its committed date. The query lists the count of such
orders for each order priority sorted in ascending priority order.
"""
Q4_seed = """(Select o_orderpriority, Count(*) as order_count 
 From orders, web_lineitem 
 Where orders.o_orderkey = web_lineitem.wl_orderkey
 and web_lineitem.wl_commitdate < web_lineitem.wl_receiptdate
 and orders.o_orderdate between '1995-01-01' and '1995-03-31' 
 Group By o_orderpriority 
 Order By o_orderpriority asc)
 UNION ALL  
 (Select o_orderpriority, Count(*) as order_count 
 From orders, store_lineitem 
 Where orders.o_orderkey = store_lineitem.sl_orderkey
 and store_lineitem.sl_commitdate < store_lineitem.sl_receiptdate
 and orders.o_orderdate between '1995-01-01' and '1995-03-31' 
 Group By o_orderpriority 
 Order By o_orderpriority asc);"""
Q4_seed_output = """"The above seed query gives output as follows:
"1-URGENT       "	14161
"2-HIGH         "	14427
"3-MEDIUM       "	14357
"4-NOT SPECIFIED"	14270
"5-LOW          "	14412
"1-URGENT       "	14161
"2-HIGH         "	14427
"3-MEDIUM       "	14357
"4-NOT SPECIFIED"	14270
"5-LOW          "	14412
"""
Q4_actual_output = """But my expected output is as follows:
"1-URGENT       "	5176
"2-HIGH         "	5311
"3-MEDIUM       "	5164
"4-NOT SPECIFIED"	5182
"5-LOW          "	5210
Fix the seed SQL query.
"""
Q4_feedback1 = """
Do not put any redundant filter predicate. 
All the filter predicates should be as per the seed query.
The second projection values are larger than expected. So it must not be count(*).
Maintain the distinct groups in the first projection.
"""

Q5_text = """The  Query lists for each nation in Asia the revenue volume that resulted from lineitem
transactions in which the customer ordering parts and the supplier filling them were both within that nation. The
query is run in order to determine whether to institute local distribution centers in a given region. The query considers only parts ordered in the year of 1995. The query displays the nations and revenue volume in descending order by
revenue. Revenue volume for all qualifying lineitems in a particular nation is defined as sum(l_extendedprice * (1 -
l_discount))."""
Q5_seed = """(Select n_name, Sum(wl_extendedprice*(1 - wl_discount)) as revenue 
 From customer, nation, orders, region, supplier, web_lineitem 
 Where customer.c_custkey = orders.o_custkey
 and customer.c_nationkey = nation.n_nationkey
 and nation.n_nationkey = supplier.s_nationkey
 and orders.o_orderkey = web_lineitem.wl_orderkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_suppkey = web_lineitem.wl_suppkey
 and region.r_name = 'ASIA'
 and orders.o_orderdate between '1995-01-01' and '1995-12-31' 
 Group By n_name 
 Order By revenue desc, n_name asc)
 UNION ALL  
 (Select n_name, Sum(sl_extendedprice*(1 - sl_discount)) as revenue 
 From customer, nation, orders, region, store_lineitem, supplier 
 Where orders.o_orderkey = store_lineitem.sl_orderkey
 and store_lineitem.sl_suppkey = supplier.s_suppkey
 and customer.c_custkey = orders.o_custkey
 and customer.c_nationkey = nation.n_nationkey
 and nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and region.r_name = 'ASIA'
 and orders.o_orderdate between '1995-01-01' and '1995-12-31' 
 Group By n_name 
 Order By revenue desc, n_name asc); """
Q5_actual_output = """But the expected output is as follows:
"CHINA                    "	114024399.8524
"INDIA                    "	105857316.5416
"INDONESIA                "	103946938.2060
"VIETNAM                  "	102155527.2612
"JAPAN                    "	90082333.7968

Fix the seed query."""
Q5_seed_output = """The above seed query gives the following output:

"CHINA                    "	57012199.9262
"INDIA                    "	52928658.2708
"INDONESIA                "	51973469.1030
"VIETNAM                  "	51077763.6306
"JAPAN                    "	45041166.8984
"CHINA                    "	57012199.9262
"INDIA                    "	52928658.2708
"INDONESIA                "	51973469.1030
"VIETNAM                  "	51077763.6306
"JAPAN                    "	45041166.8984
"""

Q6_text = """The Query considers all the line items shipped in year 1993 and 1994, with discounts between
0.06-0.01 and 0.06+0.01. The query lists the amount by which the total revenue would have
increased if these discounts had been eliminated for line items with l_quantity less than 24. Note that the
potential revenue increase is equal to the sum of [extendedprice * discount] for all line items with discounts and
quantities in the qualifying range."""
Q6_seed = """select sum(wl_extendedprice*wl_discount) as revenue
        from web_lineitem 
        where wl_shipdate >= date '1993-01-01'
        and wl_shipdate < date '1994-12-31'
        and wl_discount between 0.05 and 0.07
        and wl_quantity < 24
        UNION ALL
        select sum(sl_extendedprice*sl_discount) as revenue
        from store_lineitem 
        where sl_shipdate >= date '1993-01-01'
        and sl_shipdate < date '1994-12-31'
        and sl_discount between 0.05 and 0.07
        and sl_quantity < 24;"""
Q6_actual_output = """The expected output is as follows:
4035401223.9508. 

It is a single aggregated value. So the final query should have one aggregate function in the projection."""
Q6_seed_output = """The above seed query gives the following output: 
1866473627.9654
1866473627.9654.

Fix the seed query SQL. """

Q14_text = """The Query determines what percentage of the revenue in a given year and month was derived from
promotional parts. The query considers only parts actually shipped in that month and gives the percentage. Revenue
is defined as (extended price * (1-discount))."""
Q14_seed = """(Select Sum(0) as promo_revenue 
 From part, store_lineitem 
 Where part.p_partkey = store_lineitem.sl_partkey
 and store_lineitem.sl_shipdate between '1995-01-01' and '1995-01-31')
 UNION ALL  
 (Select Sum(0) as promo_revenue 
 From part, web_lineitem 
 Where part.p_partkey = web_lineitem.wl_partkey
 and web_lineitem.wl_shipdate between '1995-01-01' and '1995-01-31');  """
Q14_seed_output = """Output of the above seed query is as follows:
0
0"""
Q14_actual_output = """
But the actual output should be:
16.9227056452702565

The actual query gives a single aggregated value.
Fix the seed sql query.
"""
Q14_feedback1 = """
The query produced by you gives output as follows:

16.92270564527025649000
16.92270564527025649000

Fix the query.
Use all the filter predicates of the seed query. Do not use any other filter.
The actual query gives a single aggregated value.
So maybe, put the aggregation after the union.
"""

Q7_text = """The Query finds, for two given nations, the gross discounted revenues derived from line items in
which parts were shipped from a supplier in either nation to a customer in the other nation during 1995 and 1996.
The query lists the supplier nation, the customer nation, the year, and the revenue from shipments that took place in
that year. The query orders the answer by Supplier nation, Customer nation, and year (all ascending).
"""
Q7_seed = """(Select n1.n_name as supp_nation, n2.n_name as cust_nation, wl_shipdate as l_year, wl_extendedprice*(1 - wl_discount) as revenue 
 From customer, nation n1, nation n2, orders, supplier, web_lineitem 
 Where orders.o_orderkey = web_lineitem.wl_orderkey
 and supplier.s_suppkey = web_lineitem.wl_suppkey
 and customer.c_custkey = orders.o_custkey
 and customer.c_nationkey = n2.n_nationkey
 and n1.n_nationkey = supplier.s_nationkey
 and (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') OR (n2.n_name = 'FRANCE' and n1.n_name = 'GERMANY')
 and web_lineitem.wl_shipdate between '1995-01-01' and '1996-12-31')
 UNION ALL  
 (Select n1.n_name as supp_nation, n2.n_name as cust_nation, sl_shipdate as l_year, sl_extendedprice*(1 - sl_discount) as revenue 
 From customer, nation n1, nation n2, orders, supplier, store_lineitem 
 Where orders.o_orderkey = store_lineitem.sl_orderkey
 and supplier.s_suppkey = store_lineitem.sl_suppkey
 and customer.c_custkey = orders.o_custkey
 and customer.c_nationkey = n2.n_nationkey
 and n1.n_nationkey = supplier.s_nationkey
 and (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') OR (n2.n_name = 'FRANCE' and n1.n_name = 'GERMANY')
 and store_lineitem.sl_shipdate between '1995-01-01' and '1996-12-31'); 
;"""
Q7_seed_output = """Output of the above seed query is not known as it is taking more than 4 minutes to execute.
"""
Q7_actual_output = """But the actual output should be as follows, which was produced in just 4 seconds:
"FRANCE"	"GERMANY"	1995	9274470.3002
"FRANCE"	"GERMANY"	1996	10449559.1472
"GERMANY"	"FRANCE"	1995	12465637.4074
"GERMANY"	"FRANCE"	1996	11114624.2242

Fix the seed query."""
Q7_feedback1 = """The output of your current query is as follows:
"FRANCE"	"GERMANY"	1995	4637235.1501
"FRANCE"	"GERMANY"	1995	4637235.1501
"FRANCE"	"GERMANY"	1996	5224779.5736
"FRANCE"	"GERMANY"	1996	5224779.5736
"GERMANY"	"FRANCE"	1995	6232818.7037
"GERMANY"	"FRANCE"	1995	6232818.7037
"GERMANY"	"FRANCE"	1996	5557312.1121
"GERMANY"	"FRANCE"	1996	5557312.1121

Looks like it has duplicated groups. If group by clause is present within union subqueries, try putting it out."""

Q8_text = """"""
Q8_seed = """"""
Q8_seed_output = """Output of the above seed query is as follows:"""
Q8_actual_output = """But the actual output should be as follows:

Fix the seed query."""

Q9_text = """"""
Q9_seed = """"""
Q9_seed_output = """Output of the above seed query is as follows:"""
Q9_actual_output = """But the actual output should be as follows:

Fix the seed query."""

Q21_text = """The query identifies suppliers, for nation 'ARGENTINA', whose product was part of a
multi-supplier online order (with current status of 'F') where they were the 
only supplier who failed to meet the committed delivery date."""

Q21_seed = """Select s_name, <unknown> as numwait 
 From web_lineitem l1, web_lineitem l2, nation, orders, supplier 
 Where l1.wl_orderkey = l2.wl_orderkey
 and l2.wl_orderkey = o_orderkey
 and l1.wl_suppkey = s_suppkey
 and n_nationkey = s_nationkey
 and l1.wl_commitdate < l1.wl_receiptdate
 and n_name = 'ARGENTINA'
 and o_orderstatus = 'F' 
 Order By s_name asc; """

Q21_seed_output = """The seed query has the second projection unidentified. 
The actual output of the query shows it an integer number, in decreasing order.
Validate the predicates in the seed query against the text description and refine them.
"""
Q21_actual_output = """The actual output should be as follows:

"Supplier#000000985       "	18
"Supplier#000000521       "	17
"Supplier#000000748       "	17
"Supplier#000001110       "	17
"Supplier#000001771       "	17
"Supplier#000001823       "	17
"Supplier#000002320       "	17
"Supplier#000003512       "	17
"Supplier#000002122       "	16
"Supplier#000002928       "	16
"Supplier#000004227       "	16
"Supplier#000002686       "	15
"Supplier#000004503       "	15
"Supplier#000004550       "	15
"Supplier#000000544       "	14
"Supplier#000000721       "	14
"Supplier#000001573       "	14
"Supplier#000001991       "	14
"Supplier#000002057       "	14
"Supplier#000004004       "	14
"Supplier#000004198       "	14
"Supplier#000004581       "	14
"Supplier#000004640       "	14
"Supplier#000000849       "	13
"Supplier#000001902       "	13
"Supplier#000002745       "	13
"Supplier#000002883       "	13
"Supplier#000002886       "	13
"Supplier#000002977       "	13
"Supplier#000002982       "	13
"Supplier#000003311       "	13
"Supplier#000003321       "	13
"Supplier#000003404       "	13
"Supplier#000003426       "	13
"Supplier#000003636       "	13
"Supplier#000004350       "	13
"Supplier#000004385       "	13
"Supplier#000004605       "	13
"Supplier#000000071       "	12
"Supplier#000000567       "	12
"Supplier#000000678       "	12
"Supplier#000000714       "	12
"Supplier#000001402       "	12
"Supplier#000001544       "	12
"Supplier#000002017       "	12
"Supplier#000002429       "	12
"Supplier#000003210       "	12
"Supplier#000003453       "	12
"Supplier#000003495       "	12
"Supplier#000003813       "	12
"Supplier#000004627       "	12
"Supplier#000004798       "	12
"Supplier#000000186       "	11
"Supplier#000000485       "	11
"Supplier#000000624       "	11
"Supplier#000000730       "	11
"Supplier#000000868       "	11
"Supplier#000000945       "	11
"Supplier#000000950       "	11
"Supplier#000001084       "	11
"Supplier#000001270       "	11
"Supplier#000001280       "	11
"Supplier#000002143       "	11
"Supplier#000002519       "	11
"Supplier#000002547       "	11
"Supplier#000002612       "	11
"Supplier#000003174       "	11
"Supplier#000003351       "	11
"Supplier#000003581       "	11
"Supplier#000003789       "	11
"Supplier#000003791       "	11
"Supplier#000004050       "	11
"Supplier#000004080       "	11
"Supplier#000004210       "	11
"Supplier#000004309       "	11
"Supplier#000004573       "	11
"Supplier#000004745       "	11
"Supplier#000000836       "	10
"Supplier#000001186       "	10
"Supplier#000001360       "	10
"Supplier#000001810       "	10
"Supplier#000001811       "	10
"Supplier#000002174       "	10
"Supplier#000002250       "	10
"Supplier#000002291       "	10
"Supplier#000002734       "	10
"Supplier#000002808       "	10
"Supplier#000002957       "	10
"Supplier#000003006       "	10
"Supplier#000003065       "	10
"Supplier#000003483       "	10
"Supplier#000003743       "	10
"Supplier#000003859       "	10
"Supplier#000004213       "	10
"Supplier#000004248       "	10
"Supplier#000004277       "	10
"Supplier#000004344       "	10
"Supplier#000004434       "	10
"Supplier#000004593       "	10
"Supplier#000004841       "	10
"Supplier#000004909       "	10
"Supplier#000000297       "	9
"Supplier#000000430       "	9
"Supplier#000000852       "	9
"Supplier#000000873       "	9
"Supplier#000000886       "	9
"Supplier#000001020       "	9
"Supplier#000001076       "	9
"Supplier#000001136       "	9
"Supplier#000001957       "	9
"Supplier#000002052       "	9
"Supplier#000002107       "	9
"Supplier#000002111       "	9
"Supplier#000002359       "	9
"Supplier#000002502       "	9
"Supplier#000002733       "	9
"Supplier#000002821       "	9
"Supplier#000002843       "	9
"Supplier#000003005       "	9
"Supplier#000003209       "	9
"Supplier#000003559       "	9
"Supplier#000003916       "	9
"Supplier#000004053       "	9
"Supplier#000004189       "	9
"Supplier#000004252       "	9
"Supplier#000004359       "	9
"Supplier#000004777       "	9
"Supplier#000004803       "	9
"Supplier#000004892       "	9
"Supplier#000000029       "	8
"Supplier#000000127       "	8
"Supplier#000000244       "	8
"Supplier#000000336       "	8
"Supplier#000000792       "	8
"Supplier#000000989       "	8
"Supplier#000001224       "	8
"Supplier#000001596       "	8
"Supplier#000001647       "	8
"Supplier#000001965       "	8
"Supplier#000002199       "	8
"Supplier#000002515       "	8
"Supplier#000002880       "	8
"Supplier#000003010       "	8
"Supplier#000003194       "	8
"Supplier#000003601       "	8
"Supplier#000003787       "	8
"Supplier#000004301       "	8
"Supplier#000004457       "	8
"Supplier#000004756       "	8
"Supplier#000004881       "	8
"Supplier#000000003       "	7
"Supplier#000000230       "	7
"Supplier#000000518       "	7
"Supplier#000000539       "	7
"Supplier#000001533       "	7
"Supplier#000001734       "	7
"Supplier#000001854       "	7
"Supplier#000001884       "	7
"Supplier#000001936       "	7
"Supplier#000002463       "	7
"Supplier#000002803       "	7
"Supplier#000003510       "	7
"Supplier#000003535       "	7
"Supplier#000003570       "	7
"Supplier#000003661       "	7
"Supplier#000003816       "	7
"Supplier#000004214       "	7
"Supplier#000004282       "	7
"Supplier#000004762       "	7
"Supplier#000004863       "	7
"Supplier#000000725       "	6
"Supplier#000000801       "	6
"Supplier#000001213       "	6
"Supplier#000001743       "	6
"Supplier#000002058       "	6
"Supplier#000002128       "	6
"Supplier#000002332       "	6
"Supplier#000002633       "	6
"Supplier#000002762       "	6
"Supplier#000002899       "	6
"Supplier#000003119       "	6
"Supplier#000003790       "	6
"Supplier#000003841       "	6
"Supplier#000004466       "	6
"Supplier#000004646       "	6
"Supplier#000001660       "	5
"Supplier#000001918       "	5
"Supplier#000004407       "	5
"Supplier#000004773       "	5
"Supplier#000004834       "	5
"Supplier#000000184       "	4
"Supplier#000000363       "	4
"Supplier#000001124       "	4
"Supplier#000002493       "	4
"Supplier#000003554       "	4
"Supplier#000003804       "	4
"Supplier#000004584       "	4
"Supplier#000000203       "	3
"Supplier#000001509       "	3
"Supplier#000002316       "	3
"Supplier#000002491       "	3

Fix the seed query."""

Q21_feedback1 = """
You produced the following query:
SELECT s_name, COUNT(*) AS numfailures
FROM supplier, nation, web_lineitem, orders
WHERE s_suppkey = wl_suppkey
  AND o_orderkey = wl_orderkey
  AND s_nationkey = n_nationkey
  AND n_name = 'ARGENTINA'
  AND o_orderstatus = 'F'
  AND wl_receiptdate > wl_commitdate
  AND NOT EXISTS (
    SELECT 1
    FROM web_lineitem l2
    WHERE l2.wl_orderkey = wl_orderkey
      AND l2.wl_suppkey <> wl_suppkey
      AND l2.wl_receiptdate > l2.wl_commitdate
  )
GROUP BY s_name
ORDER BY numfailures DESC, s_name;


It gives the following output:

"Supplier#000000521       "	217
"Supplier#000004605       "	217
"Supplier#000004004       "	210
"Supplier#000004252       "	210
"Supplier#000004466       "	210
"Supplier#000000985       "	209
"Supplier#000004762       "	209
"Supplier#000001402       "	206
"Supplier#000001743       "	206
"Supplier#000001136       "	205
"Supplier#000004756       "	204
"Supplier#000001810       "	203
"Supplier#000001811       "	203
"Supplier#000002745       "	203
"Supplier#000003661       "	201
"Supplier#000002429       "	200
"Supplier#000002547       "	200
"Supplier#000002843       "	200
"Supplier#000002320       "	199
"Supplier#000003559       "	199
"Supplier#000004309       "	199
"Supplier#000000336       "	198
"Supplier#000000518       "	198
"Supplier#000001918       "	198
"Supplier#000003426       "	198
"Supplier#000000624       "	197
"Supplier#000002803       "	197
"Supplier#000004581       "	197
"Supplier#000000127       "	196
"Supplier#000000873       "	196
"Supplier#000002734       "	196
"Supplier#000003512       "	196
"Supplier#000000721       "	195
"Supplier#000000849       "	195
"Supplier#000002316       "	195
"Supplier#000003816       "	195
"Supplier#000004301       "	195
"Supplier#000004909       "	195
"Supplier#000000003       "	194
"Supplier#000002143       "	194
"Supplier#000003804       "	194
"Supplier#000004550       "	194
"Supplier#000002111       "	192
"Supplier#000002612       "	192
"Supplier#000002883       "	192
"Supplier#000003065       "	192
"Supplier#000003601       "	192
"Supplier#000004640       "	192
"Supplier#000001884       "	191
"Supplier#000002502       "	191
"Supplier#000000363       "	190
"Supplier#000000792       "	190
"Supplier#000000950       "	190
"Supplier#000001573       "	190
"Supplier#000003209       "	190
"Supplier#000003311       "	190
"Supplier#000003404       "	190
"Supplier#000003841       "	190
"Supplier#000004385       "	190
"Supplier#000000029       "	189
"Supplier#000000297       "	189
"Supplier#000001270       "	189
"Supplier#000003510       "	189
"Supplier#000004210       "	189
"Supplier#000004627       "	189
"Supplier#000001647       "	188
"Supplier#000001734       "	188
"Supplier#000001902       "	188
"Supplier#000002519       "	188
"Supplier#000002733       "	188
"Supplier#000003006       "	188
"Supplier#000003636       "	188
"Supplier#000001084       "	187
"Supplier#000002686       "	187
"Supplier#000003791       "	187
"Supplier#000001110       "	186
"Supplier#000001124       "	186
"Supplier#000002017       "	186
"Supplier#000002122       "	186
"Supplier#000002291       "	186
"Supplier#000003321       "	186
"Supplier#000003743       "	186
"Supplier#000003010       "	185
"Supplier#000003174       "	185
"Supplier#000003581       "	185
"Supplier#000003790       "	185
"Supplier#000003813       "	185
"Supplier#000004227       "	185
"Supplier#000000230       "	184
"Supplier#000000485       "	184
"Supplier#000002928       "	184
"Supplier#000004050       "	184
"Supplier#000000186       "	183
"Supplier#000001280       "	183
"Supplier#000001360       "	183
"Supplier#000003554       "	183
"Supplier#000004080       "	183
"Supplier#000004277       "	183
"Supplier#000004881       "	183
"Supplier#000000748       "	182
"Supplier#000001957       "	182
"Supplier#000002128       "	182
"Supplier#000003916       "	182
"Supplier#000002052       "	181
"Supplier#000002058       "	181
"Supplier#000002515       "	181
"Supplier#000003495       "	181
"Supplier#000003570       "	181
"Supplier#000004053       "	181
"Supplier#000000184       "	180
"Supplier#000001965       "	180
"Supplier#000002332       "	180
"Supplier#000002899       "	180
"Supplier#000003194       "	180
"Supplier#000002463       "	179
"Supplier#000003859       "	179
"Supplier#000004584       "	179
"Supplier#000004745       "	179
"Supplier#000004773       "	179
"Supplier#000001224       "	178
"Supplier#000001509       "	178
"Supplier#000001771       "	178
"Supplier#000002107       "	178
"Supplier#000002977       "	178
"Supplier#000004798       "	178
"Supplier#000004803       "	178
"Supplier#000000678       "	177
"Supplier#000001854       "	177
"Supplier#000002821       "	177
"Supplier#000002982       "	177
"Supplier#000002250       "	176
"Supplier#000000886       "	175
"Supplier#000001076       "	175
"Supplier#000001213       "	175
"Supplier#000002493       "	175
"Supplier#000002057       "	174
"Supplier#000002174       "	174
"Supplier#000002808       "	174
"Supplier#000003453       "	174
"Supplier#000004593       "	174
"Supplier#000004646       "	174
"Supplier#000004777       "	174
"Supplier#000004841       "	174
"Supplier#000000244       "	173
"Supplier#000000725       "	173
"Supplier#000002886       "	173
"Supplier#000003005       "	173
"Supplier#000003351       "	173
"Supplier#000003483       "	173
"Supplier#000003789       "	173
"Supplier#000004350       "	173
"Supplier#000004359       "	173
"Supplier#000004503       "	173
"Supplier#000000714       "	172
"Supplier#000001544       "	172
"Supplier#000004407       "	172
"Supplier#000000071       "	171
"Supplier#000000852       "	171
"Supplier#000001186       "	171
"Supplier#000001936       "	171
"Supplier#000004248       "	171
"Supplier#000004457       "	171
"Supplier#000004573       "	171
"Supplier#000004892       "	171
"Supplier#000000868       "	170
"Supplier#000002633       "	170
"Supplier#000002957       "	170
"Supplier#000003210       "	170
"Supplier#000000801       "	169
"Supplier#000001020       "	169
"Supplier#000001533       "	169
"Supplier#000004282       "	169
"Supplier#000002199       "	168
"Supplier#000002491       "	168
"Supplier#000003535       "	168
"Supplier#000004189       "	168
"Supplier#000004434       "	168
"Supplier#000002762       "	167
"Supplier#000004344       "	166
"Supplier#000004863       "	165
"Supplier#000000544       "	163
"Supplier#000001660       "	163
"Supplier#000004834       "	163
"Supplier#000000836       "	162
"Supplier#000001823       "	162
"Supplier#000003119       "	162
"Supplier#000002880       "	161
"Supplier#000000730       "	160
"Supplier#000000945       "	160
"Supplier#000003787       "	160
"Supplier#000004198       "	160
"Supplier#000000203       "	159
"Supplier#000000430       "	158
"Supplier#000000989       "	158
"Supplier#000002359       "	158
"Supplier#000000539       "	157
"Supplier#000004214       "	157
"Supplier#000001596       "	152
"Supplier#000001991       "	148
"Supplier#000004213       "	141
"Supplier#000000567       "	139

Result is not matching. 
Validate all the predicates against the text again and fix the query.
Consider 'multi-supplier' order with emphasis.
Use alias for each instance of web_lineitem table.
The second projection values are more than expected."""

Q21_feedback2 = """
Use alias for each instance of web_lineitem table.
Do not reproduce the same incorrect query that you produced before.
Last query produced by you was:
SELECT s_name, COUNT(*) AS numfailures
FROM supplier, nation, web_lineitem wl1, orders
WHERE s_suppkey = wl1.wl_suppkey
  AND o_orderkey = wl1.wl_orderkey
  AND s_nationkey = n_nationkey
  AND n_name = 'ARGENTINA'
  AND o_orderstatus = 'F'
  AND wl1.wl_receiptdate > wl1.wl_commitdate
  AND NOT EXISTS (
    SELECT 1
    FROM web_lineitem wl2
    WHERE wl2.wl_orderkey = wl1.wl_orderkey
      AND wl2.wl_suppkey <> wl1.wl_suppkey
      AND wl2.wl_receiptdate <= wl2.wl_commitdate
  )
GROUP BY s_name
ORDER BY numfailures DESC, s_name;


Result is still not matching. 
The second projection values are more than expected.
numwait values are more. See below, which is the current result.

"Supplier#000002686       "	57
"Supplier#000001810       "	56
"Supplier#000004301       "	56
"Supplier#000000003       "	55
"Supplier#000004762       "	55
"Supplier#000000985       "	53
"Supplier#000004466       "	53
"Supplier#000000297       "	52
"Supplier#000001076       "	52
"Supplier#000001224       "	52
"Supplier#000002320       "	52
"Supplier#000002612       "	52
"Supplier#000000521       "	51
"Supplier#000000873       "	51
"Supplier#000001124       "	51
"Supplier#000001811       "	51
"Supplier#000001854       "	51
"Supplier#000002429       "	51
"Supplier#000003791       "	51
"Supplier#000004210       "	51
"Supplier#000001136       "	50
"Supplier#000001270       "	50
"Supplier#000001509       "	50
"Supplier#000001884       "	50
"Supplier#000002547       "	50
"Supplier#000004227       "	50
"Supplier#000003426       "	49
"Supplier#000003661       "	49
"Supplier#000004004       "	49
"Supplier#000004773       "	49
"Supplier#000000801       "	48
"Supplier#000001020       "	48
"Supplier#000001360       "	48
"Supplier#000001647       "	48
"Supplier#000002491       "	48
"Supplier#000004627       "	48
"Supplier#000000945       "	47
"Supplier#000001902       "	47
"Supplier#000002057       "	47
"Supplier#000002982       "	47
"Supplier#000003010       "	47
"Supplier#000004550       "	47
"Supplier#000004646       "	47
"Supplier#000001402       "	46
"Supplier#000002803       "	46
"Supplier#000002843       "	46
"Supplier#000003119       "	46
"Supplier#000003601       "	46
"Supplier#000003841       "	46
"Supplier#000003859       "	46
"Supplier#000004605       "	46
"Supplier#000004756       "	46
"Supplier#000000230       "	45
"Supplier#000000485       "	45
"Supplier#000001533       "	45
"Supplier#000001743       "	45
"Supplier#000001918       "	45
"Supplier#000002128       "	45
"Supplier#000002515       "	45
"Supplier#000003535       "	45
"Supplier#000003559       "	45
"Supplier#000003570       "	45
"Supplier#000004050       "	45
"Supplier#000004248       "	45
"Supplier#000004863       "	45
"Supplier#000000336       "	44
"Supplier#000000886       "	44
"Supplier#000001186       "	44
"Supplier#000003065       "	44
"Supplier#000003510       "	44
"Supplier#000003581       "	44
"Supplier#000004841       "	44
"Supplier#000000186       "	43
"Supplier#000001084       "	43
"Supplier#000002502       "	43
"Supplier#000002745       "	43
"Supplier#000002808       "	43
"Supplier#000003483       "	43
"Supplier#000004252       "	43
"Supplier#000004309       "	43
"Supplier#000004385       "	43
"Supplier#000004573       "	43
"Supplier#000004909       "	43
"Supplier#000000518       "	42
"Supplier#000000714       "	42
"Supplier#000000730       "	42
"Supplier#000000792       "	42
"Supplier#000001660       "	42
"Supplier#000001957       "	42
"Supplier#000002017       "	42
"Supplier#000002122       "	42
"Supplier#000002519       "	42
"Supplier#000002880       "	42
"Supplier#000002899       "	42
"Supplier#000003209       "	42
"Supplier#000003404       "	42
"Supplier#000003453       "	42
"Supplier#000003512       "	42
"Supplier#000003554       "	42
"Supplier#000003804       "	42
"Supplier#000004407       "	42
"Supplier#000004457       "	42
"Supplier#000004581       "	42
"Supplier#000004777       "	42
"Supplier#000004803       "	42
"Supplier#000001734       "	41
"Supplier#000002199       "	41
"Supplier#000002332       "	41
"Supplier#000002733       "	41
"Supplier#000002977       "	41
"Supplier#000003005       "	41
"Supplier#000003636       "	41
"Supplier#000003816       "	41
"Supplier#000004640       "	41
"Supplier#000000849       "	40
"Supplier#000000852       "	40
"Supplier#000002058       "	40
"Supplier#000002143       "	40
"Supplier#000002316       "	40
"Supplier#000002633       "	40
"Supplier#000003006       "	40
"Supplier#000003194       "	40
"Supplier#000003495       "	40
"Supplier#000004053       "	40
"Supplier#000004434       "	40
"Supplier#000000244       "	39
"Supplier#000000748       "	39
"Supplier#000001280       "	39
"Supplier#000001573       "	39
"Supplier#000001936       "	39
"Supplier#000002250       "	39
"Supplier#000002734       "	39
"Supplier#000002957       "	39
"Supplier#000003351       "	39
"Supplier#000003787       "	39
"Supplier#000003916       "	39
"Supplier#000004359       "	39
"Supplier#000004881       "	39
"Supplier#000000721       "	38
"Supplier#000000868       "	38
"Supplier#000000950       "	38
"Supplier#000003174       "	38
"Supplier#000004213       "	38
"Supplier#000004344       "	38
"Supplier#000004798       "	38
"Supplier#000000127       "	37
"Supplier#000000544       "	37
"Supplier#000000678       "	37
"Supplier#000000836       "	37
"Supplier#000001965       "	37
"Supplier#000002107       "	37
"Supplier#000002463       "	37
"Supplier#000002493       "	37
"Supplier#000002821       "	37
"Supplier#000003210       "	37
"Supplier#000003311       "	37
"Supplier#000004282       "	37
"Supplier#000004584       "	37
"Supplier#000000029       "	36
"Supplier#000000363       "	36
"Supplier#000002111       "	36
"Supplier#000003321       "	36
"Supplier#000003789       "	36
"Supplier#000003790       "	36
"Supplier#000004080       "	36
"Supplier#000004593       "	36
"Supplier#000004892       "	36
"Supplier#000000430       "	35
"Supplier#000000539       "	35
"Supplier#000000624       "	35
"Supplier#000002052       "	35
"Supplier#000002291       "	35
"Supplier#000002762       "	35
"Supplier#000004350       "	35
"Supplier#000004503       "	35
"Supplier#000000071       "	34
"Supplier#000000725       "	34
"Supplier#000001544       "	34
"Supplier#000003743       "	34
"Supplier#000004745       "	34
"Supplier#000000184       "	33
"Supplier#000001213       "	33
"Supplier#000001596       "	33
"Supplier#000002883       "	33
"Supplier#000003813       "	33
"Supplier#000004277       "	33
"Supplier#000000203       "	32
"Supplier#000001110       "	32
"Supplier#000002174       "	32
"Supplier#000002886       "	32
"Supplier#000004189       "	32
"Supplier#000001823       "	31
"Supplier#000002928       "	31
"Supplier#000002359       "	30
"Supplier#000004198       "	30
"Supplier#000004834       "	30
"Supplier#000004214       "	29
"Supplier#000000989       "	28
"Supplier#000001771       "	27
"Supplier#000000567       "	26
"Supplier#000001991       "	26
Check again whether all the text requirements are captured in the query.
"""

