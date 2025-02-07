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
Put the SQL within Python style comment quotes):"""

seed_query_question = """Refine the following 'seed query' SQL to reach to the final query:"""

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

Fix the seed query."""

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