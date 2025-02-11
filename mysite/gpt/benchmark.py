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
1. Strictly use the tables given in the seed query. Do not use any table that is absent in the seed query.
2. Do not use redundant join conditions. Do not use CROSS-JOIN.
3. Do not use any predicate with place holder parameter.
4. Do not use window functions, such as RANK() OVER PARTITION BY, and functions such as NULLIF, COALESCE.
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

example_of_seed_refinement = """Here is an example of seed query refinement for you:

text: select how many orders were placed online per city in 1995 January. 
City is defined by last 5 characters of customer's address.

seed query: 
select count(*), c_address as city 
from orders, customers, web_lineitem
where o_orderdate between DATE '1995-01-01' and DATE '1995-01-31
and c_custkey = o_custkey
and o_orderkey = wl_orderkey'
group by c_address;

refined query:
select count(*), RIGHT(c_address,5) as city 
from orders, customers, web_lineitem
where o_orderdate between DATE '1995-01-01' and DATE '1995-01-31
and c_custkey = o_custkey
and o_orderkey = wl_orderkey'
group by RIGHT(c_address,5);

The following instance is an incorrect seed refinement because it does not strictly reuse the tables of the seed query:
select count(*), RIGHT(c_address,5) as city 
from orders, customers
where YEAR(o_orderdate) = 1995
and MONTH(o_orderdate) = 1
and c_custkey = o_custkey
group by RIGHT(c_address,5);
"""

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

Q2_text = """The Query finds, in Europe, for each part made of Brass and of size 15, the supplier who
can supply it at minimum cost. If several European suppliers offer the desired part type and size at the same
(minimum) cost, the query lists the parts from suppliers with the 100 highest account balances. For each supplier,
the query lists the supplier's account balance, name and nation; the part's number and manufacturer; the supplier's
address, phone number and comment information.
"""
Q2_seed = """Select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment 
 From nation, part, partsupp, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and part.p_partkey = partsupp.ps_partkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and part.p_size = 15
 and region.r_name = 'EUROPE'
 and part.p_type LIKE '%BRASS' 
 Order By s_acctbal desc, n_name asc, s_name asc, p_partkey asc
Limit 100;   """
Q2_seed_output = """Output of the above seed query is as follows:
9984.69	"Supplier#000008875       "	"ROMANIA                  "	13872	"Manufacturer#3           "	"hRdOqKqyU,sHq"	"29-132-904-4395"	"ong the bold pinto beans are furiously blithely slow"
9984.69	"Supplier#000008875       "	"ROMANIA                  "	13872	"Manufacturer#3           "	"hRdOqKqyU,sHq"	"29-132-904-4395"	"ong the bold pinto beans are furiously blithely slow"
9955.05	"Supplier#000008810       "	"UNITED KINGDOM           "	73795	"Manufacturer#2           "	",Ot93zDXOFSjWSKsKrT7XJ4YPCP,A"	"33-527-478-5988"	"ily according to the carefully express pinto beans. unusual requests use quickly carefully s"
9955.05	"Supplier#000008810       "	"UNITED KINGDOM           "	73795	"Manufacturer#2           "	",Ot93zDXOFSjWSKsKrT7XJ4YPCP,A"	"33-527-478-5988"	"ily according to the carefully express pinto beans. unusual requests use quickly carefully s"
9955.05	"Supplier#000008810       "	"UNITED KINGDOM           "	81285	"Manufacturer#2           "	",Ot93zDXOFSjWSKsKrT7XJ4YPCP,A"	"33-527-478-5988"	"ily according to the carefully express pinto beans. unusual requests use quickly carefully s"
9955.05	"Supplier#000008810       "	"UNITED KINGDOM           "	81285	"Manufacturer#2           "	",Ot93zDXOFSjWSKsKrT7XJ4YPCP,A"	"33-527-478-5988"	"ily according to the carefully express pinto beans. unusual requests use quickly carefully s"
9938.53	"Supplier#000005359       "	"UNITED KINGDOM           "	185358	"Manufacturer#4           "	"QKuHYh,vZGiwu2FWEJoLDx04"	"33-429-790-6131"	"uriously regular requests hag"
9938.53	"Supplier#000005359       "	"UNITED KINGDOM           "	185358	"Manufacturer#4           "	"QKuHYh,vZGiwu2FWEJoLDx04"	"33-429-790-6131"	"uriously regular requests hag"
9937.84	"Supplier#000005969       "	"ROMANIA                  "	108438	"Manufacturer#1           "	"ANDENSOSmk,miq23Xfb5RWt6dvUcvt6Qa"	"29-520-692-3537"	"efully express instructions. regular requests against the slyly fin"
9937.84	"Supplier#000005969       "	"ROMANIA                  "	108438	"Manufacturer#1           "	"ANDENSOSmk,miq23Xfb5RWt6dvUcvt6Qa"	"29-520-692-3537"	"efully express instructions. regular requests against the slyly fin"
9936.22	"Supplier#000005250       "	"UNITED KINGDOM           "	249	"Manufacturer#4           "	"B3rqp0xbSEim4Mpy2RH J"	"33-320-228-2957"	"etect about the furiously final accounts. slyly ironic pinto beans sleep inside the furiously"
9936.22	"Supplier#000005250       "	"UNITED KINGDOM           "	249	"Manufacturer#4           "	"B3rqp0xbSEim4Mpy2RH J"	"33-320-228-2957"	"etect about the furiously final accounts. slyly ironic pinto beans sleep inside the furiously"
9923.77	"Supplier#000002324       "	"GERMANY                  "	29821	"Manufacturer#4           "	"y3OD9UywSTOk"	"17-779-299-1839"	"ackages boost blithely. blithely regular deposits c"
9923.77	"Supplier#000002324       "	"GERMANY                  "	29821	"Manufacturer#4           "	"y3OD9UywSTOk"	"17-779-299-1839"	"ackages boost blithely. blithely regular deposits c"
9871.22	"Supplier#000006373       "	"GERMANY                  "	43868	"Manufacturer#5           "	"J8fcXWsTqM"	"17-813-485-8637"	"etect blithely bold asymptotes. fluffily ironic platelets wake furiously; blit"
9871.22	"Supplier#000006373       "	"GERMANY                  "	43868	"Manufacturer#5           "	"J8fcXWsTqM"	"17-813-485-8637"	"etect blithely bold asymptotes. fluffily ironic platelets wake furiously; blit"
9870.78	"Supplier#000001286       "	"GERMANY                  "	81285	"Manufacturer#2           "	"YKA,E2fjiVd7eUrzp2Ef8j1QxGo2DFnosaTEH"	"17-516-924-4574"	" regular accounts. furiously unusual courts above the fi"
9870.78	"Supplier#000001286       "	"GERMANY                  "	81285	"Manufacturer#2           "	"YKA,E2fjiVd7eUrzp2Ef8j1QxGo2DFnosaTEH"	"17-516-924-4574"	" regular accounts. furiously unusual courts above the fi"
9870.78	"Supplier#000001286       "	"GERMANY                  "	181285	"Manufacturer#4           "	"YKA,E2fjiVd7eUrzp2Ef8j1QxGo2DFnosaTEH"	"17-516-924-4574"	" regular accounts. furiously unusual courts above the fi"
9870.78	"Supplier#000001286       "	"GERMANY                  "	181285	"Manufacturer#4           "	"YKA,E2fjiVd7eUrzp2Ef8j1QxGo2DFnosaTEH"	"17-516-924-4574"	" regular accounts. furiously unusual courts above the fi"
9852.52	"Supplier#000008973       "	"RUSSIA                   "	18972	"Manufacturer#2           "	"t5L67YdBYYH6o,Vz24jpDyQ9"	"32-188-594-7038"	"rns wake final foxes. carefully unusual depende"
9852.52	"Supplier#000008973       "	"RUSSIA                   "	18972	"Manufacturer#2           "	"t5L67YdBYYH6o,Vz24jpDyQ9"	"32-188-594-7038"	"rns wake final foxes. carefully unusual depende"
9847.83	"Supplier#000008097       "	"RUSSIA                   "	130557	"Manufacturer#2           "	"xMe97bpE69NzdwLoX"	"32-375-640-3593"	" the special excuses. silent sentiments serve carefully final ac"
9847.83	"Supplier#000008097       "	"RUSSIA                   "	130557	"Manufacturer#2           "	"xMe97bpE69NzdwLoX"	"32-375-640-3593"	" the special excuses. silent sentiments serve carefully final ac"
9847.57	"Supplier#000006345       "	"FRANCE                   "	66344	"Manufacturer#4           "	"VSt3rzk3qG698u6ld8HhOByvrTcSTSvQlDQDag"	"16-886-766-7945"	"ges. slyly regular requests are. ruthless, express excuses cajole blithely across the unu"
9847.57	"Supplier#000006345       "	"FRANCE                   "	66344	"Manufacturer#4           "	"VSt3rzk3qG698u6ld8HhOByvrTcSTSvQlDQDag"	"16-886-766-7945"	"ges. slyly regular requests are. ruthless, express excuses cajole blithely across the unu"
9847.57	"Supplier#000006345       "	"FRANCE                   "	86344	"Manufacturer#1           "	"VSt3rzk3qG698u6ld8HhOByvrTcSTSvQlDQDag"	"16-886-766-7945"	"ges. slyly regular requests are. ruthless, express excuses cajole blithely across the unu"
9847.57	"Supplier#000006345       "	"FRANCE                   "	86344	"Manufacturer#1           "	"VSt3rzk3qG698u6ld8HhOByvrTcSTSvQlDQDag"	"16-886-766-7945"	"ges. slyly regular requests are. ruthless, express excuses cajole blithely across the unu"
9847.57	"Supplier#000006345       "	"FRANCE                   "	173827	"Manufacturer#2           "	"VSt3rzk3qG698u6ld8HhOByvrTcSTSvQlDQDag"	"16-886-766-7945"	"ges. slyly regular requests are. ruthless, express excuses cajole blithely across the unu"
9847.57	"Supplier#000006345       "	"FRANCE                   "	173827	"Manufacturer#2           "	"VSt3rzk3qG698u6ld8HhOByvrTcSTSvQlDQDag"	"16-886-766-7945"	"ges. slyly regular requests are. ruthless, express excuses cajole blithely across the unu"
9836.93	"Supplier#000007342       "	"RUSSIA                   "	4841	"Manufacturer#4           "	"JOlK7C1,7xrEZSSOw"	"32-399-414-5385"	"blithely carefully bold theodolites. fur"
9836.93	"Supplier#000007342       "	"RUSSIA                   "	4841	"Manufacturer#4           "	"JOlK7C1,7xrEZSSOw"	"32-399-414-5385"	"blithely carefully bold theodolites. fur"
9817.97	"Supplier#000008787       "	"GERMANY                  "	13784	"Manufacturer#4           "	"D4KuRSgCr1RnTzVLqYD 8WVo0oKVpg,AI"	"17-929-294-2305"	"theodolites. deposits are furiously theodolites. slyly pending accounts haggle quickly. fina"
9817.97	"Supplier#000008787       "	"GERMANY                  "	13784	"Manufacturer#4           "	"D4KuRSgCr1RnTzVLqYD 8WVo0oKVpg,AI"	"17-929-294-2305"	"theodolites. deposits are furiously theodolites. slyly pending accounts haggle quickly. fina"
9817.10	"Supplier#000002352       "	"RUSSIA                   "	124815	"Manufacturer#2           "	"4LfoHUZjgjEbAKw TgdKcgOc4D4uCYw"	"32-551-831-1437"	"wake carefully alongside of the carefully final ex"
9817.10	"Supplier#000002352       "	"RUSSIA                   "	124815	"Manufacturer#2           "	"4LfoHUZjgjEbAKw TgdKcgOc4D4uCYw"	"32-551-831-1437"	"wake carefully alongside of the carefully final ex"
9817.10	"Supplier#000002352       "	"RUSSIA                   "	152351	"Manufacturer#3           "	"4LfoHUZjgjEbAKw TgdKcgOc4D4uCYw"	"32-551-831-1437"	"wake carefully alongside of the carefully final ex"
9817.10	"Supplier#000002352       "	"RUSSIA                   "	152351	"Manufacturer#3           "	"4LfoHUZjgjEbAKw TgdKcgOc4D4uCYw"	"32-551-831-1437"	"wake carefully alongside of the carefully final ex"
9756.30	"Supplier#000004579       "	"FRANCE                   "	139552	"Manufacturer#2           "	"K5nhdAhx6aGpbcRNj0"	"16-946-122-1848"	"ly regular dinos. regular deposi"
9756.30	"Supplier#000004579       "	"FRANCE                   "	139552	"Manufacturer#2           "	"K5nhdAhx6aGpbcRNj0"	"16-946-122-1848"	"ly regular dinos. regular deposi"
9739.86	"Supplier#000003384       "	"FRANCE                   "	138357	"Manufacturer#2           "	"o,Z3v4POifevE k9U1b 6J1ucX,I"	"16-494-913-5925"	"s after the furiously bold packages sleep fluffily idly final requests: quickly final"
9739.86	"Supplier#000003384       "	"FRANCE                   "	138357	"Manufacturer#2           "	"o,Z3v4POifevE k9U1b 6J1ucX,I"	"16-494-913-5925"	"s after the furiously bold packages sleep fluffily idly final requests: quickly final"
9721.95	"Supplier#000008757       "	"UNITED KINGDOM           "	156241	"Manufacturer#3           "	"Atg6GnM4dT2"	"33-821-407-2995"	"eep furiously sauternes; quickl"
9721.95	"Supplier#000008757       "	"UNITED KINGDOM           "	156241	"Manufacturer#3           "	"Atg6GnM4dT2"	"33-821-407-2995"	"eep furiously sauternes; quickl"
9681.33	"Supplier#000008406       "	"RUSSIA                   "	78405	"Manufacturer#1           "	",qUuXcftUl"	"32-139-873-8571"	"haggle slyly regular excuses. quic"
9681.33	"Supplier#000008406       "	"RUSSIA                   "	78405	"Manufacturer#1           "	",qUuXcftUl"	"32-139-873-8571"	"haggle slyly regular excuses. quic"
9652.21	"Supplier#000007618       "	"RUSSIA                   "	107617	"Manufacturer#1           "	"lLb8,1p07ZseCSxYpYt"	"32-642-503-8109"	"equests are blithely; doggedly unusual packages haggle furiously about"
9652.21	"Supplier#000007618       "	"RUSSIA                   "	107617	"Manufacturer#1           "	"lLb8,1p07ZseCSxYpYt"	"32-642-503-8109"	"equests are blithely; doggedly unusual packages haggle furiously about"
9651.40	"Supplier#000001099       "	"RUSSIA                   "	151098	"Manufacturer#3           "	"Ttj  R 9PUekFZI 3zq"	"32-784-328-6730"	" the permanently final foxes. quickly express excuses around the furiously close requests "
9651.40	"Supplier#000001099       "	"RUSSIA                   "	151098	"Manufacturer#3           "	"Ttj  R 9PUekFZI 3zq"	"32-784-328-6730"	" the permanently final foxes. quickly express excuses around the furiously close requests "
9643.55	"Supplier#000005148       "	"ROMANIA                  "	107617	"Manufacturer#1           "	"kT4ciVFslx9z4s79p Js825"	"29-252-617-4850"	"final excuses. final ideas boost quickly furiously speci"
9643.55	"Supplier#000005148       "	"ROMANIA                  "	107617	"Manufacturer#1           "	"kT4ciVFslx9z4s79p Js825"	"29-252-617-4850"	"final excuses. final ideas boost quickly furiously speci"
9624.82	"Supplier#000001816       "	"FRANCE                   "	34306	"Manufacturer#3           "	"e7vab91vLJPWxxZnewmnDBpDmxYHrb"	"16-392-237-6726"	"e packages are around the special ideas. special, pending foxes us"
9624.82	"Supplier#000001816       "	"FRANCE                   "	34306	"Manufacturer#3           "	"e7vab91vLJPWxxZnewmnDBpDmxYHrb"	"16-392-237-6726"	"e packages are around the special ideas. special, pending foxes us"
9624.78	"Supplier#000009658       "	"ROMANIA                  "	189657	"Manufacturer#1           "	"oE9uBgEfSS4opIcepXyAYM,x"	"29-748-876-2014"	"ronic asymptotes wake bravely final"
9624.78	"Supplier#000009658       "	"ROMANIA                  "	189657	"Manufacturer#1           "	"oE9uBgEfSS4opIcepXyAYM,x"	"29-748-876-2014"	"ronic asymptotes wake bravely final"
9612.94	"Supplier#000003228       "	"ROMANIA                  "	120715	"Manufacturer#2           "	"KDdpNKN3cWu7ZSrbdqp7AfSLxx,qWB"	"29-325-784-8187"	"warhorses. quickly even deposits sublate daringly ironic instructions. slyly blithe t"
9612.94	"Supplier#000003228       "	"ROMANIA                  "	120715	"Manufacturer#2           "	"KDdpNKN3cWu7ZSrbdqp7AfSLxx,qWB"	"29-325-784-8187"	"warhorses. quickly even deposits sublate daringly ironic instructions. slyly blithe t"
9612.94	"Supplier#000003228       "	"ROMANIA                  "	198189	"Manufacturer#4           "	"KDdpNKN3cWu7ZSrbdqp7AfSLxx,qWB"	"29-325-784-8187"	"warhorses. quickly even deposits sublate daringly ironic instructions. slyly blithe t"
9612.94	"Supplier#000003228       "	"ROMANIA                  "	198189	"Manufacturer#4           "	"KDdpNKN3cWu7ZSrbdqp7AfSLxx,qWB"	"29-325-784-8187"	"warhorses. quickly even deposits sublate daringly ironic instructions. slyly blithe t"
9571.83	"Supplier#000004305       "	"ROMANIA                  "	66786	"Manufacturer#1           "	"qNHZ7WmCzygwMPRDO9Ps"	"29-973-481-1831"	"kly carefully express asymptotes. furiou"
9571.83	"Supplier#000004305       "	"ROMANIA                  "	66786	"Manufacturer#1           "	"qNHZ7WmCzygwMPRDO9Ps"	"29-973-481-1831"	"kly carefully express asymptotes. furiou"
9571.83	"Supplier#000004305       "	"ROMANIA                  "	179270	"Manufacturer#2           "	"qNHZ7WmCzygwMPRDO9Ps"	"29-973-481-1831"	"kly carefully express asymptotes. furiou"
9571.83	"Supplier#000004305       "	"ROMANIA                  "	179270	"Manufacturer#2           "	"qNHZ7WmCzygwMPRDO9Ps"	"29-973-481-1831"	"kly carefully express asymptotes. furiou"
9558.10	"Supplier#000003532       "	"UNITED KINGDOM           "	88515	"Manufacturer#4           "	"EOeuiiOn21OVpTlGguufFDFsbN1p0lhpxHp"	"33-152-301-2164"	" foxes. quickly even excuses use. slyly special foxes nag bl"
9558.10	"Supplier#000003532       "	"UNITED KINGDOM           "	88515	"Manufacturer#4           "	"EOeuiiOn21OVpTlGguufFDFsbN1p0lhpxHp"	"33-152-301-2164"	" foxes. quickly even excuses use. slyly special foxes nag bl"
9492.79	"Supplier#000005975       "	"GERMANY                  "	25974	"Manufacturer#5           "	"S6mIiCTx82z7lV"	"17-992-579-4839"	"arefully pending accounts. blithely regular excuses boost carefully carefully ironic p"
9492.79	"Supplier#000005975       "	"GERMANY                  "	25974	"Manufacturer#5           "	"S6mIiCTx82z7lV"	"17-992-579-4839"	"arefully pending accounts. blithely regular excuses boost carefully carefully ironic p"
9461.05	"Supplier#000002536       "	"UNITED KINGDOM           "	20033	"Manufacturer#1           "	"8mmGbyzaU 7ZS2wJumTibypncu9pNkDc4FYA"	"33-556-973-5522"	". slyly regular deposits wake slyly. furiously regular warthogs are."
9461.05	"Supplier#000002536       "	"UNITED KINGDOM           "	20033	"Manufacturer#1           "	"8mmGbyzaU 7ZS2wJumTibypncu9pNkDc4FYA"	"33-556-973-5522"	". slyly regular deposits wake slyly. furiously regular warthogs are."
9453.01	"Supplier#000000802       "	"ROMANIA                  "	175767	"Manufacturer#1           "	",6HYXb4uaHITmtMBj4Ak57Pd"	"29-342-882-6463"	"gular frets. permanently special multipliers believe blithely alongs"
9453.01	"Supplier#000000802       "	"ROMANIA                  "	175767	"Manufacturer#1           "	",6HYXb4uaHITmtMBj4Ak57Pd"	"29-342-882-6463"	"gular frets. permanently special multipliers believe blithely alongs"
9408.65	"Supplier#000007772       "	"UNITED KINGDOM           "	117771	"Manufacturer#4           "	"AiC5YAH,gdu0i7"	"33-152-491-1126"	"nag against the final requests. furiously unusual packages cajole blit"
9408.65	"Supplier#000007772       "	"UNITED KINGDOM           "	117771	"Manufacturer#4           "	"AiC5YAH,gdu0i7"	"33-152-491-1126"	"nag against the final requests. furiously unusual packages cajole blit"
9359.61	"Supplier#000004856       "	"ROMANIA                  "	62349	"Manufacturer#5           "	"HYogcF3Jb yh1"	"29-334-870-9731"	"y ironic theodolites. blithely sile"
9359.61	"Supplier#000004856       "	"ROMANIA                  "	62349	"Manufacturer#5           "	"HYogcF3Jb yh1"	"29-334-870-9731"	"y ironic theodolites. blithely sile"
9357.45	"Supplier#000006188       "	"UNITED KINGDOM           "	46187	"Manufacturer#4           "	"g801,ssP8wpTk4Hm"	"33-583-607-1633"	"ously always regular packages. fluffily even accounts beneath the furiously final pack"
9357.45	"Supplier#000006188       "	"UNITED KINGDOM           "	46187	"Manufacturer#4           "	"g801,ssP8wpTk4Hm"	"33-583-607-1633"	"ously always regular packages. fluffily even accounts beneath the furiously final pack"
9357.45	"Supplier#000006188       "	"UNITED KINGDOM           "	138648	"Manufacturer#1           "	"g801,ssP8wpTk4Hm"	"33-583-607-1633"	"ously always regular packages. fluffily even accounts beneath the furiously final pack"
9357.45	"Supplier#000006188       "	"UNITED KINGDOM           "	138648	"Manufacturer#1           "	"g801,ssP8wpTk4Hm"	"33-583-607-1633"	"ously always regular packages. fluffily even accounts beneath the furiously final pack"
9352.04	"Supplier#000003439       "	"GERMANY                  "	170921	"Manufacturer#4           "	"qYPDgoiBGhCYxjgC"	"17-128-996-4650"	" according to the carefully bold ideas"
9352.04	"Supplier#000003439       "	"GERMANY                  "	170921	"Manufacturer#4           "	"qYPDgoiBGhCYxjgC"	"17-128-996-4650"	" according to the carefully bold ideas"
9312.97	"Supplier#000007807       "	"RUSSIA                   "	90279	"Manufacturer#5           "	"oGYMPCk9XHGB2PBfKRnHA"	"32-673-872-5854"	"ecial packages among the pending, even requests use regula"
9312.97	"Supplier#000007807       "	"RUSSIA                   "	90279	"Manufacturer#5           "	"oGYMPCk9XHGB2PBfKRnHA"	"32-673-872-5854"	"ecial packages among the pending, even requests use regula"
9312.97	"Supplier#000007807       "	"RUSSIA                   "	100276	"Manufacturer#5           "	"oGYMPCk9XHGB2PBfKRnHA"	"32-673-872-5854"	"ecial packages among the pending, even requests use regula"
9312.97	"Supplier#000007807       "	"RUSSIA                   "	100276	"Manufacturer#5           "	"oGYMPCk9XHGB2PBfKRnHA"	"32-673-872-5854"	"ecial packages among the pending, even requests use regula"
9312.95	"Supplier#000006713       "	"GERMANY                  "	99185	"Manufacturer#1           "	"JjmCvBijjmCzRJBXy0Up5EjZ9eM6o9bw"	"17-366-234-7745"	"ic theodolites are along the packages. blithely iro"
9312.95	"Supplier#000006713       "	"GERMANY                  "	99185	"Manufacturer#1           "	"JjmCvBijjmCzRJBXy0Up5EjZ9eM6o9bw"	"17-366-234-7745"	"ic theodolites are along the packages. blithely iro"
9309.80	"Supplier#000006427       "	"ROMANIA                  "	111404	"Manufacturer#2           "	"rrMkXW7o0O0U5,CsVTzEKtSRfAWtvhQe5Iu"	"29-908-367-5652"	"packages. final pinto beans cajole. carefully ironic ideas doze. bold accounts cajole along the"
9309.80	"Supplier#000006427       "	"ROMANIA                  "	111404	"Manufacturer#2           "	"rrMkXW7o0O0U5,CsVTzEKtSRfAWtvhQe5Iu"	"29-908-367-5652"	"packages. final pinto beans cajole. carefully ironic ideas doze. bold accounts cajole along the"
9293.21	"Supplier#000005757       "	"UNITED KINGDOM           "	28250	"Manufacturer#5           "	"FBJ0hUWAem3whciziO7lndaJ"	"33-580-351-5737"	"ng requests are carefully along the slyly even packages. carefully unusual deposi"
9293.21	"Supplier#000005757       "	"UNITED KINGDOM           "	28250	"Manufacturer#5           "	"FBJ0hUWAem3whciziO7lndaJ"	"33-580-351-5737"	"ng requests are carefully along the slyly even packages. carefully unusual deposi"
9280.27	"Supplier#000007194       "	"ROMANIA                  "	47193	"Manufacturer#3           "	"zhRUQkBSrFYxIAXTfInj vyGRQjeK"	"29-318-454-2133"	"o beans haggle after the furiously unusual deposits. carefully silent dolphins cajole carefully"
9280.27	"Supplier#000007194       "	"ROMANIA                  "	47193	"Manufacturer#3           "	"zhRUQkBSrFYxIAXTfInj vyGRQjeK"	"29-318-454-2133"	"o beans haggle after the furiously unusual deposits. carefully silent dolphins cajole carefully"
9274.80	"Supplier#000008854       "	"RUSSIA                   "	76346	"Manufacturer#3           "	"1xhLoOUM7I3mZ1mKnerw OSqdbb4QbGa"	"32-524-148-5221"	"y. courts do wake slyly. carefully ironic platelets haggle above the slyly regular the"
9274.80	"Supplier#000008854       "	"RUSSIA                   "	76346	"Manufacturer#3           "	"1xhLoOUM7I3mZ1mKnerw OSqdbb4QbGa"	"32-524-148-5221"	"y. courts do wake slyly. carefully ironic platelets haggle above the slyly regular the"
9249.35	"Supplier#000003973       "	"FRANCE                   "	26466	"Manufacturer#1           "	"d18GiDsL6Wm2IsGXM,RZf1jCsgZAOjNYVThTRP4"	"16-722-866-1658"	"uests are furiously. regular tithes through the regular, final accounts cajole furiously above the q"
9249.35	"Supplier#000003973       "	"FRANCE                   "	26466	"Manufacturer#1           "	"d18GiDsL6Wm2IsGXM,RZf1jCsgZAOjNYVThTRP4"	"16-722-866-1658"	"uests are furiously. regular tithes through the regular, final accounts cajole furiously above the q"
9249.35	"Supplier#000003973       "	"FRANCE                   "	33972	"Manufacturer#1           "	"d18GiDsL6Wm2IsGXM,RZf1jCsgZAOjNYVThTRP4"	"16-722-866-1658"	"uests are furiously. regular tithes through the regular, final accounts cajole furiously above the q"
9249.35	"Supplier#000003973       "	"FRANCE                   "	33972	"Manufacturer#1           "	"d18GiDsL6Wm2IsGXM,RZf1jCsgZAOjNYVThTRP4"	"16-722-866-1658"	"uests are furiously. regular tithes through the regular, final accounts cajole furiously above the q"
"""
Q2_actual_output = """Validate all the predicates of the seed query against the text description.
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

Looks like it has duplicated groups. 
If group by clause is present within union subqueries, try putting it out."""

Q8_text = """The market share for 'INDIA' within Asian region is defined as the fraction of the revenue,
the sum of [extended price * (1-discount)], from the products of 'ECONOMY ANODIZED STEEL' type sold online in that
region that was supplied by the Indian suppliers. The query determines
this for the years 1995 and 1996 presented in this order.
"""
Q8_seed = """Select o_orderdate as o_year, Sum(0) as mkt_share 
 From customer, web_lineitem, nation n1, nation n2, orders, part, region, supplier 
 Where c_custkey = o_custkey
 and c_nationkey = n1.n_nationkey
 and wl_orderkey = o_orderkey
 and wl_partkey = p_partkey
 and wl_suppkey = s_suppkey
 and n1.n_regionkey = r_regionkey
 and n2.n_nationkey = s_nationkey
 and p_type = 'ECONOMY ANODIZED STEEL'
 and r_name = 'ASIA'
 and o_orderdate between '1995-01-01' and '1996-12-31'
group by o_orderdate;"""
Q8_seed_output = """Output of the above seed query is as follows:

"1995-01-02"	0
"1995-01-03"	0
"1995-01-04"	0
"1995-01-05"	0
"1995-01-06"	0
"1995-01-07"	0
"1995-01-10"	0
"1995-01-11"	0
"1995-01-12"	0
"1995-01-13"	0
"1995-01-14"	0
"1995-01-15"	0
"1995-01-16"	0
"1995-01-17"	0
"1995-01-18"	0
"1995-01-20"	0
"1995-01-21"	0
"1995-01-22"	0
"1995-01-23"	0
"1995-01-24"	0
"1995-01-25"	0
"1995-01-26"	0
"1995-01-27"	0
"1995-01-28"	0
"1995-01-29"	0
"1995-01-30"	0
"1995-01-31"	0
"1995-02-02"	0
"1995-02-03"	0
"1995-02-04"	0
"1995-02-05"	0
"1995-02-06"	0
"1995-02-07"	0
"1995-02-08"	0
"1995-02-09"	0
"1995-02-10"	0
"1995-02-11"	0
"1995-02-12"	0
"1995-02-13"	0
"1995-02-14"	0
"1995-02-15"	0
"1995-02-17"	0
"1995-02-18"	0
"1995-02-19"	0
"1995-02-20"	0
"1995-02-22"	0
"1995-02-23"	0
"1995-02-24"	0
"1995-02-25"	0
"1995-02-26"	0
"1995-02-27"	0
"1995-02-28"	0
"1995-03-01"	0
"1995-03-02"	0
"1995-03-03"	0
"1995-03-04"	0
"1995-03-05"	0
"1995-03-06"	0
"1995-03-07"	0
"1995-03-08"	0
"1995-03-09"	0
"1995-03-10"	0
"1995-03-11"	0
"1995-03-12"	0
"1995-03-13"	0
"1995-03-14"	0
"1995-03-15"	0
"1995-03-16"	0
"1995-03-17"	0
"1995-03-18"	0
"1995-03-19"	0
"1995-03-20"	0
"1995-03-21"	0
"1995-03-22"	0
"1995-03-23"	0
"1995-03-24"	0
"1995-03-26"	0
"1995-03-27"	0
"1995-03-28"	0
"1995-03-29"	0
"1995-03-30"	0
"1995-03-31"	0
"1995-04-01"	0
"1995-04-02"	0
"1995-04-03"	0
"1995-04-05"	0
"1995-04-06"	0
"1995-04-07"	0
"1995-04-08"	0
"1995-04-09"	0
"1995-04-11"	0
"1995-04-12"	0
"1995-04-13"	0
"1995-04-14"	0
"1995-04-16"	0
"1995-04-17"	0
"1995-04-18"	0
"1995-04-19"	0
"1995-04-22"	0
"1995-04-23"	0
"1995-04-24"	0
"1995-04-25"	0
"1995-04-26"	0
"1995-04-27"	0
"1995-04-28"	0
"1995-04-29"	0
"1995-04-30"	0
"1995-05-01"	0
"1995-05-02"	0
"1995-05-03"	0
"1995-05-04"	0
"1995-05-05"	0
"1995-05-06"	0
"1995-05-07"	0
"1995-05-09"	0
"1995-05-10"	0
"1995-05-11"	0
"1995-05-13"	0
"1995-05-15"	0
"1995-05-17"	0
"1995-05-18"	0
"1995-05-19"	0
"1995-05-20"	0
"1995-05-21"	0
"1995-05-22"	0
"1995-05-23"	0
"1995-05-24"	0
"1995-05-26"	0
"1995-05-27"	0
"1995-05-28"	0
"1995-05-29"	0
"1995-05-30"	0
"1995-05-31"	0
"1995-06-01"	0
"1995-06-02"	0
"1995-06-03"	0
"1995-06-04"	0
"1995-06-06"	0
"1995-06-08"	0
"1995-06-09"	0
"1995-06-11"	0
"1995-06-12"	0
"1995-06-13"	0
"1995-06-14"	0
"1995-06-15"	0
"1995-06-16"	0
"1995-06-17"	0
"1995-06-19"	0
"1995-06-21"	0
"1995-06-22"	0
"1995-06-23"	0
"1995-06-24"	0
"1995-06-25"	0
"1995-06-27"	0
"1995-06-28"	0
"1995-06-29"	0
"1995-06-30"	0
"1995-07-01"	0
"1995-07-02"	0
"1995-07-04"	0
"1995-07-05"	0
"1995-07-06"	0
"1995-07-07"	0
"1995-07-09"	0
"1995-07-12"	0
"1995-07-13"	0
"1995-07-14"	0
"1995-07-15"	0
"1995-07-16"	0
"1995-07-17"	0
"1995-07-18"	0
"1995-07-19"	0
"1995-07-21"	0
"1995-07-22"	0
"1995-07-23"	0
"1995-07-24"	0
"1995-07-25"	0
"1995-07-26"	0
"1995-07-27"	0
"1995-07-28"	0
"1995-07-29"	0
"1995-07-30"	0
"1995-07-31"	0
"1995-08-01"	0
"1995-08-02"	0
"1995-08-03"	0
"1995-08-04"	0
"1995-08-05"	0
"1995-08-06"	0
"1995-08-09"	0
"1995-08-10"	0
"1995-08-11"	0
"1995-08-12"	0
"1995-08-13"	0
"1995-08-14"	0
"1995-08-15"	0
"1995-08-16"	0
"1995-08-17"	0
"1995-08-19"	0
"1995-08-20"	0
"1995-08-21"	0
"1995-08-22"	0
"1995-08-25"	0
"1995-08-26"	0
"1995-08-27"	0
"1995-08-28"	0
"1995-08-29"	0
"1995-08-30"	0
"1995-08-31"	0
"1995-09-01"	0
"1995-09-02"	0
"1995-09-03"	0
"1995-09-04"	0
"1995-09-05"	0
"1995-09-06"	0
"1995-09-07"	0
"1995-09-08"	0
"1995-09-09"	0
"1995-09-10"	0
"1995-09-11"	0
"1995-09-12"	0
"1995-09-13"	0
"1995-09-14"	0
"1995-09-15"	0
"1995-09-18"	0
"1995-09-19"	0
"1995-09-21"	0
"1995-09-22"	0
"1995-09-23"	0
"1995-09-24"	0
"1995-09-25"	0
"1995-09-26"	0
"1995-09-27"	0
"1995-09-28"	0
"1995-09-29"	0
"1995-09-30"	0
"1995-10-01"	0
"1995-10-02"	0
"1995-10-03"	0
"1995-10-04"	0
"1995-10-05"	0
"1995-10-06"	0
"1995-10-07"	0
"1995-10-08"	0
"1995-10-09"	0
"1995-10-10"	0
"1995-10-11"	0
"1995-10-13"	0
"1995-10-15"	0
"1995-10-16"	0
"1995-10-18"	0
"1995-10-19"	0
"1995-10-22"	0
"1995-10-23"	0
"1995-10-25"	0
"1995-10-26"	0
"1995-10-28"	0
"1995-10-29"	0
"1995-10-30"	0
"1995-11-01"	0
"1995-11-02"	0
"1995-11-03"	0
"1995-11-04"	0
"1995-11-05"	0
"1995-11-06"	0
"1995-11-07"	0
"1995-11-08"	0
"1995-11-09"	0
"1995-11-10"	0
"1995-11-11"	0
"1995-11-12"	0
"1995-11-13"	0
"1995-11-15"	0
"1995-11-16"	0
"1995-11-17"	0
"1995-11-20"	0
"1995-11-21"	0
"1995-11-22"	0
"1995-11-23"	0
"1995-11-24"	0
"1995-11-26"	0
"1995-11-27"	0
"1995-11-28"	0
"1995-11-29"	0
"1995-11-30"	0
"1995-12-01"	0
"1995-12-02"	0
"1995-12-03"	0
"1995-12-04"	0
"1995-12-05"	0
"1995-12-06"	0
"1995-12-07"	0
"1995-12-10"	0
"1995-12-13"	0
"1995-12-14"	0
"1995-12-15"	0
"1995-12-16"	0
"1995-12-19"	0
"1995-12-20"	0
"1995-12-21"	0
"1995-12-22"	0
"1995-12-23"	0
"1995-12-24"	0
"1995-12-25"	0
"1995-12-26"	0
"1995-12-27"	0
"1995-12-28"	0
"1995-12-29"	0
"1995-12-31"	0
"1996-01-01"	0
"1996-01-02"	0
"1996-01-03"	0
"1996-01-04"	0
"1996-01-05"	0
"1996-01-06"	0
"1996-01-07"	0
"1996-01-11"	0
"1996-01-13"	0
"1996-01-14"	0
"1996-01-15"	0
"1996-01-16"	0
"1996-01-18"	0
"1996-01-20"	0
"1996-01-21"	0
"1996-01-22"	0
"1996-01-23"	0
"1996-01-24"	0
"1996-01-26"	0
"1996-01-27"	0
"1996-01-29"	0
"1996-01-30"	0
"1996-01-31"	0
"1996-02-01"	0
"1996-02-02"	0
"1996-02-03"	0
"1996-02-04"	0
"1996-02-05"	0
"1996-02-06"	0
"1996-02-07"	0
"1996-02-08"	0
"1996-02-09"	0
"1996-02-10"	0
"1996-02-11"	0
"1996-02-12"	0
"1996-02-13"	0
"1996-02-14"	0
"1996-02-15"	0
"1996-02-16"	0
"1996-02-17"	0
"1996-02-18"	0
"1996-02-19"	0
"1996-02-20"	0
"1996-02-22"	0
"1996-02-23"	0
"1996-02-24"	0
"1996-02-25"	0
"1996-02-26"	0
"1996-02-27"	0
"1996-02-28"	0
"1996-03-01"	0
"1996-03-02"	0
"1996-03-03"	0
"1996-03-04"	0
"1996-03-05"	0
"1996-03-06"	0
"1996-03-07"	0
"1996-03-08"	0
"1996-03-09"	0
"1996-03-10"	0
"1996-03-11"	0
"1996-03-12"	0
"1996-03-14"	0
"1996-03-15"	0
"1996-03-16"	0
"1996-03-17"	0
"1996-03-18"	0
"1996-03-19"	0
"1996-03-21"	0
"1996-03-22"	0
"1996-03-23"	0
"1996-03-24"	0
"1996-03-25"	0
"1996-03-26"	0
"1996-03-27"	0
"1996-03-28"	0
"1996-03-29"	0
"1996-03-30"	0
"1996-03-31"	0
"1996-04-01"	0
"1996-04-02"	0
"1996-04-03"	0
"1996-04-04"	0
"1996-04-05"	0
"1996-04-07"	0
"1996-04-09"	0
"1996-04-10"	0
"1996-04-11"	0
"1996-04-12"	0
"1996-04-13"	0
"1996-04-14"	0
"1996-04-15"	0
"1996-04-16"	0
"1996-04-17"	0
"1996-04-18"	0
"1996-04-19"	0
"1996-04-20"	0
"1996-04-22"	0
"1996-04-23"	0
"1996-04-25"	0
"1996-04-28"	0
"1996-04-29"	0
"1996-04-30"	0
"1996-05-01"	0
"1996-05-02"	0
"1996-05-05"	0
"1996-05-06"	0
"1996-05-07"	0
"1996-05-08"	0
"1996-05-10"	0
"1996-05-11"	0
"1996-05-12"	0
"1996-05-14"	0
"1996-05-17"	0
"1996-05-20"	0
"1996-05-21"	0
"1996-05-22"	0
"1996-05-23"	0
"1996-05-24"	0
"1996-05-25"	0
"1996-05-26"	0
"1996-05-27"	0
"1996-05-28"	0
"1996-05-29"	0
"1996-05-30"	0
"1996-05-31"	0
"1996-06-01"	0
"1996-06-02"	0
"1996-06-03"	0
"1996-06-04"	0
"1996-06-05"	0
"1996-06-06"	0
"1996-06-07"	0
"1996-06-08"	0
"1996-06-09"	0
"1996-06-10"	0
"1996-06-11"	0
"1996-06-12"	0
"1996-06-13"	0
"1996-06-14"	0
"1996-06-15"	0
"1996-06-16"	0
"1996-06-17"	0
"1996-06-18"	0
"1996-06-19"	0
"1996-06-21"	0
"1996-06-22"	0
"1996-06-23"	0
"1996-06-24"	0
"1996-06-26"	0
"1996-06-29"	0
"1996-06-30"	0
"1996-07-01"	0
"1996-07-02"	0
"1996-07-03"	0
"1996-07-04"	0
"1996-07-05"	0
"1996-07-06"	0
"1996-07-07"	0
"1996-07-08"	0
"1996-07-09"	0
"1996-07-10"	0
"1996-07-11"	0
"1996-07-12"	0
"1996-07-13"	0
"1996-07-15"	0
"1996-07-16"	0
"1996-07-17"	0
"1996-07-18"	0
"1996-07-19"	0
"1996-07-20"	0
"1996-07-21"	0
"1996-07-22"	0
"1996-07-23"	0
"1996-07-24"	0
"1996-07-25"	0
"1996-07-26"	0
"1996-07-27"	0
"1996-07-28"	0
"1996-07-29"	0
"1996-07-30"	0
"1996-07-31"	0
"1996-08-01"	0
"1996-08-02"	0
"1996-08-03"	0
"1996-08-04"	0
"1996-08-05"	0
"1996-08-06"	0
"1996-08-07"	0
"1996-08-08"	0
"1996-08-10"	0
"1996-08-11"	0
"1996-08-12"	0
"1996-08-16"	0
"1996-08-17"	0
"1996-08-18"	0
"1996-08-19"	0
"1996-08-20"	0
"1996-08-21"	0
"1996-08-22"	0
"1996-08-23"	0
"1996-08-24"	0
"1996-08-25"	0
"1996-08-26"	0
"1996-08-29"	0
"1996-08-30"	0
"1996-08-31"	0
"1996-09-01"	0
"1996-09-02"	0
"1996-09-03"	0
"1996-09-04"	0
"1996-09-05"	0
"1996-09-07"	0
"1996-09-08"	0
"1996-09-09"	0
"1996-09-10"	0
"1996-09-11"	0
"1996-09-12"	0
"1996-09-13"	0
"1996-09-14"	0
"1996-09-15"	0
"1996-09-18"	0
"1996-09-19"	0
"1996-09-21"	0
"1996-09-22"	0
"1996-09-23"	0
"1996-09-25"	0
"1996-09-26"	0
"1996-09-27"	0
"1996-09-28"	0
"1996-09-29"	0
"1996-09-30"	0
"1996-10-01"	0
"1996-10-02"	0
"1996-10-03"	0
"1996-10-04"	0
"1996-10-05"	0
"1996-10-06"	0
"1996-10-08"	0
"1996-10-09"	0
"1996-10-10"	0
"1996-10-11"	0
"1996-10-12"	0
"1996-10-14"	0
"1996-10-15"	0
"1996-10-16"	0
"1996-10-17"	0
"1996-10-18"	0
"1996-10-20"	0
"1996-10-21"	0
"1996-10-22"	0
"1996-10-23"	0
"1996-10-25"	0
"1996-10-26"	0
"1996-10-27"	0
"1996-10-28"	0
"1996-10-30"	0
"1996-10-31"	0
"1996-11-01"	0
"1996-11-02"	0
"1996-11-03"	0
"1996-11-04"	0
"1996-11-07"	0
"1996-11-08"	0
"1996-11-09"	0
"1996-11-10"	0
"1996-11-11"	0
"1996-11-12"	0
"1996-11-13"	0
"1996-11-15"	0
"1996-11-16"	0
"1996-11-18"	0
"1996-11-19"	0
"1996-11-20"	0
"1996-11-21"	0
"1996-11-22"	0
"1996-11-23"	0
"1996-11-26"	0
"1996-11-27"	0
"1996-11-28"	0
"1996-11-29"	0
"1996-11-30"	0
"1996-12-01"	0
"1996-12-02"	0
"1996-12-03"	0
"1996-12-05"	0
"1996-12-06"	0
"1996-12-07"	0
"1996-12-08"	0
"1996-12-09"	0
"1996-12-10"	0
"1996-12-11"	0
"1996-12-12"	0
"1996-12-13"	0
"1996-12-14"	0
"1996-12-15"	0
"1996-12-16"	0
"1996-12-19"	0
"1996-12-20"	0
"1996-12-21"	0
"1996-12-22"	0
"1996-12-23"	0
"1996-12-24"	0
"1996-12-25"	0
"1996-12-26"	0
"1996-12-27"	0
"1996-12-28"	0
"1996-12-29"	0
"1996-12-30"	0
"1996-12-31"	0
"""
Q8_actual_output = """But the actual output should be as follows:
1995	0.03548762227781919462
1996	0.03570071939890738017

Fix the seed query."""
Q8_feedback1 = """You formulated the following query:
SELECT 
    EXTRACT(YEAR FROM o_orderdate) AS year, 
    SUM(CASE WHEN n.n_name = 'INDIA' THEN wl_extendedprice * (1 - wl_discount) ELSE 0 END) / SUM(wl_extendedprice * (1 - wl_discount)) AS market_share
FROM 
    web_lineitem wl
JOIN 
    orders o ON wl.wl_orderkey = o.o_orderkey
JOIN 
    partsupp ps ON wl.wl_partkey = ps.ps_partkey AND wl.wl_suppkey = ps.ps_suppkey
JOIN 
    supplier s ON ps.ps_suppkey = s.s_suppkey
JOIN 
    nation n ON s.s_nationkey = n.n_nationkey
JOIN 
    region r ON n.n_regionkey = r.r_regionkey
JOIN 
    part p ON ps.ps_partkey = p.p_partkey
WHERE 
    r.r_name = 'ASIA' 
    AND p.p_type = 'ECONOMY ANODIZED STEEL' 
    AND EXTRACT(YEAR FROM o.o_orderdate) IN (1995, 1996)
GROUP BY 
    EXTRACT(YEAR FROM o_orderdate)
ORDER BY 
    year;

Which gives the following incorrect result:
1995	0.17205993405817618513
1996	0.17698886854534015566.

Do not use NULLIF.
Use nation table twice.
Fix the query."""

Q9_text = """The Query finds, for each nation and each year, the profit for all parts ordered in that
year that contain a specified substring in their names and that were filled by a supplier in that nation. 
The profit is defined as the sum of [(extended price*(1-discount)) - (supply cost * quantity)] for all 
line items describing parts in the specified line. 
The query lists the nations in ascending alphabetical order and, for each nation, the year
and profit in descending order by year (most recent first)."""
Q9_seed = """(Select n_name as nation, o_orderdate as o_year, Sum(-ps_supplycost*wl_quantity + wl_extendedprice*(1 - wl_discount)) as sum_profit 
 From nation, orders, part, partsupp, supplier, web_lineitem 
 Where orders.o_orderkey = web_lineitem.wl_orderkey
 and part.p_partkey = partsupp.ps_partkey
 and partsupp.ps_partkey = web_lineitem.wl_partkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and supplier.s_suppkey = web_lineitem.wl_suppkey
 and nation.n_nationkey = supplier.s_nationkey
 and part.p_name LIKE 'co%' 
 Group By n_name , o_orderdate
 Order By nation asc)
 UNION ALL  
 (Select n_name as nation, o_orderdate as o_year, Sum(-ps_supplycost*sl_quantity + sl_extendedprice*(1 - sl_discount)) as sum_profit 
 From nation, orders, part, partsupp, store_lineitem, supplier 
 Where orders.o_orderkey = store_lineitem.sl_orderkey
 and part.p_partkey = partsupp.ps_partkey
 and partsupp.ps_partkey = store_lineitem.sl_partkey
 and partsupp.ps_suppkey = store_lineitem.sl_suppkey
 and store_lineitem.sl_suppkey = supplier.s_suppkey
 and nation.n_nationkey = supplier.s_nationkey
 and part.p_name LIKE 'co%' 
 Group By n_name , o_orderdate
 Order By nation asc); """
Q9_seed_output = """Output of the above seed query (first 100 rows) is as follows:
"ALGERIA                  "	"1992-01-02"	52217.6040
"ALGERIA                  "	"1992-01-07"	22592.1112
"ALGERIA                  "	"1992-01-08"	38173.3618
"ALGERIA                  "	"1992-01-19"	30579.0240
"ALGERIA                  "	"1992-01-22"	41978.8080
"ALGERIA                  "	"1992-01-25"	48992.7768
"ALGERIA                  "	"1992-01-26"	18665.3740
"ALGERIA                  "	"1992-01-27"	834.4632
"ALGERIA                  "	"1992-02-06"	10565.9337
"ALGERIA                  "	"1992-02-11"	4874.4000
"ALGERIA                  "	"1992-02-13"	26284.7644
"ALGERIA                  "	"1992-02-19"	14248.4034
"ALGERIA                  "	"1992-02-21"	29988.4800
"ALGERIA                  "	"1992-03-01"	48145.1750
"ALGERIA                  "	"1992-03-03"	13978.4336
"ALGERIA                  "	"1992-03-06"	8668.6400
"ALGERIA                  "	"1992-03-09"	8350.8620
"ALGERIA                  "	"1992-03-10"	12502.5960
"ALGERIA                  "	"1992-03-12"	6302.1500
"ALGERIA                  "	"1992-03-14"	4685.4588
"ALGERIA                  "	"1992-03-19"	16700.5240
"ALGERIA                  "	"1992-03-23"	-441.4288
"ALGERIA                  "	"1992-03-28"	64381.8790
"ALGERIA                  "	"1992-03-30"	14017.5240
"ALGERIA                  "	"1992-04-15"	36841.8432
"ALGERIA                  "	"1992-04-19"	47351.2352
"ALGERIA                  "	"1992-04-21"	44387.6300
"ALGERIA                  "	"1992-04-27"	21648.0998
"ALGERIA                  "	"1992-04-29"	3025.9698
"ALGERIA                  "	"1992-05-27"	13269.4299
"ALGERIA                  "	"1992-05-29"	48655.3158
"ALGERIA                  "	"1992-06-02"	119982.0985
"ALGERIA                  "	"1992-06-09"	7877.7168
"ALGERIA                  "	"1992-06-10"	22019.1760
"ALGERIA                  "	"1992-06-12"	24748.9000
"ALGERIA                  "	"1992-06-15"	29998.2336
"ALGERIA                  "	"1992-06-20"	2269.7752
"ALGERIA                  "	"1992-06-28"	4046.9526
"ALGERIA                  "	"1992-07-03"	11010.5732
"ALGERIA                  "	"1992-07-07"	14585.3568
"ALGERIA                  "	"1992-07-14"	32983.8400
"ALGERIA                  "	"1992-07-18"	10967.1450
"ALGERIA                  "	"1992-07-20"	25666.8600
"ALGERIA                  "	"1992-07-23"	43375.1608
"ALGERIA                  "	"1992-07-24"	16613.1888
"ALGERIA                  "	"1992-07-31"	4736.0800
"ALGERIA                  "	"1992-08-11"	24581.9340
"ALGERIA                  "	"1992-08-14"	47009.4000
"ALGERIA                  "	"1992-08-15"	12385.4820
"ALGERIA                  "	"1992-08-21"	37595.4800
"ALGERIA                  "	"1992-08-26"	24733.2340
"ALGERIA                  "	"1992-09-07"	8326.2696
"ALGERIA                  "	"1992-09-15"	36298.7366
"ALGERIA                  "	"1992-09-16"	11074.6680
"ALGERIA                  "	"1992-09-18"	44222.2568
"ALGERIA                  "	"1992-09-20"	56699.2893
"ALGERIA                  "	"1992-09-26"	12278.7248
"ALGERIA                  "	"1992-09-30"	25081.6203
"ALGERIA                  "	"1992-10-11"	30034.7310
"ALGERIA                  "	"1992-11-09"	11743.1580
"ALGERIA                  "	"1992-11-14"	14300.3728
"ALGERIA                  "	"1992-11-20"	10388.5980
"ALGERIA                  "	"1992-11-25"	9485.3142
"ALGERIA                  "	"1992-11-27"	12910.0200
"ALGERIA                  "	"1992-11-28"	73329.7500
"ALGERIA                  "	"1992-12-10"	32714.6400
"ALGERIA                  "	"1992-12-11"	1398.2430
"ALGERIA                  "	"1992-12-18"	26917.4688
"ALGERIA                  "	"1992-12-22"	7301.4240
"ALGERIA                  "	"1992-12-25"	63094.2840
"ALGERIA                  "	"1992-12-29"	42892.1640
"ALGERIA                  "	"1993-01-03"	26669.0048
"ALGERIA                  "	"1993-01-09"	21027.8992
"ALGERIA                  "	"1993-01-12"	16252.0480
"ALGERIA                  "	"1993-01-15"	16773.8270
"ALGERIA                  "	"1993-01-20"	18382.3560
"ALGERIA                  "	"1993-02-03"	14048.3280
"ALGERIA                  "	"1993-02-13"	51553.6808
"ALGERIA                  "	"1993-02-14"	12271.6256
"ALGERIA                  "	"1993-02-15"	14485.6116
"ALGERIA                  "	"1993-02-23"	1628.6328
"ALGERIA                  "	"1993-03-04"	39387.4532
"ALGERIA                  "	"1993-03-08"	10688.3955
"ALGERIA                  "	"1993-03-12"	41741.6800
"ALGERIA                  "	"1993-03-13"	3950.3278
"ALGERIA                  "	"1993-03-16"	34219.3840
"ALGERIA                  "	"1993-03-20"	37016.3320
"ALGERIA                  "	"1993-03-21"	41465.6669
"ALGERIA                  "	"1993-03-22"	9527.0658
"ALGERIA                  "	"1993-03-30"	23291.3700
"ALGERIA                  "	"1993-04-10"	24430.5040
"ALGERIA                  "	"1993-04-13"	31113.1296
"ALGERIA                  "	"1993-04-19"	34942.5664
"ALGERIA                  "	"1993-04-25"	36208.5504
"ALGERIA                  "	"1993-05-03"	3672.0305
"ALGERIA                  "	"1993-05-04"	10850.4800
"ALGERIA                  "	"1993-05-12"	25401.1484
"ALGERIA                  "	"1993-05-23"	47040.8400
"ALGERIA                  "	"1993-05-26"	293.2347
"ALGERIA                  "	"1993-05-30"	67197.4938"""
Q9_actual_output = """But the actual output (total 175 rows) should be as follows:
"ALGERIA                  "	1998	2104004.1284
"ALGERIA                  "	1997	3187898.6232
"ALGERIA                  "	1996	3348008.7712
"ALGERIA                  "	1995	2553119.6322
"ALGERIA                  "	1994	3931781.7352
"ALGERIA                  "	1993	3021645.2510
"ALGERIA                  "	1992	3600290.3246
"ARGENTINA                "	1998	2311988.7706
"ARGENTINA                "	1997	3940943.6760
"ARGENTINA                "	1996	3064148.3502
"ARGENTINA                "	1995	3255627.3616
"ARGENTINA                "	1994	2588504.4388
"ARGENTINA                "	1993	2970948.3448
"ARGENTINA                "	1992	2978168.8384
"BRAZIL                   "	1998	2781353.6244
"BRAZIL                   "	1997	3396203.4000
"BRAZIL                   "	1996	4327443.4166
"BRAZIL                   "	1995	3640143.2078
"BRAZIL                   "	1994	3744072.3024
"BRAZIL                   "	1993	3436137.2152
"BRAZIL                   "	1992	3372623.8388
"CANADA                   "	1998	2729569.8680
"CANADA                   "	1997	3741726.1976
"CANADA                   "	1996	3357645.6088
"CANADA                   "	1995	3424379.3520
"CANADA                   "	1994	4403532.1800
"CANADA                   "	1993	3712163.1378
"CANADA                   "	1992	4674680.6596
"CHINA                    "	1998	3615913.0748
"CHINA                    "	1997	4323497.9548
"CHINA                    "	1996	5528902.7568
"CHINA                    "	1995	5487705.5278
"CHINA                    "	1994	5270884.2816
"CHINA                    "	1993	4606787.9344
"CHINA                    "	1992	4182292.4676
"EGYPT                    "	1998	2481437.5772
"EGYPT                    "	1997	4114922.2236
"EGYPT                    "	1996	3702451.8090
"EGYPT                    "	1995	3529472.8496
"EGYPT                    "	1994	3442477.6896
"EGYPT                    "	1993	4074783.1102
"EGYPT                    "	1992	3409149.9078
"ETHIOPIA                 "	1998	2121814.2400
"ETHIOPIA                 "	1997	3211633.2282
"ETHIOPIA                 "	1996	2589985.8142
"ETHIOPIA                 "	1995	2686025.4524
"ETHIOPIA                 "	1994	3195577.3718
"ETHIOPIA                 "	1993	3350494.1322
"ETHIOPIA                 "	1992	3220323.7050
"FRANCE                   "	1998	2223248.8126
"FRANCE                   "	1997	3232875.8400
"FRANCE                   "	1996	4218260.7646
"FRANCE                   "	1995	3699545.5616
"FRANCE                   "	1994	4872773.1830
"FRANCE                   "	1993	3475385.2224
"FRANCE                   "	1992	4165421.9176
"GERMANY                  "	1998	2335955.0292
"GERMANY                  "	1997	3530389.6948
"GERMANY                  "	1996	3419933.8384
"GERMANY                  "	1995	4775378.2214
"GERMANY                  "	1994	4198636.0116
"GERMANY                  "	1993	3924921.8952
"GERMANY                  "	1992	4216749.2226
"INDIA                    "	1998	2936418.5240
"INDIA                    "	1997	4776117.3138
"INDIA                    "	1996	5051142.5668
"INDIA                    "	1995	4714627.7458
"INDIA                    "	1994	5107550.4262
"INDIA                    "	1993	5495393.1978
"INDIA                    "	1992	5193487.8336
"INDONESIA                "	1998	1641360.7140
"INDONESIA                "	1997	2723306.5910
"INDONESIA                "	1996	2703123.9018
"INDONESIA                "	1995	3692098.2306
"INDONESIA                "	1994	3017693.8096
"INDONESIA                "	1993	3392005.5952
"INDONESIA                "	1992	2908205.2664
"IRAN                     "	1998	2424972.8214
"IRAN                     "	1997	2507581.7116
"IRAN                     "	1996	4116180.9910
"IRAN                     "	1995	2695173.3900
"IRAN                     "	1994	4400719.3440
"IRAN                     "	1993	4372859.1966
"IRAN                     "	1992	3410539.1062
"IRAQ                     "	1998	2292411.7222
"IRAQ                     "	1997	3824125.3636
"IRAQ                     "	1996	4078092.2066
"IRAQ                     "	1995	3521641.3486
"IRAQ                     "	1994	4229767.8776
"IRAQ                     "	1993	4051171.0774
"IRAQ                     "	1992	5269411.4488
"JAPAN                    "	1998	3009313.4820
"JAPAN                    "	1997	3816386.7602
"JAPAN                    "	1996	3792853.1580
"JAPAN                    "	1995	3279782.3764
"JAPAN                    "	1994	4965852.1768
"JAPAN                    "	1993	3962919.5524
"JAPAN                    "	1992	4418434.7210
"JORDAN                   "	1998	2573778.0994
"JORDAN                   "	1997	3514194.0232
"JORDAN                   "	1996	3453956.3836
"JORDAN                   "	1995	3383323.3440
"JORDAN                   "	1994	3926366.7884
"JORDAN                   "	1993	3429469.5752
"JORDAN                   "	1992	4017195.7998
"KENYA                    "	1998	1977128.2984
"KENYA                    "	1997	3878886.6216
"KENYA                    "	1996	4283770.0346
"KENYA                    "	1995	3684121.9860
"KENYA                    "	1994	3971431.3516
"KENYA                    "	1993	2948158.9594
"KENYA                    "	1992	4498902.5148
"MOROCCO                  "	1998	2198734.4834
"MOROCCO                  "	1997	4310307.2918
"MOROCCO                  "	1996	3939839.8496
"MOROCCO                  "	1995	3641705.5186
"MOROCCO                  "	1994	3483693.4280
"MOROCCO                  "	1993	3497790.9278
"MOROCCO                  "	1992	4400737.8912
"MOZAMBIQUE               "	1998	1688883.9064
"MOZAMBIQUE               "	1997	2682173.8948
"MOZAMBIQUE               "	1996	4004977.0842
"MOZAMBIQUE               "	1995	4747655.3704
"MOZAMBIQUE               "	1994	3152877.1692
"MOZAMBIQUE               "	1993	4317364.0802
"MOZAMBIQUE               "	1992	3212744.4826
"PERU                     "	1998	2411280.2976
"PERU                     "	1997	4175254.0198
"PERU                     "	1996	4317638.4092
"PERU                     "	1995	3430729.7046
"PERU                     "	1994	4473833.9050
"PERU                     "	1993	4736330.8054
"PERU                     "	1992	3707311.5416
"ROMANIA                  "	1998	2622829.0478
"ROMANIA                  "	1997	2755161.2216
"ROMANIA                  "	1996	3453235.0612
"ROMANIA                  "	1995	4451177.9274
"ROMANIA                  "	1994	3993678.6150
"ROMANIA                  "	1993	3442406.7674
"ROMANIA                  "	1992	3095445.2332
"RUSSIA                   "	1998	2799367.1018
"RUSSIA                   "	1997	3603951.3740
"RUSSIA                   "	1996	4101893.7134
"RUSSIA                   "	1995	4658737.4644
"RUSSIA                   "	1994	3544939.9782
"RUSSIA                   "	1993	4133056.0918
"RUSSIA                   "	1992	3314228.0258
"SAUDI ARABIA             "	1998	1392239.3674
"SAUDI ARABIA             "	1997	3492712.3254
"SAUDI ARABIA             "	1996	3280364.2032
"SAUDI ARABIA             "	1995	2907473.2878
"SAUDI ARABIA             "	1994	3626943.4308
"SAUDI ARABIA             "	1993	2241815.5062
"SAUDI ARABIA             "	1992	3418476.9324
"UNITED KINGDOM           "	1998	2070530.8134
"UNITED KINGDOM           "	1997	3496624.0942
"UNITED KINGDOM           "	1996	4290566.0128
"UNITED KINGDOM           "	1995	5011375.0620
"UNITED KINGDOM           "	1994	4468388.6026
"UNITED KINGDOM           "	1993	3762968.8560
"UNITED KINGDOM           "	1992	4257172.9440
"UNITED STATES            "	1998	2393679.8082
"UNITED STATES            "	1997	4207789.5780
"UNITED STATES            "	1996	4039274.9722
"UNITED STATES            "	1995	5164820.5280
"UNITED STATES            "	1994	4835206.8246
"UNITED STATES            "	1993	4123589.6748
"UNITED STATES            "	1992	4085300.3622
"VIETNAM                  "	1998	1966093.5072
"VIETNAM                  "	1997	2382124.5384
"VIETNAM                  "	1996	3317171.0134
"VIETNAM                  "	1995	2750725.8554
"VIETNAM                  "	1994	3125713.7206
"VIETNAM                  "	1993	2677794.0856
"VIETNAM                  "	1992	2486942.9072

The expected result has much lesser number of groups per n_name.
So, the position of group by clause in the seed and the expected query must be different.
Considering performing union first and then group by.
Fix the seed query.
"""

Q10_text = """The Query finds the top 20 customers, in terms of their effect on lost revenue for a given
quarter, who have returned parts. The query considers only parts that were ordered in the specified quarter. The
query lists the customer's name, address, nation, phone number, account balance, comment information and revenue
lost. The customers are listed in descending order of lost revenue. Revenue lost is defined as
sum(extended price*(1-discount)) for all qualifying line items."""
Q10_seed = """(Select c_custkey, c_name, Sum(sl_extendedprice*(1 - sl_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment 
 From customer, nation, orders, store_lineitem 
 Where orders.o_orderkey = store_lineitem.sl_orderkey
 and customer.c_nationkey = nation.n_nationkey
 and customer.c_custkey = orders.o_custkey
 and store_lineitem.sl_returnflag = 'R'
 and orders.o_orderdate between '1995-01-01' and '1995-03-31' 
 Group By c_acctbal, c_address, c_comment, c_custkey, c_name, c_phone, n_name 
 Order By revenue desc, c_custkey asc, c_name asc, c_acctbal asc, c_phone asc, n_name asc, c_address asc, c_comment asc Limit 20)
 UNION ALL  
 (Select c_custkey, c_name, Sum(wl_extendedprice*(1 - wl_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment 
 From customer, nation, orders, web_lineitem 
 Where customer.c_nationkey = nation.n_nationkey
 and orders.o_orderkey = web_lineitem.wl_orderkey
 and customer.c_custkey = orders.o_custkey
 and web_lineitem.wl_returnflag = 'R'
 and orders.o_orderdate between '1995-01-01' and '1995-03-31' 
 Group By c_acctbal, c_address, c_comment, c_custkey, c_name, c_phone, n_name 
 Order By revenue desc, c_custkey asc, c_name asc, c_acctbal asc, c_phone asc, n_name asc, c_address asc, c_comment asc Limit 20);  
 """
Q10_seed_output = """Output of the above seed query is as follows:
109168	"Customer#000109168"	519856.2131	2947.40	"CHINA                    "	"9HqXcL6X4eyYc4OUd"	"28-198-666-9028"	"efully final, regular asymptotes. quickly ironic packages cajole carefully. blithely final platelets wak"
30523	"Customer#000030523"	465052.1889	3463.98	"PERU                     "	"3A8vNPnR5e0Kz9ytD87XIfSGQZpVThT6RMTkd7"	"27-228-232-7684"	"s x-ray slyly. quickly unusual requests nag slyly at the unusual foxes. regular asymptotes haggle. brave, ir"
116560	"Customer#000116560"	453467.6456	5025.79	"ARGENTINA                "	"KrtxggNsT247cfR7kgQeqlff3UuV3ntO2O4V7"	"11-288-140-7306"	" cajole slyly: fluffily unusual forges cajole closely excuses. regular requests are car"
88561	"Customer#000088561"	432414.4599	5748.66	"BRAZIL                   "	"yZf,sUaIXAj96VD"	"12-940-159-1382"	"nal ideas. ironic forges boost final theodolites. regular theodolites sleep quickly. frays along the slyly iron"
148096	"Customer#000148096"	414511.6748	7424.11	"BRAZIL                   "	"wT,X7eAaHza2MezMhFJV3gvw"	"12-348-875-7461"	" carefully along the regular asymptotes. final asympt"
53578	"Customer#000053578"	414169.1446	5104.21	"JAPAN                    "	"AIMJGZAS09,bi73bd5UI u56EEdq8Y74PrGNsw"	"22-702-522-5799"	" the regular packages cajole after the instructions. furiously regular p"
33859	"Customer#000033859"	414129.0876	736.03	"ETHIOPIA                 "	"gKSkGmDoUQo"	"15-777-264-6894"	"lithely ironic pinto beans. blithely special theodolites wake. requests sleep? express pack"
116908	"Customer#000116908"	413815.5826	8079.23	"VIETNAM                  "	"kCt00BJVwUSKxuOySsLlLCS"	"31-857-393-5714"	"ending platelets. final dependencies haggle about the ironic, express courts. s"
136261	"Customer#000136261"	411027.9035	6938.35	"CHINA                    "	"kYo34Vg9fmnmO9"	"28-404-274-5637"	"e carefully. slyly regular requests sleep ironic deposits. furiously special requests nag carefully. pending foxes a"
135283	"Customer#000135283"	405256.1568	4815.70	"VIETNAM                  "	"iOMyJSkf4KvhHk"	"31-335-767-1486"	"sleep quickly. slyly final accounts about the blithely regular ideas haggle blithely along the"
33242	"Customer#000033242"	405008.7760	1943.50	"VIETNAM                  "	"SkUOoPzhj TOiD1SOLzXuE2CjVBkDMTGP"	"31-986-245-4915"	"ideas detect above the sauternes. furiously bold accounts haggle fl"
114196	"Customer#000114196"	399714.0085	6143.47	"UNITED KINGDOM           "	"mtxwzFZtNM9CJuXZxByEqKvPY5doW dxf"	"33-448-114-5319"	"sits sleep quickly! carefully bold packages cajole. furiously final courts cajole carefully ac"
29177	"Customer#000029177"	390522.5906	8021.35	"ETHIOPIA                 "	"z2IuNjmQ5tFcKXBdRzeolZ"	"15-847-713-2601"	" the slyly bold packages. even asymptotes nag carefully among the excu"
125224	"Customer#000125224"	388107.1938	4914.39	"KENYA                    "	"58XfDlDHRMv2g2jmWT1"	"24-418-197-2510"	"ily. carefully pending requests nag finally dogged accounts. ruthless, express deposits can wake furiously among t"
129826	"Customer#000129826"	386727.9483	3140.91	"MOROCCO                  "	"4GjFPTcfkiqDXz0I1l7wIbKy 6uEkuAPsm0Gq"	"25-392-585-6654"	"express deposits integrate above the express packages. furio"
137560	"Customer#000137560"	385141.5356	7740.63	"SAUDI ARABIA             "	"nHreto1raUjP3"	"30-834-144-7543"	"ns. slyly even dolphins above the bold requests solve along the deposits. slyl"
39133	"Customer#000039133"	376593.8134	9998.87	"KENYA                    "	"qM8wFLRAvta"	"24-625-551-5999"	"fluffily final pinto beans are final excuses. final, "
68104	"Customer#000068104"	374649.5796	1406.31	"JORDAN                   "	"CF3NdL0EwkK"	"23-674-353-2981"	"ely final dependencies nod bold, even packages. furiously ev"
59701	"Customer#000059701"	374561.7646	-826.85	"MOZAMBIQUE               "	"3ozfZWPYiBeYjFZO MupLVNHw"	"26-422-892-1848"	"pinto beans nag never express accounts. final, sil"
73081	"Customer#000073081"	374412.6512	1137.00	"IRAN                     "	"IwsV4dgelg8 ,hQUqCb8eX4tyW3xX1EWaeWc2"	"20-566-486-8077"	"aggle blithely above the regular theodolites. quickly ironic dolphins along the even, s"
109168	"Customer#000109168"	519856.2131	2947.40	"CHINA                    "	"9HqXcL6X4eyYc4OUd"	"28-198-666-9028"	"efully final, regular asymptotes. quickly ironic packages cajole carefully. blithely final platelets wak"
30523	"Customer#000030523"	465052.1889	3463.98	"PERU                     "	"3A8vNPnR5e0Kz9ytD87XIfSGQZpVThT6RMTkd7"	"27-228-232-7684"	"s x-ray slyly. quickly unusual requests nag slyly at the unusual foxes. regular asymptotes haggle. brave, ir"
116560	"Customer#000116560"	453467.6456	5025.79	"ARGENTINA                "	"KrtxggNsT247cfR7kgQeqlff3UuV3ntO2O4V7"	"11-288-140-7306"	" cajole slyly: fluffily unusual forges cajole closely excuses. regular requests are car"
88561	"Customer#000088561"	432414.4599	5748.66	"BRAZIL                   "	"yZf,sUaIXAj96VD"	"12-940-159-1382"	"nal ideas. ironic forges boost final theodolites. regular theodolites sleep quickly. frays along the slyly iron"
148096	"Customer#000148096"	414511.6748	7424.11	"BRAZIL                   "	"wT,X7eAaHza2MezMhFJV3gvw"	"12-348-875-7461"	" carefully along the regular asymptotes. final asympt"
53578	"Customer#000053578"	414169.1446	5104.21	"JAPAN                    "	"AIMJGZAS09,bi73bd5UI u56EEdq8Y74PrGNsw"	"22-702-522-5799"	" the regular packages cajole after the instructions. furiously regular p"
33859	"Customer#000033859"	414129.0876	736.03	"ETHIOPIA                 "	"gKSkGmDoUQo"	"15-777-264-6894"	"lithely ironic pinto beans. blithely special theodolites wake. requests sleep? express pack"
116908	"Customer#000116908"	413815.5826	8079.23	"VIETNAM                  "	"kCt00BJVwUSKxuOySsLlLCS"	"31-857-393-5714"	"ending platelets. final dependencies haggle about the ironic, express courts. s"
136261	"Customer#000136261"	411027.9035	6938.35	"CHINA                    "	"kYo34Vg9fmnmO9"	"28-404-274-5637"	"e carefully. slyly regular requests sleep ironic deposits. furiously special requests nag carefully. pending foxes a"
135283	"Customer#000135283"	405256.1568	4815.70	"VIETNAM                  "	"iOMyJSkf4KvhHk"	"31-335-767-1486"	"sleep quickly. slyly final accounts about the blithely regular ideas haggle blithely along the"
33242	"Customer#000033242"	405008.7760	1943.50	"VIETNAM                  "	"SkUOoPzhj TOiD1SOLzXuE2CjVBkDMTGP"	"31-986-245-4915"	"ideas detect above the sauternes. furiously bold accounts haggle fl"
114196	"Customer#000114196"	399714.0085	6143.47	"UNITED KINGDOM           "	"mtxwzFZtNM9CJuXZxByEqKvPY5doW dxf"	"33-448-114-5319"	"sits sleep quickly! carefully bold packages cajole. furiously final courts cajole carefully ac"
29177	"Customer#000029177"	390522.5906	8021.35	"ETHIOPIA                 "	"z2IuNjmQ5tFcKXBdRzeolZ"	"15-847-713-2601"	" the slyly bold packages. even asymptotes nag carefully among the excu"
125224	"Customer#000125224"	388107.1938	4914.39	"KENYA                    "	"58XfDlDHRMv2g2jmWT1"	"24-418-197-2510"	"ily. carefully pending requests nag finally dogged accounts. ruthless, express deposits can wake furiously among t"
129826	"Customer#000129826"	386727.9483	3140.91	"MOROCCO                  "	"4GjFPTcfkiqDXz0I1l7wIbKy 6uEkuAPsm0Gq"	"25-392-585-6654"	"express deposits integrate above the express packages. furio"
137560	"Customer#000137560"	385141.5356	7740.63	"SAUDI ARABIA             "	"nHreto1raUjP3"	"30-834-144-7543"	"ns. slyly even dolphins above the bold requests solve along the deposits. slyl"
39133	"Customer#000039133"	376593.8134	9998.87	"KENYA                    "	"qM8wFLRAvta"	"24-625-551-5999"	"fluffily final pinto beans are final excuses. final, "
68104	"Customer#000068104"	374649.5796	1406.31	"JORDAN                   "	"CF3NdL0EwkK"	"23-674-353-2981"	"ely final dependencies nod bold, even packages. furiously ev"
59701	"Customer#000059701"	374561.7646	-826.85	"MOZAMBIQUE               "	"3ozfZWPYiBeYjFZO MupLVNHw"	"26-422-892-1848"	"pinto beans nag never express accounts. final, sil"
73081	"Customer#000073081"	374412.6512	1137.00	"IRAN                     "	"IwsV4dgelg8 ,hQUqCb8eX4tyW3xX1EWaeWc2"	"20-566-486-8077"	"aggle blithely above the regular theodolites. quickly ironic dolphins along the even, s"
"""
Q10_actual_output = """But the actual output should be as follows:

109168	"Customer#000109168"	1039712.4262	2947.40	"CHINA                    "	"9HqXcL6X4eyYc4OUd"	"28-198-666-9028"	"efully final, regular asymptotes. quickly ironic packages cajole carefully. blithely final platelets wak"
30523	"Customer#000030523"	930104.3778	3463.98	"PERU                     "	"3A8vNPnR5e0Kz9ytD87XIfSGQZpVThT6RMTkd7"	"27-228-232-7684"	"s x-ray slyly. quickly unusual requests nag slyly at the unusual foxes. regular asymptotes haggle. brave, ir"
116560	"Customer#000116560"	906935.2912	5025.79	"ARGENTINA                "	"KrtxggNsT247cfR7kgQeqlff3UuV3ntO2O4V7"	"11-288-140-7306"	" cajole slyly: fluffily unusual forges cajole closely excuses. regular requests are car"
88561	"Customer#000088561"	864828.9198	5748.66	"BRAZIL                   "	"yZf,sUaIXAj96VD"	"12-940-159-1382"	"nal ideas. ironic forges boost final theodolites. regular theodolites sleep quickly. frays along the slyly iron"
148096	"Customer#000148096"	829023.3496	7424.11	"BRAZIL                   "	"wT,X7eAaHza2MezMhFJV3gvw"	"12-348-875-7461"	" carefully along the regular asymptotes. final asympt"
53578	"Customer#000053578"	828338.2892	5104.21	"JAPAN                    "	"AIMJGZAS09,bi73bd5UI u56EEdq8Y74PrGNsw"	"22-702-522-5799"	" the regular packages cajole after the instructions. furiously regular p"
33859	"Customer#000033859"	828258.1752	736.03	"ETHIOPIA                 "	"gKSkGmDoUQo"	"15-777-264-6894"	"lithely ironic pinto beans. blithely special theodolites wake. requests sleep? express pack"
116908	"Customer#000116908"	827631.1652	8079.23	"VIETNAM                  "	"kCt00BJVwUSKxuOySsLlLCS"	"31-857-393-5714"	"ending platelets. final dependencies haggle about the ironic, express courts. s"
136261	"Customer#000136261"	822055.8070	6938.35	"CHINA                    "	"kYo34Vg9fmnmO9"	"28-404-274-5637"	"e carefully. slyly regular requests sleep ironic deposits. furiously special requests nag carefully. pending foxes a"
135283	"Customer#000135283"	810512.3136	4815.70	"VIETNAM                  "	"iOMyJSkf4KvhHk"	"31-335-767-1486"	"sleep quickly. slyly final accounts about the blithely regular ideas haggle blithely along the"
33242	"Customer#000033242"	810017.5520	1943.50	"VIETNAM                  "	"SkUOoPzhj TOiD1SOLzXuE2CjVBkDMTGP"	"31-986-245-4915"	"ideas detect above the sauternes. furiously bold accounts haggle fl"
114196	"Customer#000114196"	799428.0170	6143.47	"UNITED KINGDOM           "	"mtxwzFZtNM9CJuXZxByEqKvPY5doW dxf"	"33-448-114-5319"	"sits sleep quickly! carefully bold packages cajole. furiously final courts cajole carefully ac"
29177	"Customer#000029177"	781045.1812	8021.35	"ETHIOPIA                 "	"z2IuNjmQ5tFcKXBdRzeolZ"	"15-847-713-2601"	" the slyly bold packages. even asymptotes nag carefully among the excu"
125224	"Customer#000125224"	776214.3876	4914.39	"KENYA                    "	"58XfDlDHRMv2g2jmWT1"	"24-418-197-2510"	"ily. carefully pending requests nag finally dogged accounts. ruthless, express deposits can wake furiously among t"
129826	"Customer#000129826"	773455.8966	3140.91	"MOROCCO                  "	"4GjFPTcfkiqDXz0I1l7wIbKy 6uEkuAPsm0Gq"	"25-392-585-6654"	"express deposits integrate above the express packages. furio"
137560	"Customer#000137560"	770283.0712	7740.63	"SAUDI ARABIA             "	"nHreto1raUjP3"	"30-834-144-7543"	"ns. slyly even dolphins above the bold requests solve along the deposits. slyl"
39133	"Customer#000039133"	753187.6268	9998.87	"KENYA                    "	"qM8wFLRAvta"	"24-625-551-5999"	"fluffily final pinto beans are final excuses. final, "
68104	"Customer#000068104"	749299.1592	1406.31	"JORDAN                   "	"CF3NdL0EwkK"	"23-674-353-2981"	"ely final dependencies nod bold, even packages. furiously ev"
59701	"Customer#000059701"	749123.5292	-826.85	"MOZAMBIQUE               "	"3ozfZWPYiBeYjFZO MupLVNHw"	"26-422-892-1848"	"pinto beans nag never express accounts. final, sil"
73081	"Customer#000073081"	748825.3024	1137.00	"IRAN                     "	"IwsV4dgelg8 ,hQUqCb8eX4tyW3xX1EWaeWc2"	"20-566-486-8077"	"aggle blithely above the regular theodolites. quickly ironic dolphins along the even, s"

Fix the seed query.
"""

Q11_text = """The Query finds, from scanning the available stock of suppliers in India, all
the parts that represent 0.001% of the total value of all available parts. The query displays the part
number and the value of those parts in descending order of value.
"""
Q11_seed = """SELECT ps_partkey, n_name, MIN(partsupp.ps_availqty), MIN(partsupp.ps_supplycost)
       FROM nation, supplier, partsupp
       WHERE supplier.s_nationkey = nation.n_nationkey 
       AND supplier.s_suppkey = partsupp.ps_suppkey 
       AND nation.n_name = 'INDIA'
       GROUP BY partsupp.ps_partkey, nation.n_name
       HAVING MIN(partsupp.ps_availqty) >= 1 AND MIN(partsupp.ps_supplycost) >= 0.01;"""
Q11_seed_output = """The above seed query produces the following output (first 100 rows):
 11	"INDIA                    "	4540	709.87
14	"INDIA                    "	5278	650.07
16	"INDIA                    "	854	781.91
24	"INDIA                    "	5318	62.15
28	"INDIA                    "	302	690.30
31	"INDIA                    "	1951	120.99
38	"INDIA                    "	1226	570.11
39	"INDIA                    "	6259	737.86
50	"INDIA                    "	2104	107.17
56	"INDIA                    "	1330	52.29
57	"INDIA                    "	4583	137.68
59	"INDIA                    "	8374	357.22
60	"INDIA                    "	6642	800.72
62	"INDIA                    "	5896	348.82
64	"INDIA                    "	9110	602.65
67	"INDIA                    "	7908	546.75
68	"INDIA                    "	6762	5.16
72	"INDIA                    "	2654	762.61
78	"INDIA                    "	1801	434.34
80	"INDIA                    "	4034	797.05
88	"INDIA                    "	9979	81.82
92	"INDIA                    "	3199	91.63
99	"INDIA                    "	7567	496.93
101	"INDIA                    "	5589	305.40
113	"INDIA                    "	4692	141.48
119	"INDIA                    "	4955	488.93
123	"INDIA                    "	5638	107.03
126	"INDIA                    "	5458	929.43
135	"INDIA                    "	6940	465.82
138	"INDIA                    "	2535	885.35
139	"INDIA                    "	1042	972.23
142	"INDIA                    "	3076	860.55
143	"INDIA                    "	1952	199.37
149	"INDIA                    "	7392	266.53
161	"INDIA                    "	9679	688.47
164	"INDIA                    "	3245	814.67
173	"INDIA                    "	6162	877.84
186	"INDIA                    "	7898	812.37
194	"INDIA                    "	377	430.21
207	"INDIA                    "	7316	557.72
209	"INDIA                    "	1513	507.38
210	"INDIA                    "	4516	259.05
213	"INDIA                    "	9377	797.64
225	"INDIA                    "	4421	116.17
226	"INDIA                    "	966	931.43
231	"INDIA                    "	5360	308.95
234	"INDIA                    "	3807	438.37
237	"INDIA                    "	7643	255.01
239	"INDIA                    "	4889	168.50
240	"INDIA                    "	6917	91.89
242	"INDIA                    "	2514	208.39
244	"INDIA                    "	5942	148.84
246	"INDIA                    "	3607	194.36
248	"INDIA                    "	8054	425.78
266	"INDIA                    "	1533	379.56
270	"INDIA                    "	5513	559.02
281	"INDIA                    "	935	587.67
285	"INDIA                    "	8086	835.59
289	"INDIA                    "	4570	715.63
294	"INDIA                    "	5459	642.44
302	"INDIA                    "	6118	885.76
307	"INDIA                    "	1809	354.10
320	"INDIA                    "	7644	663.54
324	"INDIA                    "	8571	575.93
325	"INDIA                    "	511	457.53
330	"INDIA                    "	9145	539.12
345	"INDIA                    "	2457	786.54
346	"INDIA                    "	6697	26.92
347	"INDIA                    "	5863	8.56
362	"INDIA                    "	6711	357.00
381	"INDIA                    "	6258	10.65
386	"INDIA                    "	5970	203.07
388	"INDIA                    "	2913	582.22
393	"INDIA                    "	1801	698.08
420	"INDIA                    "	9500	738.91
431	"INDIA                    "	5332	226.17
434	"INDIA                    "	5295	583.57
444	"INDIA                    "	3183	494.99
445	"INDIA                    "	9549	641.72
454	"INDIA                    "	6906	195.33
456	"INDIA                    "	8189	50.22
516	"INDIA                    "	6105	600.29
517	"INDIA                    "	1205	263.56
521	"INDIA                    "	7639	728.23
539	"INDIA                    "	9489	22.09
546	"INDIA                    "	4052	218.77
548	"INDIA                    "	8170	721.14
550	"INDIA                    "	4396	27.02
553	"INDIA                    "	7642	433.01
556	"INDIA                    "	3441	637.90
562	"INDIA                    "	2106	269.54
564	"INDIA                    "	3416	505.71
565	"INDIA                    "	4922	359.95
571	"INDIA                    "	5772	123.84
586	"INDIA                    "	4787	104.34
590	"INDIA                    "	1278	812.56
595	"INDIA                    "	3290	170.95
598	"INDIA                    "	30	696.54
599	"INDIA                    "	272	19.55
600	"INDIA                    "	5063	97.67
"""
Q11_actual_output = """But the actual query should produce the following output in the first 100 rows:

18488	"INDIA                    "	19174541.94
144829	"INDIA                    "	17135050.20
190890	"INDIA                    "	16161446.30
161073	"INDIA                    "	15922727.76
164317	"INDIA                    "	15644069.74
90101	"INDIA                    "	15625869.68
83048	"INDIA                    "	15538328.11
153736	"INDIA                    "	15508971.80
4034	"INDIA                    "	15467341.92
53934	"INDIA                    "	15049974.47
31030	"INDIA                    "	14971930.41
20546	"INDIA                    "	14894720.75
90302	"INDIA                    "	14850545.24
191496	"INDIA                    "	14633952.90
94361	"INDIA                    "	14616285.20
109262	"INDIA                    "	14565469.80
64702	"INDIA                    "	14441808.47
26444	"INDIA                    "	14433196.77
192862	"INDIA                    "	14196434.62
77553	"INDIA                    "	14153465.50
71548	"INDIA                    "	14004323.26
172714	"INDIA                    "	13968532.17
191431	"INDIA                    "	13734318.33
17739	"INDIA                    "	13690503.01
21876	"INDIA                    "	13656979.44
9034	"INDIA                    "	13511965.62
150847	"INDIA                    "	13396116.52
112638	"INDIA                    "	13348431.76
77643	"INDIA                    "	13276206.09
40131	"INDIA                    "	13234395.53
174820	"INDIA                    "	13194373.09
120234	"INDIA                    "	13174796.94
445	"INDIA                    "	13067392.32
176425	"INDIA                    "	13063923.31
197395	"INDIA                    "	12981969.21
92104	"INDIA                    "	12808623.15
158086	"INDIA                    "	12770522.75
168417	"INDIA                    "	12719649.46
191798	"INDIA                    "	12700646.65
195686	"INDIA                    "	12686302.73
96534	"INDIA                    "	12681861.55
159390	"INDIA                    "	12631786.34
100058	"INDIA                    "	12537249.57
34088	"INDIA                    "	12460615.04
143095	"INDIA                    "	12389065.18
67713	"INDIA                    "	12372536.00
166370	"INDIA                    "	12263381.62
15680	"INDIA                    "	12221638.69
175636	"INDIA                    "	12178581.75
114208	"INDIA                    "	12152882.10
117539	"INDIA                    "	12080773.77
172128	"INDIA                    "	12071351.80
3046	"INDIA                    "	12069953.30
16322	"INDIA                    "	12034579.48
40890	"INDIA                    "	12008546.53
143071	"INDIA                    "	12007955.49
48913	"INDIA                    "	11997334.26
118789	"INDIA                    "	11953448.23
2348	"INDIA                    "	11918765.08
177521	"INDIA                    "	11912783.25
184812	"INDIA                    "	11907931.99
199610	"INDIA                    "	11907718.24
146876	"INDIA                    "	11852239.04
186479	"INDIA                    "	11837118.26
173216	"INDIA                    "	11822897.01
15756	"INDIA                    "	11787465.70
97529	"INDIA                    "	11779480.19
41436	"INDIA                    "	11752500.65
14742	"INDIA                    "	11728370.94
174980	"INDIA                    "	11724460.40
99436	"INDIA                    "	11716653.62
128098	"INDIA                    "	11687444.32
13248	"INDIA                    "	11685383.76
47730	"INDIA                    "	11641450.73
177497	"INDIA                    "	11635582.42
103162	"INDIA                    "	11616976.71
99664	"INDIA                    "	11599517.20
186425	"INDIA                    "	11583901.30
175965	"INDIA                    "	11533005.95
179769	"INDIA                    "	11527910.32
142135	"INDIA                    "	11511373.75
81463	"INDIA                    "	11383887.29
181529	"INDIA                    "	11379335.37
88528	"INDIA                    "	11379051.56
22576	"INDIA                    "	11338105.60
105040	"INDIA                    "	11320447.16
146408	"INDIA                    "	11289037.13
93409	"INDIA                    "	11226166.09
35924	"INDIA                    "	11226074.00
95689	"INDIA                    "	11221234.80
170420	"INDIA                    "	11219830.84
79834	"INDIA                    "	11167488.22
65840	"INDIA                    "	11162658.60
198886	"INDIA                    "	11161349.20
141693	"INDIA                    "	11139793.20
13194	"INDIA                    "	11106633.26
181413	"INDIA                    "	11088579.67
75744	"INDIA                    "	11070084.72
98943	"INDIA                    "	11039316.76
109035	"INDIA                    "	11027669.66

Fix the seed query."""
Q11_feedback1 = """You produced the following query.
It does not produce any row in the result.
Fix the query.
Verify whether all the predicates implied in the text are present in the query.
Consider having filter and join predicates inside the inner query as well."""

Q12_text = """The Query counts, by ship mode, for line items actually received by customers in
the year 1995, the number of line items belonging to orders for which the receiptdate exceeds the commitdate for
two different specified ship modes. Only line items that were actually shipped before the commitdate are considered. 
The late line items are partitioned into two groups, those with priority URGENT or HIGH, and those with a
priority other than URGENT or HIGH.
"""
Q12_seed = """(Select sl_shipmode, 0 as high_line_count, Count(*) as low_line_count 
 From orders, store_lineitem 
 Where orders.o_orderkey = store_lineitem.sl_orderkey
 and store_lineitem.sl_shipdate < store_lineitem.sl_commitdate
 and store_lineitem.sl_commitdate < store_lineitem.sl_receiptdate
 and store_lineitem.sl_shipmode IN ('SHIP', 'TRUCK')
 and store_lineitem.sl_receiptdate between '1995-01-01' and '1995-12-31'
group by sl_shipmode)
 UNION ALL  
 (Select wl_shipmode, 0 as high_line_count, Count(*) as low_line_count 
 From orders, web_lineitem 
 Where orders.o_orderkey = web_lineitem.wl_orderkey
 and web_lineitem.wl_shipdate < web_lineitem.wl_commitdate
 and web_lineitem.wl_commitdate < web_lineitem.wl_receiptdate
 and web_lineitem.wl_shipmode IN ('SHIP', 'TRUCK')
 and web_lineitem.wl_receiptdate between '1995-01-01' and '1995-12-31'
 group by wl_shipmode);"""
Q12_seed_output = """The above seed query produces the following output: 
"SHIP      "	0	7778
"TRUCK     "	0	7890
"SHIP      "	0	7778
"TRUCK     "	0	7890
"""
Q12_actual_output = """But the actual query should produce the following output:
"SHIP      "	6176	9380
"TRUCK     "	6392	9388

Fix the seed query."""
Q12_feedback1 = """You produced the following query:
(SELECT sl_shipmode, 
        SUM(CASE WHEN o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS high_line_count, 
        SUM(CASE WHEN o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS low_line_count 
 FROM orders, store_lineitem 
 WHERE orders.o_orderkey = store_lineitem.sl_orderkey
   AND store_lineitem.sl_shipdate < store_lineitem.sl_commitdate
   AND store_lineitem.sl_commitdate < store_lineitem.sl_receiptdate
   AND store_lineitem.sl_shipmode IN ('SHIP', 'TRUCK')
   AND store_lineitem.sl_receiptdate BETWEEN '1995-01-01' AND '1995-12-31'
 GROUP BY sl_shipmode)
UNION ALL  
(SELECT wl_shipmode, 
        SUM(CASE WHEN o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS high_line_count, 
        SUM(CASE WHEN o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS low_line_count 
 FROM orders, web_lineitem 
 WHERE orders.o_orderkey = web_lineitem.wl_orderkey
   AND web_lineitem.wl_shipdate < web_lineitem.wl_commitdate
   AND web_lineitem.wl_commitdate < web_lineitem.wl_receiptdate
   AND web_lineitem.wl_shipmode IN ('SHIP', 'TRUCK')
   AND web_lineitem.wl_receiptdate BETWEEN '1995-01-01' AND '1995-12-31'
 GROUP BY wl_shipmode);

It gives the following output:
"SHIP      "	3088	4690
"TRUCK     "	3196	4694
"SHIP      "	3088	4690
"TRUCK     "	3196	4694.
Looks like one more grouping is do to be done.
Consider doing union first and then group by.
Fix the query."""

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

Q15_text = """The Query finds the supplier who contributed the most to the overall revenue for parts shipped during
the first quarter of 1995. In case of a tie, the query lists all suppliers whose contribution was equal to the
maximum, presented in supplier number order. Revenue is calculated as sum(extended price*(1-discount)).
"""
Q15_seed = """Here are some hints on the query.
(Select Max(wl_suppkey) as s_suppkey, s_name, s_address, s_phone, Sum(wl_extendedprice*(1 - wl_discount)) as total_revenue 
 From supplier, web_lineitem 
 Where supplier.s_suppkey = web_lineitem.wl_suppkey
 and web_lineitem.wl_shipdate between '1995-01-01' and '1995-01-31'
 group by s_name, s_address, s_phone)
 UNION ALL  
 (Select Max(sl_suppkey) as s_suppkey, s_name, s_address, s_phone, Sum(sl_extendedprice*(1 - sl_discount)) as total_revenue 
 From store_lineitem, supplier 
 Where store_lineitem.sl_suppkey = supplier.s_suppkey
 and store_lineitem.sl_shipdate between '1995-01-01' and '1995-01-31'
 group by s_name, s_address, s_phone);
 """
Q15_seed_output = """ The seed query produces the following output (first 100 rows): 
1559	"Supplier#000001559       "	"cTgcN,Vi N"	"17-553-423-6440"	204029.9644
1528	"Supplier#000001528       "	"it4EmP9yhW0IZvUnapbYfoKXVvY2"	"13-570-757-5808"	275455.7616
4818	"Supplier#000004818       "	" pq 8W7GgiPQEHo4qMvREULChRuKqxSP9GQm2ox"	"14-584-467-9693"	80264.3490
4759	"Supplier#000004759       "	"bxwpbOzAFn2goILzqnWso"	"28-520-218-8613"	149383.4646
3867	"Supplier#000003867       "	"Vl0it8GSnzR3rthsVcI5bk2qxEi"	"10-523-466-5029"	240073.1633
3414	"Supplier#000003414       "	"loSCvoLk0uUDFaCvql3yt5tHp6P1a"	"30-252-605-4415"	254902.4632
2581	"Supplier#000002581       "	"JNhWdkCsY913kACx9zOSkDnj2kqnyH"	"24-129-434-1961"	157632.0462
788	"Supplier#000000788       "	"jL QLbG475Uszs4 2RxBp4oR"	"30-377-394-1108"	173025.0691
488	"Supplier#000000488       "	"AzfObar4VYwnQvsGbISGCshVM AIWYq9"	"23-609-606-3265"	472276.5929
626	"Supplier#000000626       "	"uaYN3Mg6sVtD"	"23-672-133-9305"	186703.1658
2601	"Supplier#000002601       "	"O,xUQnlTenpSKG4f9SBXcWpr1Yb"	"10-898-717-9719"	289316.9714
1907	"Supplier#000001907       "	"ybAHrX,OnohkqolW ZBY4YhW63IRCUtuIehrJk8J"	"30-901-567-4832"	456761.9070
3059	"Supplier#000003059       "	"4L5nNuegCdCY3m,88BFVEut28TFlbZ6hHCj"	"24-184-213-4553"	40651.0995
1333	"Supplier#000001333       "	"gKunwze79R7QX8j"	"17-962-950-1375"	496383.4482
1574	"Supplier#000001574       "	"010G9zS4hWlWkl1X3 F7Ha2 Vnq2Qc2"	"23-997-787-7515"	265654.9187
2240	"Supplier#000002240       "	"hTJM3PXSdUuF0wFb0geX"	"12-390-860-8618"	438020.7881
3017	"Supplier#000003017       "	"45FE 2ykESNBNe4Sp b"	"14-285-994-8319"	392420.4972
1014	"Supplier#000001014       "	"8o3SbVMNnBM1foNWeoiVXoPpLhhZcc6tB"	"12-528-127-4486"	253684.0798
3896	"Supplier#000003896       "	"mWr2VTZVOlAdk6RXnSAPSNHL3Xdu3FfnrPQh"	"29-261-495-9146"	349147.0126
2000	"Supplier#000002000       "	"b1,LW1CcQX7Mp2RQgKXLrKGvuJmM"	"21-860-645-7227"	362476.3550
3364	"Supplier#000003364       "	"5p6HNshx2rf4EKNYzm9Y1vU3"	"26-278-540-7484"	87170.8975
206	"Supplier#000000206       "	"hva5xXEn5j7H27CdKcjFqi,QnunUZyt8,a"	"29-156-330-8311"	213188.4672
2195	"Supplier#000002195       "	"aDZzLrVyjTGTrQkFbZIsVRz HvhC4Qvd"	"28-154-128-8030"	245164.5741
2679	"Supplier#000002679       "	"MaiB0O77I5cCcE9w"	"14-228-859-6923"	205316.4659
3669	"Supplier#000003669       "	"yxO31BBDfe5vIKGwLWhZGsWzngdAgs8oSq6L"	"33-852-933-2119"	360914.4410
3849	"Supplier#000003849       "	"KgbZEaRk,6Q3mWvwh6uptrs1KRUHg 0"	"13-582-965-9117"	128591.7915
2719	"Supplier#000002719       "	"4nnzQI2CbqREQUuIsXTBVUkaP4mNS3"	"13-241-286-9786"	168175.6385
2921	"Supplier#000002921       "	"S1UO1Dfj5y0kG0ui6gJ30yt5CUVaebtyIlRSp"	"10-392-215-8665"	264817.4324
2208	"Supplier#000002208       "	"N3a93D2ALpD4hf 8DicAAi KZG"	"14-369-691-4892"	455246.2758
149	"Supplier#000000149       "	"pVyWsjOidpHKp4NfKU4yLeym"	"16-660-553-2456"	199249.4052
153	"Supplier#000000153       "	"qkCHAU1v9CtEBOGXhdFXJsx5L5gViVm5k,"	"19-346-843-5260"	424435.7317
565	"Supplier#000000565       "	",oYB9wlD3mtL lj3PJC67a RGXaqh69sHK5G4e"	"30-487-989-9411"	60801.0918
145	"Supplier#000000145       "	"pSsVRBOlNKVuvXcjAEKxxy0hD8kA aZf"	"16-136-582-9756"	261958.5550
3789	"Supplier#000003789       "	"iDiVfkHeNky"	"11-732-905-1016"	188963.5953
3629	"Supplier#000003629       "	"fco32LPL55pLFoH0"	"28-780-309-4610"	461024.2724
3861	"Supplier#000003861       "	"kiZ4YdXV0Ah,kRkyIJ17R6Qh"	"21-850-998-6046"	232950.2762
1179	"Supplier#000001179       "	"l9JHnPjgaxrY11cPiCE"	"28-156-806-5348"	383605.9158
3886	"Supplier#000003886       "	"c,VP75QAJqi ig1gNOWsxXdsuZQbi4ZJ5 3"	"34-418-995-7074"	280850.9316
1289	"Supplier#000001289       "	"xrtlwRNxzz 8J,6"	"18-840-902-3456"	330317.6159
2683	"Supplier#000002683       "	"WMUccHizpga,Z2BSAV"	"17-754-432-1167"	191762.7157
2743	"Supplier#000002743       "	"Ogj4Ndxtu7FARpgYmpeQMjoAnYlIi"	"20-176-647-8475"	208417.9130
1685	"Supplier#000001685       "	"YZZuwx45JDn6 QsARZq"	"26-877-710-5379"	111460.1084
1307	"Supplier#000001307       "	"MPZXf,9fmn0H5Bgtfyv2c74pCy"	"27-897-389-5613"	277859.9316
4272	"Supplier#000004272       "	"0CUBdD9KyyE"	"21-375-837-7482"	205402.2794
4249	"Supplier#000004249       "	"dqjRKOF5zt6IBAEHkbniUdyu4kREGJGqQV9"	"15-272-365-4305"	221361.0593
1080	"Supplier#000001080       "	"RPVbFa a5wCXrpH Y8V"	"31-148-197-8002"	308125.2413
3048	"Supplier#000003048       "	"Cu nnq0xHJm8MfHUw5KwjlYgI"	"30-282-606-3047"	309744.2258
4414	"Supplier#000004414       "	"DvgX74btfxm83Rxf81LTicpT9Eo vTi 0"	"23-302-983-3738"	195060.3992
2314	"Supplier#000002314       "	"iU5DZSv3 i04wiV,cTxg0joJLG5AJzVxMdtHW"	"10-848-716-8078"	159823.0904
1350	"Supplier#000001350       "	"JeFDZXbYECk"	"23-431-834-8413"	376672.9593
2722	"Supplier#000002722       "	"YlPsKWwJQTZj5aAq4XTVnYDBZaw9"	"29-868-338-3436"	493951.0595
3643	"Supplier#000003643       "	"DPMM fUKicM rvf3pg27FFtux,XG07bcom5o "	"28-892-419-1661"	412551.8009
1282	"Supplier#000001282       "	"UA gsripU3RMLvlJ"	"12-744-273-6433"	85002.2986
4963	"Supplier#000004963       "	"F5naCEAdQhhjm3IwJsla7 OF6mMbbjtH90"	"24-756-311-4817"	199946.4774
2026	"Supplier#000002026       "	"ZJzy4wu,lfoZugo6,cRgWvbca gFv4SzximYFeG"	"24-814-878-2691"	231664.3372
3010	"Supplier#000003010       "	"vOeYAusMVN9HDeDJZuvr9nC0E1a"	"11-900-702-4200"	148510.1252
453	"Supplier#000000453       "	"bpt98PxU5HSQt61bVB695JPjBmJKUv hNzQeHvC"	"31-882-179-6015"	383841.5961
3836	"Supplier#000003836       "	"tdBz4J0l7wDJJu Dej1"	"16-958-229-2290"	266318.1655
2803	"Supplier#000002803       "	"6pUdfifEqaKeD3rScPUlSApW1TbxYlA5NleGhl"	"11-452-177-8866"	492650.5959
4208	"Supplier#000004208       "	"x3jZYF7ZYN 8a4LY1c1kEsh"	"21-468-998-1571"	281296.9845
980	"Supplier#000000980       "	"jfgiJfywBW88ZEYM 5V"	"23-105-829-3910"	317521.6427
1355	"Supplier#000001355       "	"OmfBiXdFaTc"	"12-815-691-5142"	296834.0398
1487	"Supplier#000001487       "	"0ISabDJ8Fj7H8qu3qPvx"	"28-878-390-8065"	368173.9737
4478	"Supplier#000004478       "	"zkONvYIl3m8"	"15-839-281-1312"	218278.9653
852	"Supplier#000000852       "	"n3zasd04WljXdo9xMjQRkZKrEB"	"11-574-892-3228"	294850.9465
3871	"Supplier#000003871       "	"0l9vr6DDagytSNo,SD2mkG4XezH8L1loBW7uXV"	"10-678-935-9330"	185494.6337
3948	"Supplier#000003948       "	"XYiD 7s4dikkL8B9"	"15-543-886-7762"	150303.9137
949	"Supplier#000000949       "	"a,UE,6nRVl2fCphkOoetR1ajIzAEJ1Aa1G1HV"	"33-332-697-2768"	393401.6226
4765	"Supplier#000004765       "	"LCJ8Mtc6bGYDK7Kg5EKq1tudlQpkT54R8R8j"	"14-523-229-1984"	140051.6038
4238	"Supplier#000004238       "	"ycSsD71UyOr"	"10-689-876-6705"	570893.0966
2631	"Supplier#000002631       "	"jbO, cOqoJTHQAetYnUNOKa3U"	"28-153-887-4167"	226962.3896
4258	"Supplier#000004258       "	"oXKtTTKlpcYIbuiMgfnP0sWD2P2Ngas"	"33-173-309-5477"	435411.9214
2500	"Supplier#000002500       "	"3E6egPL3NCIN4Vawqt0pxUoH7ji Zp"	"28-937-418-6930"	313244.3106
1266	"Supplier#000001266       "	"lwHRpzssxFZW9"	"20-985-871-8162"	27616.7880
3715	"Supplier#000003715       "	"tEgnSYHQkY"	"23-414-190-3025"	241145.5545
3082	"Supplier#000003082       "	"vLhIfPUhiW1Y rYmcj"	"33-680-262-1683"	282728.1721
4467	"Supplier#000004467       "	"7BTW,9Tvb7WeewsQrIUhKsAaaU8pLxHNHpG mN1"	"19-443-184-3520"	145223.3124
3730	"Supplier#000003730       "	"CQwSVgaug86Vhwt"	"19-147-682-6287"	245003.2459
2310	"Supplier#000002310       "	"E FGFgvWTLL3beRdu5V3mEVNaYvW61"	"23-736-684-2154"	244098.9442
2291	"Supplier#000002291       "	"cREl5ZgjwUa"	"11-877-834-5524"	386624.5870
2863	"Supplier#000002863       "	"CKTfjYetZLnm1KxDLjb3Br4Nec"	"18-744-168-9042"	342632.2199
4670	"Supplier#000004670       "	"fnLEhL1yrH7XT4N"	"18-302-357-9648"	303404.2827
4438	"Supplier#000004438       "	"X3YNSKLk1Bmh9OAFND7qUAdEb1I"	"29-834-445-2433"	78105.3685
610	"Supplier#000000610       "	"cRikc,rgxAM3yz0IR85OD"	"30-402-585-4900"	229642.4004
406	"Supplier#000000406       "	"zMhU58CDF4aHTeodxg9IgRZgq"	"31-926-216-4352"	167740.0460
1631	"Supplier#000001631       "	"3JwfERzppDc6h7BV0I"	"22-255-355-8658"	278166.0510
1654	"Supplier#000001654       "	"5F6ZEzmh6PWDS"	"12-219-980-3825"	409857.3706
3117	"Supplier#000003117       "	"awKOkc6y,vmP3jAD6BfL4i"	"18-191-586-4745"	324128.2180
1534	"Supplier#000001534       "	",rRNXQRJQd6JXzpwnEuKkKn"	"30-891-630-2295"	428143.9176
2361	"Supplier#000002361       "	"1p U8A z36oRmNfqN"	"27-333-451-9926"	57937.6140
2435	"Supplier#000002435       "	"2Wg6s 0Y5Mnfuois3iIoaq1wrVETh"	"31-379-702-6071"	350485.5576
370	"Supplier#000000370       "	"yyNSJAG9UXcWit4SeMkEIrNcdVq5"	"10-602-768-3758"	313897.5375
913	"Supplier#000000913       "	"c78mMYZkHE7ktVSoB9D"	"34-601-419-1634"	258912.8464
3790	"Supplier#000003790       "	"vKEajCmMniaZBxwyOb1qwk63IlQJL8bENqQIJn"	"11-386-956-2273"	140554.4534
4338	"Supplier#000004338       "	"fHVztcdO93vdaB2W9p3O67V20"	"18-464-476-3509"	167439.7608
2239	"Supplier#000002239       "	"NL9w9GFCzq6N"	"10-191-563-6127"	252531.6953
4460	"Supplier#000004460       "	"2AqMg2cKVwauYYw8hSpKQEM0M ZuK2ZuWK3T"	"32-777-383-6892"	240052.4582
2277	"Supplier#000002277       "	"uxpnnDRwK1m, "	"17-762-810-4943"	361000.0582
4175	"Supplier#000004175       "	"MVBQawEe,35kLvhEdC8F8tJMWenyu,apYhHEDohw"	"10-920-963-6166"	320703.3293
2376	"Supplier#000002376       "	"dKY1qJIOWUBZF99EXp"	"14-803-406-2891"	222827.0811
"""
Q15_actual_output = """But the actual query should produce the following output:
1181	"Supplier#000001181       "	"e,Ld995DWYXRrqQRLS9MtmWinb64wGm5JjMhRXF"	"19-963-905-7803"	3532163.1474

Give me the correct query.
If there is a common subquery used more than once, make it a CTE with an alias and use it."""
Q15_feedback1 = """You formulated the following query:

WITH combined_revenue AS (
    SELECT wl_suppkey AS s_suppkey, 
           s_name, 
           s_address, 
           s_phone, 
           SUM(wl_extendedprice * (1 - wl_discount)) AS total_revenue
    FROM web_lineitem
    JOIN supplier ON supplier.s_suppkey = web_lineitem.wl_suppkey
    WHERE wl_shipdate BETWEEN '1995-01-01' AND '1995-03-31'
    GROUP BY wl_suppkey, s_name, s_address, s_phone

    UNION ALL

    SELECT sl_suppkey AS s_suppkey, 
           s_name, 
           s_address, 
           s_phone, 
           SUM(sl_extendedprice * (1 - sl_discount)) AS total_revenue
    FROM store_lineitem
    JOIN supplier ON supplier.s_suppkey = store_lineitem.sl_suppkey
    WHERE sl_shipdate BETWEEN '1995-01-01' AND '1995-03-31'
    GROUP BY sl_suppkey, s_name, s_address, s_phone
)

SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
FROM combined_revenue
WHERE total_revenue = (
    SELECT MAX(total_revenue) 
    FROM combined_revenue
)
ORDER BY s_suppkey;

It gives the following output:
1181	"Supplier#000001181       "	"e,Ld995DWYXRrqQRLS9MtmWinb64wGm5JjMhRXF"	"19-963-905-7803"	1766081.5737
1181	"Supplier#000001181       "	"e,Ld995DWYXRrqQRLS9MtmWinb64wGm5JjMhRXF"	"19-963-905-7803"	1766081.5737

Eliminate duplication in result. Fix the query."""
Q15_feedback2 = """You also formulated the following query:
SELECT s_suppkey, s_name, s_address, s_phone, SUM(total_revenue) AS total_revenue
FROM (
    SELECT s.s_suppkey, s.s_name, s.s_address, s.s_phone, SUM(wl.wl_extendedprice * (1 - wl.wl_discount)) AS total_revenue
    FROM supplier s
    JOIN web_lineitem wl ON s.s_suppkey = wl.wl_suppkey
    WHERE wl.wl_shipdate BETWEEN '1995-01-01' AND '1995-03-31'
    GROUP BY s.s_suppkey, s.s_name, s.s_address, s.s_phone
    UNION ALL
    SELECT s.s_suppkey, s.s_name, s.s_address, s.s_phone, SUM(sl.sl_extendedprice * (1 - sl.sl_discount)) AS total_revenue
    FROM supplier s
    JOIN store_lineitem sl ON s.s_suppkey = sl.sl_suppkey
    WHERE sl.sl_shipdate BETWEEN '1995-01-01' AND '1995-03-31'
    GROUP BY s.s_suppkey, s.s_name, s.s_address, s.s_phone
) AS combined_revenue
GROUP BY s_suppkey, s_name, s_address, s_phone
HAVING SUM(total_revenue) = (
    SELECT MAX(total_revenue)
    FROM (
        SELECT s.s_suppkey, SUM(wl.wl_extendedprice * (1 - wl.wl_discount)) AS total_revenue
        FROM supplier s
        JOIN web_lineitem wl ON s.s_suppkey = wl.wl_suppkey
        WHERE wl.wl_shipdate BETWEEN '1995-01-01' AND '1995-03-31'
        GROUP BY s.s_suppkey
        UNION ALL
        SELECT s.s_suppkey, SUM(sl.sl_extendedprice * (1 - sl.sl_discount)) AS total_revenue
        FROM supplier s
        JOIN store_lineitem sl ON s.s_suppkey = sl.sl_suppkey
        WHERE sl.sl_shipdate BETWEEN '1995-01-01' AND '1995-03-31'
        GROUP BY s.s_suppkey
    ) AS max_revenue
)
ORDER BY s_suppkey;

It is not producing any result. Fix the query.

The following query also does not produce any result:
SELECT s_suppkey, s_name, s_address, s_phone, SUM(total_revenue) AS total_revenue
FROM (
    SELECT s.s_suppkey, s.s_name, s.s_address, s.s_phone, SUM(wl.wl_extendedprice * (1 - wl.wl_discount)) AS total_revenue
    FROM supplier s
    JOIN web_lineitem wl ON s.s_suppkey = wl.wl_suppkey
    WHERE wl.wl_shipdate BETWEEN '1995-01-01' AND '1995-03-31'
    GROUP BY s.s_suppkey, s.s_name, s.s_address, s.s_phone
    UNION ALL
    SELECT s.s_suppkey, s.s_name, s.s_address, s.s_phone, SUM(sl.sl_extendedprice * (1 - sl.sl_discount)) AS total_revenue
    FROM supplier s
    JOIN store_lineitem sl ON s.s_suppkey = sl.sl_suppkey
    WHERE sl.sl_shipdate BETWEEN '1995-01-01' AND '1995-03-31'
    GROUP BY s.s_suppkey, s.s_name, s.s_address, s.s_phone
) AS combined_revenue
GROUP BY s_suppkey, s_name, s_address, s_phone
HAVING SUM(total_revenue) = (
    SELECT MAX(total_revenue)
    FROM (
        SELECT s.s_suppkey, SUM(wl.wl_extendedprice * (1 - wl.wl_discount)) AS total_revenue
        FROM supplier s
        JOIN web_lineitem wl ON s.s_suppkey = wl.wl_suppkey
        WHERE wl.wl_shipdate BETWEEN '1995-01-01' AND '1995-03-31'
        GROUP BY s.s_suppkey
        UNION ALL
        SELECT s.s_suppkey, SUM(sl.sl_extendedprice * (1 - sl.sl_discount)) AS total_revenue
        FROM supplier s
        JOIN store_lineitem sl ON s.s_suppkey = sl.sl_suppkey
        WHERE sl.sl_shipdate BETWEEN '1995-01-01' AND '1995-03-31'
        GROUP BY s.s_suppkey
    ) AS max_revenue
)
ORDER BY s_suppkey;

Fix the query. """

Q16_text = """The Query counts the number of suppliers who can supply parts that satisfy a particular
customer's requirements. The customer is interested in parts of sizes 1, 4, and 7 as long as 
they are not like 'MEDIUM POLISHED%', not of `Brand#23'`, and not from a supplier who has had complaints 
registered at the Better Business Bureau.
Results must be presented in descending count and ascending brand, type, and size."""
Q16_seed = """Select p_brand, p_type, p_size, Count(*) as supplier_cnt 
 From part, partsupp, supplier 
 Where part.p_partkey = partsupp.ps_partkey
 and part.p_size IN (1, 4, 7) 
 Group By p_brand, p_size, p_type; 
 Validate all the predicates of the above query against the text description.
 Make sure s_comment based predicate string captures Customer Complaints.
If the seed query has redundant tables, 
where join predicates are missing, may be there is a hidden <> predicate!"""
Q16_seed_output = """The above seed query produces the following output: """
Q16_actual_output = """But the actual query should produce the following output:

Fix the seed query."""
Q16_feedback1 = """You formulated the following SQL query:
SELECT p_brand, p_type, p_size, COUNT(*) AS supplier_cnt
FROM part
JOIN partsupp ON part.p_partkey = partsupp.ps_partkey
JOIN supplier ON partsupp.ps_suppkey = supplier.s_suppkey
WHERE part.p_size IN (1, 4, 7)
AND part.p_type <> 'polished medium'
AND part.p_brand <> 'Brand#23'
AND supplier.s_comment NOT LIKE '%Better Business Bureau%'
GROUP BY p_brand, p_size, p_type
ORDER BY supplier_cnt DESC, p_brand ASC, p_type ASC, p_size ASC;

It produces 7118 rows. But the actual queries return 6878 rows. Fix the query. 
"""

Q17_text = """"""
Q17_seed = """"""
Q17_seed_output = """The above seed query produces the following output: """
Q17_actual_output = """But the actual query should produce the following output:

Fix the seed query."""

Q18_text = """The Query finds a list of the top 100 customers who have ever placed more than 300 orders online.
The query lists the customer name, customer key, the order key, 
date and total price and the quantity for the order."""
Q18_seed = """SELECT c_custkey, c_name, wl_orderkey, SUM(wl_quantity)  
        FROM customer, orders, web_lineitem
        WHERE orders.o_orderkey = web_lineitem.wl_orderkey AND customer.c_custkey = orders.o_custkey
        GROUP BY customer.c_custkey, customer.c_name, web_lineitem.wl_orderkey
        HAVING SUM(web_lineitem.wl_quantity) >= 300.01;"""
Q18_seed_output = """
Output of the seed query is as follows:
3566	"Customer#000003566"	2329187	304.00
12251	"Customer#000012251"	735366	309.00
13072	"Customer#000013072"	1481925	301.00
13940	"Customer#000013940"	2232932	304.00
15631	"Customer#000015631"	1845057	302.00
16384	"Customer#000016384"	502886	312.00
17746	"Customer#000017746"	6882	303.00
24341	"Customer#000024341"	1474818	302.00
50008	"Customer#000050008"	2366755	302.00
53029	"Customer#000053029"	2662214	302.00
64483	"Customer#000064483"	2745894	304.00
66533	"Customer#000066533"	29158	305.00
66790	"Customer#000066790"	2199712	327.00
69904	"Customer#000069904"	1742403	305.00
77260	"Customer#000077260"	1436544	307.00
82441	"Customer#000082441"	857959	305.00
88703	"Customer#000088703"	2995076	302.00
88876	"Customer#000088876"	983201	304.00
105995	"Customer#000105995"	2096705	307.00
113131	"Customer#000113131"	967334	301.00
114586	"Customer#000114586"	551136	308.00
117919	"Customer#000117919"	2869152	317.00
119989	"Customer#000119989"	1544643	320.00
120098	"Customer#000120098"	1971680	308.00
136573	"Customer#000136573"	2761378	301.00
141098	"Customer#000141098"	565574	301.00
141823	"Customer#000141823"	2806245	310.00
147197	"Customer#000147197"	1263015	320.00
148885	"Customer#000148885"	2942469	313.00"""
Q18_actual_output = """
But the actual output of the query is as follows:
"Customer#000013940"	13940	2232932	"1997-04-13"	522720.61	304.00
"Customer#000066790"	66790	2199712	"1996-09-30"	515531.82	327.00
"Customer#000024341"	24341	1474818	"1992-11-15"	491348.26	302.00
"Customer#000050008"	50008	2366755	"1996-12-09"	483891.26	302.00
"Customer#000077260"	77260	1436544	"1992-09-12"	479499.43	307.00
"Customer#000105995"	105995	2096705	"1994-07-03"	469692.58	307.00
"Customer#000148885"	148885	2942469	"1992-05-31"	469630.44	313.00
"Customer#000114586"	114586	551136	"1993-05-19"	469605.59	308.00
"Customer#000147197"	147197	1263015	"1997-02-02"	467149.67	320.00
"Customer#000064483"	64483	2745894	"1996-07-04"	466991.35	304.00
"Customer#000136573"	136573	2761378	"1996-05-31"	461282.73	301.00
"Customer#000016384"	16384	502886	"1994-04-12"	458378.92	312.00
"Customer#000117919"	117919	2869152	"1996-06-20"	456815.92	317.00
"Customer#000012251"	12251	735366	"1993-11-24"	455107.26	309.00
"Customer#000120098"	120098	1971680	"1995-06-14"	453451.23	308.00
"Customer#000088876"	88876	983201	"1993-12-30"	446717.46	304.00
"Customer#000141823"	141823	2806245	"1996-12-29"	446269.12	310.00
"Customer#000053029"	53029	2662214	"1993-08-13"	446144.49	302.00
"Customer#000066533"	66533	29158	"1995-10-21"	443576.50	305.00
"Customer#000003566"	3566	2329187	"1998-01-04"	439803.36	304.00
"Customer#000119989"	119989	1544643	"1997-09-20"	434568.25	320.00
"Customer#000113131"	113131	967334	"1995-12-15"	432957.75	301.00
"Customer#000141098"	141098	565574	"1995-09-24"	430986.69	301.00
"Customer#000015631"	15631	1845057	"1994-05-12"	419879.59	302.00
"Customer#000069904"	69904	1742403	"1996-10-19"	408513.00	305.00
"Customer#000017746"	17746	6882	"1997-04-09"	408446.93	303.00
"Customer#000013072"	13072	1481925	"1998-03-15"	399195.47	301.00
"Customer#000082441"	82441	857959	"1994-02-07"	382579.74	305.00
"Customer#000088703"	88703	2995076	"1994-01-30"	363812.12	302.00

Fix the seed query."""
Q18_feedback1 = """You formulated the below query:
SELECT c.c_custkey, c.c_name, o.o_orderkey, o.o_orderdate, o.o_totalprice, SUM(wl.wl_quantity) AS total_quantity
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN web_lineitem wl ON o.o_orderkey = wl.wl_orderkey
GROUP BY c.c_custkey, c.c_name, o.o_orderkey, o.o_orderdate, o.o_totalprice
HAVING COUNT(DISTINCT o.o_orderkey) > 300
ORDER BY total_quantity DESC
LIMIT 100;

It produces 0 rows in result.
Fix the seed query. Hint: Try using SUM(wl_quantity) in both projection and HAVING clause.
Also, reorder the projections to match the actual output."""

Q20_text = """The query identifies suppliers who have an excess of a given part available; 
an excess is defined to be more than 50% of the parts like the given part that 
the supplier shipped in 1995 for France. 
Only parts made of Ivory available online are considered."""
Q20_seed = """Select s_name, s_address 
 From web_lineitem, nation, part, partsupp, supplier 
 Where wl_partkey = part.p_partkey
 and part.p_partkey = partsupp.ps_partkey
 and wl_suppkey = partsupp.ps_suppkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_nationkey = supplier.s_nationkey
 and nation.n_name = 'FRANCE'
 and wl_quantity <= 9687.99
 and wl_shipdate between '1995-01-01' and '1995-12-31'
 and part.p_name LIKE '%ivory%'
 and partsupp.ps_availqty >= 12 
 Order By s_name asc;"""
Q20_seed_output = """
The above seed query produces the following output:
"Supplier#000000198       "	"ncWe9nTBqJETno"
"Supplier#000000198       "	"ncWe9nTBqJETno"
"Supplier#000000322       "	"lB2qcFCrwazl7Qa"
"Supplier#000000322       "	"lB2qcFCrwazl7Qa"
"Supplier#000000509       "	"SF7dR8V5pK"
"Supplier#000000509       "	"SF7dR8V5pK"
"Supplier#000000553       "	"a,liVofXbCJ"
"Supplier#000000556       "	"g3QRUaiDAI1nQQPJLJfAa9W"
"Supplier#000000593       "	"qvlFqgoEMzzksE2uQlchYQ8V"
"Supplier#000000616       "	"Ktao GA3 5k7oF,wkDyhc0uatR72dD65pD"
"Supplier#000000616       "	"Ktao GA3 5k7oF,wkDyhc0uatR72dD65pD"
"Supplier#000000769       "	"ak2320fUkG"
"Supplier#000000769       "	"ak2320fUkG"
"Supplier#000000769       "	"ak2320fUkG"
"Supplier#000000812       "	"8qh4tezyScl5bidLAysvutB,,ZI2dn6xP"
"Supplier#000000839       "	"1fSx9Sv6LraqnVP3u"
"Supplier#000000839       "	"1fSx9Sv6LraqnVP3u"
"Supplier#000000839       "	"1fSx9Sv6LraqnVP3u"
"Supplier#000000839       "	"1fSx9Sv6LraqnVP3u"
"Supplier#000000954       "	"P3O5p UFz1QsLmZX"
"Supplier#000000954       "	"P3O5p UFz1QsLmZX"
"Supplier#000000954       "	"P3O5p UFz1QsLmZX"
"Supplier#000000954       "	"P3O5p UFz1QsLmZX"
"Supplier#000000954       "	"P3O5p UFz1QsLmZX"
"Supplier#000001154       "	"lPDPT5D5b7u4uNLN, Rl"
"Supplier#000001154       "	"lPDPT5D5b7u4uNLN, Rl"
"Supplier#000001198       "	"vRfsLGzF6aE2XhsqgmJFUHGmMHepJW3X"
"Supplier#000001198       "	"vRfsLGzF6aE2XhsqgmJFUHGmMHepJW3X"
"Supplier#000001285       "	"6GzzLGh7I9P3LhBWnTz,L2gECjp1P1I9mq4TaaK"
"Supplier#000001285       "	"6GzzLGh7I9P3LhBWnTz,L2gECjp1P1I9mq4TaaK"
"Supplier#000001331       "	"6 n,NZ875vge3mSHRgD,"
"Supplier#000001383       "	"HpxV1sNupK1Qe cNH0"
"Supplier#000001384       "	"fjgJwG4DViJrxMxJbO2kS2"
"Supplier#000001384       "	"fjgJwG4DViJrxMxJbO2kS2"
"Supplier#000001398       "	"H1l294pHv2YCA2hQztBZsLGsBmhVBRRh"
"Supplier#000001398       "	"H1l294pHv2YCA2hQztBZsLGsBmhVBRRh"
"Supplier#000001462       "	"HgxOeUIzzWk7BTRw2ax8oHi"
"Supplier#000001462       "	"HgxOeUIzzWk7BTRw2ax8oHi"
"Supplier#000001541       "	"rPUV63BMAmT8Y2qhs 5Z9IT D8zjCJeBHZjW"
"Supplier#000001541       "	"rPUV63BMAmT8Y2qhs 5Z9IT D8zjCJeBHZjW"
"Supplier#000001576       "	"3dj4fsF5fNQ2boo1riXOA7N9t"
"Supplier#000001576       "	"3dj4fsF5fNQ2boo1riXOA7N9t"
"Supplier#000001576       "	"3dj4fsF5fNQ2boo1riXOA7N9t"
"Supplier#000001776       "	"T3DN kKgRFwZQAfUuH1rAWw8qS"
"Supplier#000001776       "	"T3DN kKgRFwZQAfUuH1rAWw8qS"
"Supplier#000001776       "	"T3DN kKgRFwZQAfUuH1rAWw8qS"
"Supplier#000001784       "	"WwxpO7ccLORAYgPyH"
"Supplier#000001784       "	"WwxpO7ccLORAYgPyH"
"Supplier#000001816       "	"e7vab91vLJPWxxZnewmnDBpDmxYHrb"
"Supplier#000001845       "	"Qxx8BfLUs8c1D2umIcr"
"Supplier#000001866       "	"gJ9bAJPfBjX0s5x9dU,qA"
"Supplier#000001866       "	"gJ9bAJPfBjX0s5x9dU,qA"
"Supplier#000001866       "	"gJ9bAJPfBjX0s5x9dU,qA"
"Supplier#000001938       "	"aFMa1UzMRPAO5hsX"
"Supplier#000001938       "	"aFMa1UzMRPAO5hsX"
"Supplier#000001938       "	"aFMa1UzMRPAO5hsX"
"Supplier#000001938       "	"aFMa1UzMRPAO5hsX"
"Supplier#000002070       "	"gZ8nCVAgQIMUfoYvIaTF X"
"Supplier#000002070       "	"gZ8nCVAgQIMUfoYvIaTF X"
"Supplier#000002162       "	"6ya g3MW991n9JfhxSrvgM"
"Supplier#000002179       "	"1bSbNinI5914UbVpjbR8"
"Supplier#000002179       "	"1bSbNinI5914UbVpjbR8"
"Supplier#000002202       "	"l3CTXqUqnR67po0RNhF5"
"Supplier#000002202       "	"l3CTXqUqnR67po0RNhF5"
"Supplier#000002202       "	"l3CTXqUqnR67po0RNhF5"
"Supplier#000002202       "	"l3CTXqUqnR67po0RNhF5"
"Supplier#000002202       "	"l3CTXqUqnR67po0RNhF5"
"Supplier#000002202       "	"l3CTXqUqnR67po0RNhF5"
"Supplier#000002268       "	"1So0dHWj0xfwuNopKvDKFHlCOcL1OvgtkhhUPb"
"Supplier#000002268       "	"1So0dHWj0xfwuNopKvDKFHlCOcL1OvgtkhhUPb"
"Supplier#000002319       "	"3z3bTulBgv8Re30oDzKgGlZQT"
"Supplier#000002319       "	"3z3bTulBgv8Re30oDzKgGlZQT"
"Supplier#000002319       "	"3z3bTulBgv8Re30oDzKgGlZQT"
"Supplier#000002319       "	"3z3bTulBgv8Re30oDzKgGlZQT"
"Supplier#000002397       "	"E0b,zxlk yKgtoKg1jH,"
"Supplier#000002397       "	"E0b,zxlk yKgtoKg1jH,"
"Supplier#000002397       "	"E0b,zxlk yKgtoKg1jH,"
"Supplier#000002397       "	"E0b,zxlk yKgtoKg1jH,"
"Supplier#000002548       "	"UABiGgMCkyTzQnloHsNBCr6da6ITjR"
"Supplier#000002548       "	"UABiGgMCkyTzQnloHsNBCr6da6ITjR"
"Supplier#000002548       "	"UABiGgMCkyTzQnloHsNBCr6da6ITjR"
"Supplier#000002560       "	"gC4t9RFtBMoItUG5dPD"
"Supplier#000002560       "	"gC4t9RFtBMoItUG5dPD"
"Supplier#000002676       "	"Xl4TnYEpX4JlkQh11gL8hXTYRQ1"
"Supplier#000002692       "	"1B3q56lLAYJlOR5LGa V"
"Supplier#000002692       "	"1B3q56lLAYJlOR5LGa V"
"Supplier#000002766       "	"CPJjKybUHBxm0snUwnwWxfZZLk4sbE4JISVWhr"
"Supplier#000002766       "	"CPJjKybUHBxm0snUwnwWxfZZLk4sbE4JISVWhr"
"Supplier#000002818       "	"kzzNb5Jcm9WNmB LGlHk7JgN7"
"Supplier#000002818       "	"kzzNb5Jcm9WNmB LGlHk7JgN7"
"Supplier#000002818       "	"kzzNb5Jcm9WNmB LGlHk7JgN7"
"Supplier#000002818       "	"kzzNb5Jcm9WNmB LGlHk7JgN7"
"Supplier#000002831       "	"8DGtt26QGtxI,3xEQ8gwSwY0JkzYpZWl4OjiunU"
"Supplier#000002906       "	"498dqBD0lISHzpDOGmJf3W57mBSh woorgn"
"Supplier#000002906       "	"498dqBD0lISHzpDOGmJf3W57mBSh woorgn"
"Supplier#000002924       "	"6 nxmhb4Okr1CdJZPA2TaNRrLSXFfzy"
"Supplier#000002924       "	"6 nxmhb4Okr1CdJZPA2TaNRrLSXFfzy"
"Supplier#000003029       "	"aWkIsIRUh3zz8LiwvImuv"
"Supplier#000003067       "	"9EPagnou6ashdkFA"
"Supplier#000003067       "	"9EPagnou6ashdkFA"
"Supplier#000003086       "	"EdiLbOuVZPvcIKQ 8C53GAQCRGDQEn"
"Supplier#000003133       "	"ctd9ax8DHT93kvfF91"
"Supplier#000003153       "	"zZjHS,4cNlNAK1KFaFTNpYh9Y5Ceb"
"Supplier#000003280       "	"TtNwejP, 4GKXNfky9Jc,8gaGEI"
"Supplier#000003280       "	"TtNwejP, 4GKXNfky9Jc,8gaGEI"
"Supplier#000003280       "	"TtNwejP, 4GKXNfky9Jc,8gaGEI"
"Supplier#000003419       "	"yt KX357gL"
"Supplier#000003419       "	"yt KX357gL"
"Supplier#000003429       "	"EAn2WPCt0Glq,y6"
"Supplier#000003635       "	"iZVQF YThR0AJ5kW8QaHZh"
"Supplier#000003689       "	"KuH5dUsSzixv"
"Supplier#000003689       "	"KuH5dUsSzixv"
"Supplier#000003689       "	"KuH5dUsSzixv"
"Supplier#000003689       "	"KuH5dUsSzixv"
"Supplier#000003746       "	"O43Nikgv5lasOik8Ez2mOt3uU"
"Supplier#000003746       "	"O43Nikgv5lasOik8Ez2mOt3uU"
"Supplier#000003746       "	"O43Nikgv5lasOik8Ez2mOt3uU"
"Supplier#000003746       "	"O43Nikgv5lasOik8Ez2mOt3uU"
"Supplier#000003796       "	"gC,28F ofakz0ZdgKQ2nrW7JFO35 RJN"
"Supplier#000003796       "	"gC,28F ofakz0ZdgKQ2nrW7JFO35 RJN"
"Supplier#000003825       "	"hK1aUlbzeTz MSPwcPVyRGY"
"Supplier#000003825       "	"hK1aUlbzeTz MSPwcPVyRGY"
"Supplier#000003836       "	"tdBz4J0l7wDJJu Dej1"
"Supplier#000003836       "	"tdBz4J0l7wDJJu Dej1"
"Supplier#000003850       "	",27mYEAukUi JHLAjUTMCX3hkL8uzcq88"
"Supplier#000003850       "	",27mYEAukUi JHLAjUTMCX3hkL8uzcq88"
"Supplier#000003892       "	"7upn3 0JxQtolUElV7uffY"
"Supplier#000004072       "	"lAYDI98l4wGJ98"
"Supplier#000004090       "	"vRKDWYYcJ9xGtf4xHcWTjXW22"
"Supplier#000004164       "	"f60HY65zdJb6eSCUYOmm"
"Supplier#000004164       "	"f60HY65zdJb6eSCUYOmm"
"Supplier#000004164       "	"f60HY65zdJb6eSCUYOmm"
"Supplier#000004164       "	"f60HY65zdJb6eSCUYOmm"
"Supplier#000004552       "	"eRwxvVjYTpamQHXlldIxF,q8C"
"Supplier#000004566       "	"mAKi0qJOdVHuta0zJx3WUr4er,6QJbSrUXRFN0fN"
"Supplier#000004579       "	"K5nhdAhx6aGpbcRNj0"
"Supplier#000004579       "	"K5nhdAhx6aGpbcRNj0"
"Supplier#000004592       "	"6eoAjyJrWXrsoJr2HelM8zc4ZV5sW,d2je"
"Supplier#000004592       "	"6eoAjyJrWXrsoJr2HelM8zc4ZV5sW,d2je"
"Supplier#000004597       "	"gKuHIUE7XWqK9ZDCA,Kp0jFza4PvTq,RtFF"
"Supplier#000004597       "	"gKuHIUE7XWqK9ZDCA,Kp0jFza4PvTq,RtFF"
"Supplier#000004597       "	"gKuHIUE7XWqK9ZDCA,Kp0jFza4PvTq,RtFF"
"Supplier#000004597       "	"gKuHIUE7XWqK9ZDCA,Kp0jFza4PvTq,RtFF"
"Supplier#000004746       "	"HrNlq N3KfDAfcfX3uho4LqI"
"""
Q20_actual_output = """
Validate whether the predicates of the seed query match against the text.
Fix the query."""

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

Q23_text = """Find the cities and part brands where a customer first buys and returns on web, and then buys again from store. City is identified as the last
5 characters of customer's address."""
Q23_seed = """SELECT   c_address AS city,
         p_brand             AS part_brand
FROM     customer,
         orders o1,
         orders o2,
         store_lineitem,
         web_lineitem,
         part
WHERE    c_custkey = o1.o_custkey
AND      c_custkey = o2.o_custkey 
AND      o1.o_orderkey = wl_orderkey
AND      wl_returnflag = 'A' 
AND      o2.o_orderkey = sl_orderkey
AND      sl_returnflag = 'N' 
AND      wl_partkey = sl_partkey
AND      sl_partkey = p_partkey
AND      o1.o_orderdate < o2.o_orderdate
AND      wl_receiptdate < sl_receiptdate 
AND      o1.o_orderdate BETWEEN date '1995-01-01' AND      date '1995-12-31'
AND      o2.o_orderdate BETWEEN date '1995-01-01' AND      date '1995-12-31'
GROUP BY c_address,
         p_brand 
ORDER BY city, part_brand;   """

Q24_text = """Find the cities where the customer buys an item from the store 
and buys it again from web, 
where the initial purchase could have been made
from the web as well. 
City is identified as the last 5 characters of customer's address."""
Q24_seed = """SELECT c_address AS city
FROM   customer,
       orders o1,
       orders o2,
       store_lineitem,
       web_lineitem w,
       part,
       web_lineitem w1,
       partsupp ps1,
       partsupp ps2
WHERE  c_custkey = o1.o_custkey
       AND c_custkey = o2.o_custkey
       AND o1.o_orderkey = sl_orderkey
       AND sl_returnflag = 'A'
       AND o2.o_orderkey = w.wl_orderkey
       AND w.wl_returnflag = 'N'
       AND w.wl_partkey = sl_partkey
       AND sl_partkey = p_partkey
       AND w1.wl_partkey = p_partkey
       AND sl_receiptdate < w.wl_receiptdate
       AND o1.o_orderdate < o2.o_orderdate
       AND w.wl_suppkey = ps1.ps_suppkey
       AND w1.wl_suppkey = ps2.ps_suppkey
       AND ps2.ps_availqty >= ps1.ps_availqty
       AND o1.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1995-12-31'
       AND o2.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1995-12-31'
GROUP  BY c_address;

Do not change the duplicated instances of tables from the seed query.
Use all the join and filter predicates from the seed query.
"""
Q24_actual_output = """The expected output is as follows:
"PLmwP"
"""
Q24_seed_output = """
The above seed query gives the following output:
"Ss5mZQDrMpA Wg4HNZbVUPLmwP".

Fix the query."""
