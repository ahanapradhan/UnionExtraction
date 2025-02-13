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

Consider performing union first and then group by.
Strictly use the filter predicates used in the query.
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
Q3_feedback1 = """Consider performing union first then using group by."""

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

Consider performing union first and then group by to produce a single aggregated value."""
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
Q8_seed_output = """Output of the above seed query has 618 rows.
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
Q9_seed_output = """Output of the above seed query has 25272 rows.
"""
Q9_actual_output = """But the actual output has only 175 rows.
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
Q10_seed_output = """Output of the above seed query has 40 rows.
"""
Q10_actual_output = """But the actual output should have 20 rows.

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
Q11_seed_output = """The above seed query produces 31082 rows.
"""
Q11_actual_output = """But the actual query should produce 22699 rows.

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

Q13_text = """This query determines the distribution of customers by the number of orders they have made, including customers
who have no record of orders, past or present. It counts and reports how many customers have no orders, how many
have 1, 2, 3, etc. A check is made to ensure that the orders counted do not fall into one of several special categories
of orders. Special categories are identified in the order comment column by looking for pattern '%special%requests%'."""
Q13_seed = """Select o_orderdate as c_count, Count(*) as c_orderdate, <unknown> as custdist 
 From customer, orders 
 Where customer.c_custkey = orders.o_custkey 
 Group By o_orderdate, custdist
 Order By c_count DESC, custdist <unknwon>; 
 
 Projection 'o_orderdate as c_count' must be there in the query.
 The seed query produces more rows than the actual output.
 So, either 'Group by o_orderdate, custdist' needs more attributes in this clause, or there needs to be nested group by clause.
 Nested group by has one group by clause in the inner query, one group by clause in the outer query.
 In such a nested query, COUNT function is used in the projection only once in the inner query, and once in the outer query.
 Since 'Count(*) as c_orderdate' is present in the outer query, it will stay as it is. 
 If any more COUNT is required, that should be in the inner query.
 'Order by c_count DESC' is correct. Do not change it.
"""
Q13_seed_output = """The above seed query gives the following result (first 100 rows):
"1998-08-02"	581	1
"1998-08-01"	618	1
"1998-07-31"	621	1
"1998-07-30"	619	1
"1998-07-29"	608	1
"1998-07-28"	612	1
"1998-07-27"	602	1
"1998-07-26"	630	1
"1998-07-25"	627	1
"1998-07-24"	606	1
"1998-07-23"	640	1
"1998-07-22"	648	1
"1998-07-21"	629	1
"1998-07-20"	606	1
"1998-07-19"	638	1
"1998-07-18"	640	1
"1998-07-17"	641	1
"1998-07-16"	620	1
"1998-07-15"	639	1
"1998-07-14"	636	1
"1998-07-13"	572	1
"1998-07-12"	630	1
"1998-07-11"	668	1
"1998-07-10"	658	1
"1998-07-09"	606	1
"1998-07-08"	620	1
"1998-07-07"	627	1
"1998-07-06"	620	1
"1998-07-05"	592	1
"1998-07-04"	606	1
"1998-07-03"	645	1
"1998-07-02"	616	1
"1998-07-01"	651	1
"1998-06-30"	588	1
"1998-06-29"	629	1
"1998-06-28"	634	1
"1998-06-27"	567	1
"1998-06-26"	632	1
"1998-06-25"	620	1
"1998-06-24"	659	1
"1998-06-23"	588	1
"1998-06-22"	616	1
"1998-06-21"	639	1
"1998-06-20"	603	1
"1998-06-19"	567	1
"1998-06-18"	625	1
"1998-06-17"	626	1
"1998-06-16"	597	1
"1998-06-15"	606	1
"1998-06-14"	638	1
"1998-06-13"	634	1
"1998-06-12"	636	1
"1998-06-11"	685	1
"1998-06-10"	616	1
"1998-06-09"	597	1
"1998-06-08"	660	1
"1998-06-07"	585	1
"1998-06-06"	640	1
"1998-06-05"	620	1
"1998-06-04"	612	1
"1998-06-03"	623	1
"1998-06-02"	648	1
"1998-06-01"	600	1
"1998-05-31"	593	1
"1998-05-30"	604	1
"1998-05-29"	649	1
"1998-05-28"	656	1
"1998-05-27"	630	1
"1998-05-26"	671	1
"1998-05-25"	590	1
"1998-05-24"	636	1
"1998-05-23"	640	1
"1998-05-22"	578	1
"1998-05-21"	622	1
"1998-05-20"	643	1
"1998-05-19"	667	1
"1998-05-18"	623	1
"1998-05-17"	663	1
"1998-05-16"	605	1
"1998-05-15"	607	1
"1998-05-14"	652	1
"1998-05-13"	609	1
"1998-05-12"	631	1
"1998-05-11"	621	1
"1998-05-10"	624	1
"1998-05-09"	631	1
"1998-05-08"	590	1
"1998-05-07"	599	1
"1998-05-06"	609	1
"1998-05-05"	613	1
"1998-05-04"	670	1
"1998-05-03"	615	1
"1998-05-02"	635	1
"1998-05-01"	656	1
"1998-04-30"	654	1
"1998-04-29"	639	1
"1998-04-28"	625	1
"1998-04-27"	633	1
"1998-04-26"	611	1
"1998-04-25"	566	1
"""
Q13_actual_output = """The actual output of the query is (first 100 rows):
NULL	0	50005
"1995-01-13"	1	686
"1996-05-31"	1	685
"1993-03-19"	1	685
"1998-01-21"	1	683
"1995-04-13"	1	682
"1998-02-16"	1	681
"1996-12-31"	1	681
"1994-03-15"	1	681
"1996-09-23"	1	680
"1997-07-28"	1	678
"1994-09-12"	1	678
"1993-08-01"	1	678
"1992-02-24"	1	678
"1998-03-21"	1	676
"1997-08-23"	1	676
"1993-08-12"	1	676
"1993-01-22"	1	676
"1997-03-22"	1	675
"1998-01-15"	1	674
"1997-12-02"	1	674
"1997-01-05"	1	674
"1994-08-01"	1	674
"1992-10-24"	1	673
"1996-11-29"	1	672
"1996-07-26"	1	672
"1995-03-26"	1	672
"1993-01-02"	1	672
"1995-10-11"	1	671
"1994-01-02"	1	671
"1997-08-17"	1	670
"1996-07-01"	1	670
"1996-02-21"	1	670
"1995-04-21"	1	670
"1995-01-26"	1	670
"1998-06-11"	1	669
"1997-08-05"	1	669
"1996-03-23"	1	669
"1996-09-18"	1	668
"1996-06-15"	1	668
"1994-11-13"	1	668
"1997-01-22"	1	667
"1995-07-01"	1	667
"1993-09-24"	1	667
"1997-12-05"	1	666
"1997-04-29"	1	666
"1996-01-09"	1	666
"1995-11-15"	1	666
"1994-03-05"	1	666
"1992-12-03"	1	666
"1997-06-25"	1	665
"1994-12-19"	1	665
"1994-03-28"	1	665
"1994-02-08"	1	665
"1993-09-09"	1	665
"1993-04-24"	1	665
"1993-04-04"	1	665
"1992-12-05"	1	665
"1992-07-13"	1	665
"1997-12-08"	1	664
"1996-11-25"	1	664
"1995-04-23"	1	664
"1998-05-04"	1	663
"1996-05-08"	1	663
"1996-02-29"	1	663
"1995-07-16"	1	663
"1995-01-28"	1	663
"1992-06-18"	1	663
"1992-01-04"	1	663
"1998-03-29"	1	662
"1997-06-05"	1	662
"1996-06-20"	1	662
"1995-04-06"	1	662
"1995-01-30"	1	662
"1998-01-02"	1	661
"1997-03-05"	1	661
"1995-06-03"	1	661
"1994-11-14"	1	661
"1994-06-20"	1	661
"1992-05-29"	1	661
"1997-08-04"	1	660
"1995-05-04"	1	660
"1994-12-22"	1	660
"1993-09-01"	1	660
"1992-01-31"	1	660
"1998-05-26"	1	659
"1997-06-12"	1	659
"1996-09-27"	1	659
"1995-12-19"	1	659
"1995-07-26"	1	659
"1992-08-20"	1	659
"1998-05-19"	1	658
"1997-02-17"	1	658
"1996-06-30"	1	658
"1996-03-07"	1	658
"1996-01-03"	1	658
"1995-07-27"	1	658
"1993-10-18"	1	658
"1997-12-06"	1	657
"1997-05-28"	1	657

Fix the seed query.
Projection 'o_orderdate as c_count' must be there in the query.
Validate all the other projections against the text.
Do not use COALESCE in SELECT Clause.
The query has 3 projections, with their aliases as c_count, c_orderdate and custdist. 
Fix the SQL."""
Q13_feedback1 = """
SELECT 
    subquery.c_count, 
    COUNT(*) AS c_orderdate, 
    subquery.custdist 
FROM 
    (
        SELECT 
            o.o_orderdate AS c_count, 
            COUNT(o.o_orderkey) AS custdist 
        FROM 
            customer c 
        LEFT JOIN 
            orders o 
        ON 
            c.c_custkey = o.o_custkey 
            AND o.o_comment NOT LIKE '%special%requests%' 
        GROUP BY 
            o.o_orderdate, c.c_custkey
    ) AS subquery 
GROUP BY 
    subquery.c_count, subquery.custdist 
ORDER BY 
    subquery.c_count DESC, subquery.custdist ASC;
    
It gives the following output (first 100 rows):
"1992-01-01"	607	1
"1992-01-01"	2	2
"1992-01-02"	605	1
"1992-01-02"	2	2
"1992-01-03"	587	1
"1992-01-03"	3	2
"1992-01-04"	663	1
"1992-01-04"	2	2
"1992-01-05"	630	1
"1992-01-05"	3	2
"1992-01-06"	617	1
"1992-01-06"	4	2
"1992-01-07"	624	1
"1992-01-07"	4	2
"1992-01-08"	594	1
"1992-01-08"	4	2
"1992-01-09"	635	1
"1992-01-09"	3	2
"1992-01-10"	618	1
"1992-01-10"	3	2
"1992-01-11"	631	1
"1992-01-11"	1	2
"1992-01-12"	617	1
"1992-01-12"	1	2
"1992-01-13"	597	1
"1992-01-13"	1	2
"1992-01-14"	624	1
"1992-01-14"	4	2
"1992-01-15"	594	1
"1992-01-15"	3	2
"1992-01-16"	590	1
"1992-01-16"	3	2
"1992-01-17"	606	1
"1992-01-17"	4	2
"1992-01-18"	598	1
"1992-01-19"	620	1
"1992-01-19"	2	2
"1992-01-20"	577	1
"1992-01-21"	620	1
"1992-01-21"	2	2
"1992-01-22"	600	1
"1992-01-22"	1	2
"1992-01-23"	590	1
"1992-01-23"	2	2
"1992-01-24"	596	1
"1992-01-24"	3	2
"1992-01-25"	632	1
"1992-01-25"	1	2
"1992-01-26"	601	1
"1992-01-26"	3	2
"1992-01-27"	636	1
"1992-01-27"	4	2
"1992-01-28"	626	1
"1992-01-28"	2	2
"1992-01-29"	578	1
"1992-01-29"	2	2
"1992-01-30"	632	1
"1992-01-30"	2	2
"1992-01-31"	660	1
"1992-01-31"	1	2
"1992-02-01"	624	1
"1992-02-02"	597	1
"1992-02-02"	2	2
"1992-02-03"	614	1
"1992-02-03"	1	2
"1992-02-04"	635	1
"1992-02-04"	2	2
"1992-02-05"	645	1
"1992-02-05"	1	2
"1992-02-06"	599	1
"1992-02-06"	1	2
"1992-02-07"	610	1
"1992-02-07"	1	2
"1992-02-08"	608	1
"1992-02-08"	2	2
"1992-02-09"	555	1
"1992-02-09"	1	2
"1992-02-10"	575	1
"1992-02-10"	3	2
"1992-02-11"	607	1
"1992-02-11"	1	2
"1992-02-12"	592	1
"1992-02-12"	2	2
"1992-02-13"	632	1
"1992-02-13"	2	2
"1992-02-14"	615	1
"1992-02-14"	1	2
"1992-02-15"	611	1
"1992-02-16"	625	1
"1992-02-16"	1	2
"1992-02-17"	648	1
"1992-02-17"	1	2
"1992-02-18"	613	1
"1992-02-18"	2	2
"1992-02-19"	611	1
"1992-02-19"	1	2
"1992-02-20"	595	1
"1992-02-20"	4	2
"1992-02-21"	621	1
"1992-02-21"	2	2

Order seems different.

'Order by c_count DESC' is correct. Do not change it.
'subquery.c_count ASC' is incorrect.
Fix the query.
"""
Q13_feedback2 = """
The following query also produces the same incorrect result as before:
SELECT 
    subquery.c_count, 
    COUNT(*) AS c_orderdate, 
    subquery.custdist 
FROM 
    (
        SELECT 
            o.o_orderdate AS c_count, 
            COUNT(o.o_orderkey) AS custdist 
        FROM 
            customer c 
        LEFT JOIN 
            orders o 
        ON 
            c.c_custkey = o.o_custkey 
            AND o.o_comment NOT LIKE '%special%requests%' 
        GROUP BY 
            o.o_orderdate, c.c_custkey
    ) AS subquery 
GROUP BY 
    subquery.c_count, subquery.custdist 
ORDER BY 
    subquery.c_count DESC, subquery.custdist;

Order by subquery.c_count DESC is correct. Do not change it.
'Order by subquery.custdist' and 'Order by subquery.custdist ASC' are equivalent, which is giving incorrect ordering.
Fix the qeury.
"""
Q13_feedback4 = """
The following query formulated by you gives 4483 rows:
SELECT 
    c_count, 
    COUNT(*) AS c_orderdate, 
    order_count AS custdist
FROM (
    SELECT 
        customer.c_custkey, 
        o_orderdate AS c_count,
        COUNT(orders.o_orderkey) AS order_count
    FROM 
        customer
    LEFT JOIN 
        orders ON customer.c_custkey = orders.o_custkey 
        AND orders.o_comment NOT LIKE '%special%requests%'
    GROUP BY 
        customer.c_custkey, o_orderdate
) AS subquery
GROUP BY 
    c_count, order_count
ORDER BY 
    custdist desc, c_count asc;
    
But the order is different.
The following is actual output:
	0	50005
"1995-01-13"	1	686
"1996-05-31"	1	685
"1993-03-19"	1	685
"1998-01-21"	1	683
"1995-04-13"	1	682
"1998-02-16"	1	681
"1996-12-31"	1	681
"1994-03-15"	1	681
"1996-09-23"	1	680
"1997-07-28"	1	678
"1994-09-12"	1	678
"1993-08-01"	1	678
"1992-02-24"	1	678
"1998-03-21"	1	676
"1997-08-23"	1	676
"1993-08-12"	1	676
"1993-01-22"	1	676
"1997-03-22"	1	675
"1998-01-15"	1	674
"1997-12-02"	1	674
"1997-01-05"	1	674
"1994-08-01"	1	674
"1992-10-24"	1	673
"1996-11-29"	1	672
"1996-07-26"	1	672
"1995-03-26"	1	672
"1993-01-02"	1	672
"1995-10-11"	1	671
"1994-01-02"	1	671
"1997-08-17"	1	670
"1996-07-01"	1	670
"1996-02-21"	1	670
"1995-04-21"	1	670
"1995-01-26"	1	670
"1998-06-11"	1	669
"1997-08-05"	1	669
"1996-03-23"	1	669
"1996-09-18"	1	668
"1996-06-15"	1	668
"1994-11-13"	1	668
"1997-01-22"	1	667
"1995-07-01"	1	667
"1993-09-24"	1	667
"1997-12-05"	1	666
"1997-04-29"	1	666
"1996-01-09"	1	666
"1995-11-15"	1	666
"1994-03-05"	1	666
"1992-12-03"	1	666
"1997-06-25"	1	665
"1994-12-19"	1	665
"1994-03-28"	1	665
"1994-02-08"	1	665
"1993-09-09"	1	665
"1993-04-24"	1	665
"1993-04-04"	1	665
"1992-12-05"	1	665
"1992-07-13"	1	665
"1997-12-08"	1	664
"1996-11-25"	1	664
"1995-04-23"	1	664
"1998-05-04"	1	663
"1996-05-08"	1	663
"1996-02-29"	1	663
"1995-07-16"	1	663
"1995-01-28"	1	663
"1992-06-18"	1	663
"1992-01-04"	1	663
"1998-03-29"	1	662
"1997-06-05"	1	662
"1996-06-20"	1	662
"1995-04-06"	1	662
"1995-01-30"	1	662
"1998-01-02"	1	661
"1997-03-05"	1	661
"1995-06-03"	1	661
"1994-11-14"	1	661
"1994-06-20"	1	661
"1992-05-29"	1	661
"1997-08-04"	1	660
"1995-05-04"	1	660
"1994-12-22"	1	660
"1993-09-01"	1	660
"1992-01-31"	1	660
"1998-05-26"	1	659
"1997-06-12"	1	659
"1996-09-27"	1	659
"1995-12-19"	1	659
"1995-07-26"	1	659
"1992-08-20"	1	659
"1998-05-19"	1	658
"1997-02-17"	1	658
"1996-06-30"	1	658
"1996-03-07"	1	658
"1996-01-03"	1	658
"1995-07-27"	1	658
"1993-10-18"	1	658
"1997-12-06"	1	657
"1997-05-28"	1	657

Your query gives the following putput:
"1992-02-21"	1	3
"1992-05-31"	1	3
"1992-09-10"	1	3
"1992-12-24"	1	3
"1993-04-15"	1	3
"1994-09-22"	1	3
"1994-10-26"	1	3
"1995-05-12"	1	3
"1995-06-22"	1	3
"1996-03-31"	1	3
"1997-04-29"	1	3
"1997-09-20"	1	3
"1992-01-01"	2	2
"1992-01-02"	2	2
"1992-01-03"	3	2
"1992-01-04"	2	2
"1992-01-05"	3	2
"1992-01-06"	4	2
"1992-01-07"	4	2
"1992-01-08"	4	2
"1992-01-09"	3	2
"1992-01-10"	3	2
"1992-01-11"	1	2
"1992-01-12"	1	2
"1992-01-13"	1	2
"1992-01-14"	4	2
"1992-01-15"	3	2
"1992-01-16"	3	2
"1992-01-17"	4	2
"1992-01-19"	2	2
"1992-01-21"	2	2
"1992-01-22"	1	2
"1992-01-23"	2	2
"1992-01-24"	3	2
"1992-01-25"	1	2
"1992-01-26"	3	2
"1992-01-27"	4	2
"1992-01-28"	2	2
"1992-01-29"	2	2
"1992-01-30"	2	2
"1992-01-31"	1	2
"1992-02-02"	2	2
"1992-02-03"	1	2
"1992-02-04"	2	2
"1992-02-05"	1	2
"1992-02-06"	1	2
"1992-02-07"	1	2
"1992-02-08"	2	2
"1992-02-09"	1	2
"1992-02-10"	3	2
"1992-02-11"	1	2
"1992-02-12"	2	2
"1992-02-13"	2	2
"1992-02-14"	1	2
"1992-02-16"	1	2
"1992-02-17"	1	2
"1992-02-18"	2	2
"1992-02-19"	1	2
"1992-02-20"	4	2
"1992-02-21"	2	2
"1992-02-22"	4	2
"1992-02-23"	1	2
"1992-02-24"	1	2
"1992-02-26"	1	2
"1992-02-27"	4	2
"1992-02-28"	1	2
"1992-03-01"	2	2
"1992-03-02"	2	2
"1992-03-03"	3	2
"1992-03-04"	2	2
"1992-03-05"	2	2
"1992-03-06"	1	2
"1992-03-07"	1	2
"1992-03-08"	2	2
"1992-03-09"	4	2
"1992-03-10"	2	2
"1992-03-11"	2	2
"1992-03-12"	2	2
"1992-03-13"	3	2
"1992-03-14"	4	2
"1992-03-16"	1	2
"1992-03-17"	2	2
"1992-03-18"	1	2
"1992-03-19"	3	2
"1992-03-20"	2	2
"1992-03-21"	2	2
"1992-03-22"	2	2
"1992-03-23"	3	2
"1992-03-25"	1	2
"1992-03-26"	2	2
"1992-03-27"	1	2
"1992-03-28"	1	2
"1992-03-30"	2	2
"1992-03-31"	2	2
"1992-04-02"	1	2
"1992-04-03"	1	2
"1992-04-04"	4	2
"1992-04-05"	1	2
"1992-04-06"	1	2
"1992-04-07"	3	2

Keep two group by clauses, one in the inner and the other in the outer query. But fix their attributes.
o_orderdate must be in the group by attributes of the outer query.
The two COUNT aggregations in the outer query will give the same value. This is incorrect. 
Use one COUNT in the outer projection, one COUNT in the inner projection.
Do not use SUM.

Do not reproduce previously formulated incorrect queries, which are given in this prompt.

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
Q15_seed_output = """ The seed query produces 9996 rows.
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

Q17_text = """The Query considers parts of a given brand and with a given container type and
determines the average lineitem quantity of such parts ordered for all orders (past and pending) in the 7-year database. 
What would be the average yearly gross (undiscounted) loss in revenue if orders for these parts with a quantity
of less than 70% of this average were no longer taken?"""
Q17_seed = """Select 0.14*wl_extendedprice as avg_yearly 
From part, web_lineitem w1, web_lineitem w1 
Where part.p_partkey = w1.wl_partkey
 and w1.wl_partkey = w2.wl1_partkey
 and w1.wl_quantity < w2.wl1_quantity
 and part.p_brand = 'Brand#53'
 and part.p_container = 'MED BAG'
 and w1.wl_quantity <= 1503238553.51 
 
 part table is used only once. Web_lineitem is used twice. Strictly follow this;
"""
Q17_seed_output = """Validate the predicates of the seed query against the text description."""
Q17_actual_output = """
part table is used only once. Do not use it both in outer and inner query.
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

Q19_text = """The query finds the gross discounted revenue for all orders for three different types of parts
that were shipped by air and delivered in person. Parts are selected based on the combination of specific brands, a
list of containers, and a range of sizes."""
Q19_seed = """select
        sum(wl_extendedprice* (1 - wl_discount)) as revenue
from
        web_lineitem,
        part
where (p_partkey = wl_partkey
                and p_brand = 'Brand#12' 
                and p_size between 1 and 5 
                and l_shipinstruct = 'DELIVER IN PERSON'
                and l_shipmode = 'AIR'
                and l_quantity between 1 and 11
                and p_container = 'SM CASE') 
    OR (p_partkey = wl_partkey
                and p_brand = 'Brand#12' 
                and p_size between 1 and 5 
                and l_shipinstruct = 'DELIVER IN PERSON'
                and l_shipmode = 'AIR'
                and l_quantity between 1 and 11
                and p_container = 'SM BOX')
    OR (p_partkey = wl_partkey
                and p_brand = 'Brand#12' 
                and p_size between 1 and 5 
                and l_shipinstruct = 'DELIVER IN PERSON'
                and l_shipmode = 'AIR'
                and l_quantity between 1 and 11
                and p_container = 'SM PACK')
    OR (p_partkey = wl_partkey
                and p_brand = 'Brand#12' 
                and p_size between 1 and 5 
                and l_shipinstruct = 'DELIVER IN PERSON'
                and l_shipmode = 'AIR'
                and l_quantity between 1 and 11
                and p_container = 'SM PKG')
    OR (p_partkey = wl_partkey
                and p_brand = 'Brand#12' 
                and p_size between 1 and 10 
                and l_shipinstruct = 'DELIVER IN PERSON'
                and l_shipmode = 'AIR'
                and l_quantity between 10 and 20
                and p_container = 'MED CASE') 
    OR (p_partkey = wl_partkey
                and p_brand = 'Brand#12' 
                and p_size between 1 and 10 
                and l_shipinstruct = 'DELIVER IN PERSON'
                and l_shipmode = 'AIR'
                and l_quantity between 10 and 20
                and p_container = 'MED BOX')
    OR (p_partkey = wl_partkey
                and p_brand = 'Brand#12' 
                and p_size between 1 and 10 
                and l_shipinstruct = 'DELIVER IN PERSON'
                and l_shipmode = 'AIR'
                and l_quantity between 10 and 20
                and p_container = 'MED PACK')
    OR (p_partkey = wl_partkey
                and p_brand = 'Brand#12' 
                and p_size between 1 and 10 
                and l_shipinstruct = 'DELIVER IN PERSON'
                and l_shipmode = 'AIR'
                and l_quantity between 10 and 20
                and p_container = 'MED PKG')
    OR (p_partkey = wl_partkey
                and p_brand = 'Brand#12' 
                and p_size between 1 and 15 
                and l_shipinstruct = 'DELIVER IN PERSON'
                and l_shipmode = 'AIR'
                and l_quantity between 20 and 30
                and p_container = 'LG CASE') 
    OR (p_partkey = wl_partkey
                and p_brand = 'Brand#12' 
                and p_size between 1 and 15 
                and l_shipinstruct = 'DELIVER IN PERSON'
                and l_shipmode = 'AIR'
                and l_quantity between 20 and 30
                and p_container = 'LG BOX')
    OR (p_partkey = wl_partkey
                and p_brand = 'Brand#12' 
                and p_size between 1 and 15 
                and l_shipinstruct = 'DELIVER IN PERSON'
                and l_shipmode = 'AIR'
                and l_quantity between 20 and 30
                and p_container = 'LG PACK')
    OR (p_partkey = wl_partkey
                and p_brand = 'Brand#12' 
                and p_size between 1 and 15 
                and l_shipinstruct = 'DELIVER IN PERSON'
                and l_shipmode = 'AIR'
                and l_quantity between 20 and 30
                and p_container = 'LG PKG');"""
Q19_seed_output = """Organize the predicates of the seed query into three types."""
Q19_actual_output = """But the actual query should produce the following output:

Fix the seed query."""

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
Consider multi-supplier orders strictly.
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

Q22_text = """This query counts how many customers within country 
codes among '13', '31', '23', '29', '30', '18', and '17' 
have not placed orders for 7 years
but who have a greater than average ``positive" account balance. 
It also reflects the magnitude of that balance.
Country code is defined as the first two characters of c_phone."""
Q22_seed = """Select c1.c_phone as cntrycode, <unknown> as numcust, c1.c_acctbal as totacctbal 
 From customer c1, customer c2, orders 
 Where c2.c_acctbal < c1.c_acctbal
 and (c1.c_phone LIKE '30%' OR c1.c_phone LIKE '13%' OR c1.c_phone LIKE '31%' OR c1.c_phone LIKE '17%' OR
 c1.c_phone LIKE '18%' OR c1.c_phone LIKE '23%' OR c1.c_phone LIKE '29%')
 and (c2.c_phone LIKE '30%' OR c2.c_phone LIKE '13%' OR c2.c_phone LIKE '31%' OR c2.c_phone LIKE '17%' OR
 c2.c_phone LIKE '18%' OR c2.c_phone LIKE '23%' OR c2.c_phone LIKE '29%')
 and c2.c_acctbal >= 0.01"""
Q22_seed_output = """
Re-use the two instances of customer table, 
and the related predicates of the seed query in your query.
Validate each predicate and optimize if possible."""
Q22_actual_output = """The query should produce the following output:
"13"	888	6737713.99
"17"	861	6460573.72
"18"	964	7236687.40
"23"	892	6701457.95
"29"	948	7158866.63
"30"	909	6808436.13
"31"	922	6806670.18"""
Q22_feedback1 = """You produced the following query:
SELECT 
    SUBSTRING(c1.c_phone FROM 1 FOR 2) AS cntrycode, 
    COUNT(DISTINCT c1.c_custkey) AS numcust, 
    SUM(c1.c_acctbal) AS totacctbal
FROM 
    customer c1
LEFT JOIN 
    orders o ON c1.c_custkey = o.o_custkey AND o.o_orderdate >= CURRENT_DATE - INTERVAL '7 years'
WHERE 
    SUBSTRING(c1.c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
    AND c1.c_acctbal > (
        SELECT AVG(c2.c_acctbal)
        FROM customer c2
        WHERE c2.c_acctbal > 0
    )
    AND o.o_orderkey IS NULL
GROUP BY 
    cntrycode
ORDER BY 
    cntrycode;

It produces the following reuslt:
"13"	2680	20224566.84
"17"	2642	19835161.13
"18"	2779	20952115.38
"23"	2697	20238991.21
"29"	2835	21222036.16
"30"	2653	19895118.39
"31"	2739	20526474.44.

Do not use predicate on any attribute that is not present in the seed query.

Fix the query."""

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
