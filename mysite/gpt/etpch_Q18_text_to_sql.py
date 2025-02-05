import sys

import tiktoken
from openai import OpenAI

# gets API Key from environment variable OPENAI_API_KEY
client = OpenAI()

next_prompt= """
You formulated the below query:
SELECT 
    customer.c_name, 
    customer.c_custkey, 
    orders.o_orderkey, 
    orders.o_orderdate, 
    orders.o_totalprice, 
    SUM(web_lineitem.wl_quantity) AS total_quantity
FROM 
    customer
JOIN 
    orders ON customer.c_custkey = orders.o_custkey
JOIN 
    web_lineitem ON orders.o_orderkey = web_lineitem.wl_orderkey
GROUP BY 
    customer.c_custkey, 
    customer.c_name, 
    orders.o_orderkey, 
    orders.o_orderdate, 
    orders.o_totalprice
HAVING 
    SUM(web_lineitem.wl_quantity) > 300
ORDER BY 
    total_quantity DESC
LIMIT 100;

Your query produces the following incorrect output:
"Customer#000066790"	66790	2199712	"1996-09-30"	515531.82	327.00
"Customer#000147197"	147197	1263015	"1997-02-02"	467149.67	320.00
"Customer#000119989"	119989	1544643	"1997-09-20"	434568.25	320.00
"Customer#000117919"	117919	2869152	"1996-06-20"	456815.92	317.00
"Customer#000148885"	148885	2942469	"1992-05-31"	469630.44	313.00
"Customer#000016384"	16384	502886	"1994-04-12"	458378.92	312.00
"Customer#000141823"	141823	2806245	"1996-12-29"	446269.12	310.00
"Customer#000012251"	12251	735366	"1993-11-24"	455107.26	309.00
"Customer#000120098"	120098	1971680	"1995-06-14"	453451.23	308.00
"Customer#000114586"	114586	551136	"1993-05-19"	469605.59	308.00
"Customer#000077260"	77260	1436544	"1992-09-12"	479499.43	307.00
"Customer#000105995"	105995	2096705	"1994-07-03"	469692.58	307.00
"Customer#000082441"	82441	857959	"1994-02-07"	382579.74	305.00
"Customer#000066533"	66533	29158	"1995-10-21"	443576.50	305.00
"Customer#000069904"	69904	1742403	"1996-10-19"	408513.00	305.00
"Customer#000064483"	64483	2745894	"1996-07-04"	466991.35	304.00
"Customer#000003566"	3566	2329187	"1998-01-04"	439803.36	304.00
"Customer#000088876"	88876	983201	"1993-12-30"	446717.46	304.00
"Customer#000013940"	13940	2232932	"1997-04-13"	522720.61	304.00
"Customer#000017746"	17746	6882	"1997-04-09"	408446.93	303.00
"Customer#000050008"	50008	2366755	"1996-12-09"	483891.26	302.00
"Customer#000015631"	15631	1845057	"1994-05-12"	419879.59	302.00
"Customer#000088703"	88703	2995076	"1994-01-30"	363812.12	302.00
"Customer#000024341"	24341	1474818	"1992-11-15"	491348.26	302.00
"Customer#000053029"	53029	2662214	"1993-08-13"	446144.49	302.00
"Customer#000013072"	13072	1481925	"1998-03-15"	399195.47	301.00
"Customer#000136573"	136573	2761378	"1996-05-31"	461282.73	301.00
"Customer#000141098"	141098	565574	"1995-09-24"	430986.69	301.00
"Customer#000113131"	113131	967334	"1995-12-15"	432957.75	301.00

Order seems different. Fix the query."""

text_2_sql_prompt = """Give me SQL for the following text:
The Query finds a list of the top 100 customers who have ever placed large quantity orders online.
The query lists the customer name, customer key, the order key, date and total price and the quantity for the order.
The output should be:
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

Give only the SQL, do not add any explaination.
Put the SQL within Python style comment quotes.

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
   FOREIGN KEY (N_REGIONKEY) REFERENCES REGION(R_REGIONKEY);
   
Mandatory instructions on SQL query formulation:
1. Strictly use all the tables present in the following query.
2. Do not use redundant join conditions.
3. Do not use any predicate with place holder parameter.
4. No attribute in the database has NULL value.

Refine the following SQL to formulate your output query:
SELECT  <failed> FROM customer, orders, web_lineitem
        WHERE orders.o_orderkey = web_lineitem.wl_orderkey AND customer.c_custkey = orders.o_custkey
        GROUP BY customer.c_custkey, customer.c_name, web_lineitem.wl_orderkey
        HAVING SUM(web_lineitem.wl_quantity) >= 300.01;
"""


def count_tokens(text):
    encoding = tiktoken.encoding_for_model("gpt-4o")
    tokens = encoding.encode(text)
    return len(tokens)


def one_round():
    text = f"{text_2_sql_prompt}\n{next_prompt}"
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "user",
                "content": f"{text}",
            },
        ], temperature=0, stream=False
    )
    reply = response.choices[0].message.content
    print(reply)
    c_token = count_tokens(text)
    print(f"\nToken count = {c_token}\n")


orig_out = sys.stdout
f = open('chatgpt_tpch_sql.py', 'w')
sys.stdout = f
one_round()
sys.stdout = orig_out
f.close()
