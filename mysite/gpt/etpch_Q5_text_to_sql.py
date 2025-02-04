import sys

import tiktoken
from openai import OpenAI

# gets API Key from environment variable OPENAI_API_KEY
client = OpenAI()

text_2_sql_prompt = """Give me SQL for the following text:
The  Query lists for each nation in Asia the revenue volume that resulted from lineitem
transactions in which the customer ordering parts and the supplier filling them were both within that nation. The
query is run in order to determine whether to institute local distribution centers in a given region. The query considers only parts ordered in the year of 1995. The query displays the nations and revenue volume in descending order by
revenue. Revenue volume for all qualifying lineitems in a particular nation is defined as sum(l_extendedprice * (1 -
l_discount)).

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
1. Do not use redundant join conditions.
2. Do not use any predicate with place holder parameter.
3. No attribute in the database has NULL value.

Refine the following SQL to reach to the final query:

(Select n_name, Sum(wl_extendedprice*(1 - wl_discount)) as revenue 
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
 Order By revenue desc, n_name asc); 
"""

next_prompt = """
The expected output is as follows:
"1-URGENT       "	5176
"2-HIGH         "	5311
"3-MEDIUM       "	5164
"4-NOT SPECIFIED"	5182
"5-LOW          "	5210

The above seed query gives the following output:
"1-URGENT       "	57053
"1-URGENT       "	57053
"2-HIGH         "	58502
"2-HIGH         "	58502
"3-MEDIUM       "	57409
"3-MEDIUM       "	57409
"4-NOT SPECIFIED"	57129
"4-NOT SPECIFIED"	57129
"5-LOW          "	57655
"5-LOW          "	57655.

Fix the query."""

shot_2 = """
You formulated the following query:
SELECT o_orderpriority, SUM(order_count) AS order_count
FROM (
    SELECT o_orderpriority, COUNT(*) AS order_count
    FROM orders
    JOIN web_lineitem ON orders.o_orderkey = web_lineitem.wl_orderkey
    WHERE web_lineitem.wl_commitdate < web_lineitem.wl_receiptdate
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-12-31'
    GROUP BY o_orderpriority

    UNION ALL

    SELECT o_orderpriority, COUNT(*) AS order_count
    FROM orders
    JOIN store_lineitem ON orders.o_orderkey = store_lineitem.sl_orderkey
    WHERE store_lineitem.sl_commitdate < store_lineitem.sl_receiptdate
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-12-31'
    GROUP BY o_orderpriority
) AS combined_orders
GROUP BY o_orderpriority
ORDER BY o_orderpriority ASC;

It gives output as follows:
"1-URGENT       "	114106
"2-HIGH         "	117004
"3-MEDIUM       "	114818
"4-NOT SPECIFIED"	114258
"5-LOW          "	115310

Result is not matching. Fix the query."""

shot_3 = """You gave the following query:
SELECT o_orderpriority, SUM(order_count) AS order_count
FROM (
    SELECT o_orderpriority, COUNT(DISTINCT o_orderkey) AS order_count
    FROM orders
    JOIN web_lineitem ON orders.o_orderkey = web_lineitem.wl_orderkey
    WHERE web_lineitem.wl_commitdate < web_lineitem.wl_receiptdate
    AND EXTRACT(YEAR FROM orders.o_orderdate) = 1995
    AND EXTRACT(QUARTER FROM orders.o_orderdate) = 1
    GROUP BY o_orderpriority

    UNION ALL

    SELECT o_orderpriority, COUNT(DISTINCT o_orderkey) AS order_count
    FROM orders
    JOIN store_lineitem ON orders.o_orderkey = store_lineitem.sl_orderkey
    WHERE store_lineitem.sl_commitdate < store_lineitem.sl_receiptdate
    AND EXTRACT(YEAR FROM orders.o_orderdate) = 1995
    AND EXTRACT(QUARTER FROM orders.o_orderdate) = 1
    GROUP BY o_orderpriority
) AS combined_orders
GROUP BY o_orderpriority
ORDER BY o_orderpriority ASC;

It gives different output:
"1-URGENT       "	10352
"2-HIGH         "	10622
"3-MEDIUM       "	10328
"4-NOT SPECIFIED"	10364
"5-LOW          "	10420.

Do not put any redundant filter predicate. All the filter predicates should be as per the seed query.
Validate all the join predicates against the text description and refine your query."""

shot_4 = """SELECT o_orderpriority, COUNT(DISTINCT o_orderkey) AS order_count
FROM (
    SELECT o_orderpriority, o_orderkey
    FROM orders
    JOIN web_lineitem ON orders.o_orderkey = web_lineitem.wl_orderkey
    WHERE web_lineitem.wl_commitdate < web_lineitem.wl_receiptdate
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'

    UNION ALL

    SELECT o_orderpriority, o_orderkey
    FROM orders
    JOIN store_lineitem ON orders.o_orderkey = store_lineitem.sl_orderkey
    WHERE store_lineitem.sl_commitdate < store_lineitem.sl_receiptdate
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
) AS combined_orders
GROUP BY o_orderpriority
ORDER BY o_orderpriority ASC;

"""

def count_tokens(text):
    encoding = tiktoken.encoding_for_model("gpt-4o")
    tokens = encoding.encode(text)
    return len(tokens)


def one_round():
    text = f"{text_2_sql_prompt}"
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
