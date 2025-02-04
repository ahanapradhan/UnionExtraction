import sys

import tiktoken
from openai import OpenAI

# gets API Key from environment variable OPENAI_API_KEY
client = OpenAI()

next_prompt = """
The first 100 rows of the result of the expected query is as follows:

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
You formulated the following query:

(SELECT 
    n.n_name AS nation, 
    EXTRACT(YEAR FROM o.o_orderdate) AS o_year, 
    SUM((wl.wl_extendedprice * (1 - wl.wl_discount)) - (ps.ps_supplycost * wl.wl_quantity)) AS sum_profit
FROM 
    nation n
JOIN 
    supplier s ON n.n_nationkey = s.s_nationkey
JOIN 
    partsupp ps ON s.s_suppkey = ps.ps_suppkey
JOIN 
    part p ON ps.ps_partkey = p.p_partkey
JOIN 
    web_lineitem wl ON p.p_partkey = wl.wl_partkey AND ps.ps_suppkey = wl.wl_suppkey
JOIN 
    orders o ON wl.wl_orderkey = o.o_orderkey
WHERE 
    p.p_name LIKE 'co%'
GROUP BY 
    n.n_name, o_year

UNION ALL

SELECT 
    n.n_name AS nation, 
    EXTRACT(YEAR FROM o.o_orderdate) AS o_year, 
    SUM((sl.sl_extendedprice * (1 - sl.sl_discount)) - (ps.ps_supplycost * sl.sl_quantity)) AS sum_profit
FROM 
    nation n
JOIN 
    supplier s ON n.n_nationkey = s.s_nationkey
JOIN 
    partsupp ps ON s.s_suppkey = ps.ps_suppkey
JOIN 
    part p ON ps.ps_partkey = p.p_partkey
JOIN 
    store_lineitem sl ON p.p_partkey = sl.sl_partkey AND ps.ps_suppkey = sl.sl_suppkey
JOIN 
    orders o ON sl.sl_orderkey = o.o_orderkey
WHERE 
    p.p_name LIKE 'co%'
GROUP BY 
    n.n_name, o_year

ORDER BY 
    nation ASC, o_year DESC); 
 
 Its first 100 output rows are as follows:
 "ALGERIA                  "	1998	1052002.0642
"ALGERIA                  "	1998	1052002.0642
"ALGERIA                  "	1997	1593949.3116
"ALGERIA                  "	1997	1593949.3116
"ALGERIA                  "	1996	1674004.3856
"ALGERIA                  "	1996	1674004.3856
"ALGERIA                  "	1995	1276559.8161
"ALGERIA                  "	1995	1276559.8161
"ALGERIA                  "	1994	1965890.8676
"ALGERIA                  "	1994	1965890.8676
"ALGERIA                  "	1993	1510822.6255
"ALGERIA                  "	1993	1510822.6255
"ALGERIA                  "	1992	1800145.1623
"ALGERIA                  "	1992	1800145.1623
"ARGENTINA                "	1998	1155994.3853
"ARGENTINA                "	1998	1155994.3853
"ARGENTINA                "	1997	1970471.8380
"ARGENTINA                "	1997	1970471.8380
"ARGENTINA                "	1996	1532074.1751
"ARGENTINA                "	1996	1532074.1751
"ARGENTINA                "	1995	1627813.6808
"ARGENTINA                "	1995	1627813.6808
"ARGENTINA                "	1994	1294252.2194
"ARGENTINA                "	1994	1294252.2194
"ARGENTINA                "	1993	1485474.1724
"ARGENTINA                "	1993	1485474.1724
"ARGENTINA                "	1992	1489084.4192
"ARGENTINA                "	1992	1489084.4192
"BRAZIL                   "	1998	1390676.8122
"BRAZIL                   "	1998	1390676.8122
"BRAZIL                   "	1997	1698101.7000
"BRAZIL                   "	1997	1698101.7000
"BRAZIL                   "	1996	2163721.7083
"BRAZIL                   "	1996	2163721.7083
"BRAZIL                   "	1995	1820071.6039
"BRAZIL                   "	1995	1820071.6039
"BRAZIL                   "	1994	1872036.1512
"BRAZIL                   "	1994	1872036.1512
"BRAZIL                   "	1993	1718068.6076
"BRAZIL                   "	1993	1718068.6076
"BRAZIL                   "	1992	1686311.9194
"BRAZIL                   "	1992	1686311.9194
"CANADA                   "	1998	1364784.9340
"CANADA                   "	1998	1364784.9340
"CANADA                   "	1997	1870863.0988
"CANADA                   "	1997	1870863.0988
"CANADA                   "	1996	1678822.8044
"CANADA                   "	1996	1678822.8044
"CANADA                   "	1995	1712189.6760
"CANADA                   "	1995	1712189.6760
"CANADA                   "	1994	2201766.0900
"CANADA                   "	1994	2201766.0900
"CANADA                   "	1993	1856081.5689
"CANADA                   "	1993	1856081.5689
"CANADA                   "	1992	2337340.3298
"CANADA                   "	1992	2337340.3298
"CHINA                    "	1998	1807956.5374
"CHINA                    "	1998	1807956.5374
"CHINA                    "	1997	2161748.9774
"CHINA                    "	1997	2161748.9774
"CHINA                    "	1996	2764451.3784
"CHINA                    "	1996	2764451.3784
"CHINA                    "	1995	2743852.7639
"CHINA                    "	1995	2743852.7639
"CHINA                    "	1994	2635442.1408
"CHINA                    "	1994	2635442.1408
"CHINA                    "	1993	2303393.9672
"CHINA                    "	1993	2303393.9672
"CHINA                    "	1992	2091146.2338
"CHINA                    "	1992	2091146.2338
"EGYPT                    "	1998	1240718.7886
"EGYPT                    "	1998	1240718.7886
"EGYPT                    "	1997	2057461.1118
"EGYPT                    "	1997	2057461.1118
"EGYPT                    "	1996	1851225.9045
"EGYPT                    "	1996	1851225.9045
"EGYPT                    "	1995	1764736.4248
"EGYPT                    "	1995	1764736.4248
"EGYPT                    "	1994	1721238.8448
"EGYPT                    "	1994	1721238.8448
"EGYPT                    "	1993	2037391.5551
"EGYPT                    "	1993	2037391.5551
"EGYPT                    "	1992	1704574.9539
"EGYPT                    "	1992	1704574.9539
"ETHIOPIA                 "	1998	1060907.1200
"ETHIOPIA                 "	1998	1060907.1200
"ETHIOPIA                 "	1997	1605816.6141
"ETHIOPIA                 "	1997	1605816.6141
"ETHIOPIA                 "	1996	1294992.9071
"ETHIOPIA                 "	1996	1294992.9071
"ETHIOPIA                 "	1995	1343012.7262
"ETHIOPIA                 "	1995	1343012.7262
"ETHIOPIA                 "	1994	1597788.6859
"ETHIOPIA                 "	1994	1597788.6859
"ETHIOPIA                 "	1993	1675247.0661
"ETHIOPIA                 "	1993	1675247.0661
"ETHIOPIA                 "	1992	1610161.8525
"ETHIOPIA                 "	1992	1610161.8525
"FRANCE                   "	1998	1111624.4063
"FRANCE                   "	1998	1111624.4063
Fix the query.

"""

text_2_sql_prompt = """Give me SQL for the following text:
The Query finds, for each nation and each year, the profit for all parts ordered in that
year that contain a specified substring in their names and that were filled by a supplier in that nation. The profit is
defined as the sum of [(l_extendedprice*(1-l_discount)) - (ps_supplycost * l_quantity)] for all lineitems describing
parts in the specified line. The query lists the nations in ascending alphabetical order and, for each nation, the year
and profit in descending order by year (most recent first).
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

(Select n_name as nation, o_orderdate as o_year, Sum(-ps_supplycost*wl_quantity + wl_extendedprice*(1 - wl_discount)) as sum_profit 
 From nation, orders, part, partsupp, supplier, web_lineitem 
 Where orders.o_orderkey = web_lineitem.wl_orderkey
 and part.p_partkey = partsupp.ps_partkey
 and partsupp.ps_partkey = web_lineitem.wl_partkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and supplier.s_suppkey = web_lineitem.wl_suppkey
 and nation.n_nationkey = supplier.s_nationkey
 and part.p_name LIKE 'co%' 
 Group By n_name 
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
 Group By n_name 
 Order By nation asc);     
"""


def count_tokens(text):
    encoding = tiktoken.encoding_for_model("gpt-4o")
    tokens = encoding.encode(text)
    return len(tokens)


def one_round():
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "user",
                "content": f"{text_2_sql_prompt}",
            },
        ], temperature=0, stream=False
    )
    reply = response.choices[0].message.content
    print(reply)
    c_token = count_tokens(text_2_sql_prompt)
    print(f"\nToken count = {c_token}\n")


orig_out = sys.stdout
f = open('chatgpt_tpch_sql.py', 'w')
sys.stdout = f
one_round()
sys.stdout = orig_out
f.close()
