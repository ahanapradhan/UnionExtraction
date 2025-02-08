import sys

import tiktoken
from openai import OpenAI

# gets API Key from environment variable OPENAI_API_KEY
client = OpenAI()

next_prompt = """

But you formulated the following query:
SELECT 
    YEAR(shipdate) AS year,
    MONTH(shipdate) AS month,
    SUM(CASE WHEN p_type LIKE 'PROMO%' THEN extendedprice * (1 - discount) ELSE 0 END) / SUM(extendedprice * (1 - discount)) * 100 AS promo_revenue_percentage
FROM (
    SELECT sl_shipdate AS shipdate, sl_extendedprice AS extendedprice, sl_discount AS discount, p_type
    FROM store_lineitem
    JOIN part ON store_lineitem.sl_partkey = part.p_partkey
    UNION ALL
    SELECT wl_shipdate AS shipdate, wl_extendedprice AS extendedprice, wl_discount AS discount, p_type
    FROM web_lineitem
    JOIN part ON web_lineitem.wl_partkey = part.p_partkey
) AS all_lineitems
GROUP BY YEAR(shipdate), MONTH(shipdate);

Which gives the following output, different from the expectation mentioned above:
1992	1	16.97176564032649054700
1992	2	16.66804489005164278000
1992	3	16.61232980576074420200
1992	4	16.70040661689721367400
1992	5	16.82180586001946410900
1992	6	16.47664667345275182900
1992	7	16.82903878322169732800
1992	8	16.80486302403979597100
1992	9	16.61180351469074865900
1992	10	16.59734880188812988900
1992	11	16.99168396001022166500
1992	12	16.35107276019392301300
1993	1	16.64344662101902751200
1993	2	16.52642226044642481400
1993	3	16.03788783299855709800
1993	4	16.45904171909886538100
1993	5	16.81400078034286409100
1993	6	16.75348689883571102500
1993	7	16.71011193366758270500
1993	8	16.47204023867521424200
1993	9	16.37151533535018778100
1993	10	17.02391472043799068300
1993	11	16.16523703666708218900
1993	12	16.79684285928120126900
1994	1	16.50109954603049911900
1994	2	16.15694817469332447700
1994	3	16.70819236315983032900
1994	4	16.46932401243986439300
1994	5	16.57492092656828500700
1994	6	16.54516332166984874900
1994	7	16.74608933850450362100
1994	8	17.07865584369848050500
1994	9	16.90016309992658239100
1994	10	16.65163740836587583400
1994	11	16.74023176740742969900
1994	12	16.47559419489590719500
1995	1	16.92270564527025649000
1995	2	16.83636326609504978400
1995	3	16.54805554616333638500
1995	4	16.73025817014522010200
1995	5	16.42769589751879457700
1995	6	16.95184762035901484100
1995	7	16.37543825623969464900
1995	8	16.49161928043903966700
1995	9	16.46708840222347933200
1995	10	16.68425467110025147000
1995	11	16.40949466869126623700
1995	12	16.47328027165203273900
1996	1	16.95005665154896062900
1996	2	16.54610749893524265800
1996	3	16.52134928333364713700
1996	4	16.61724935260654677600
1996	5	16.53341316792430143600
1996	6	16.53541807149321993100
1996	7	16.39706756112408511700
1996	8	16.52034385352041752900
1996	9	16.57503283477722379200
1996	10	16.63601749247412841400
1996	11	16.76791266687513262100
1996	12	16.32984417894051562800
1997	1	16.65041762786498481500
1997	2	16.41510764079654541400
1997	3	16.37856175893195849600
1997	4	16.26746005135828691900
1997	5	16.73959623293940329800
1997	6	16.80532549027308183400
1997	7	16.16461177957769496100
1997	8	16.18500338454358311900
1997	9	16.69285154883621476600
1997	10	16.77298773350473540700
1997	11	16.45120295401345662700
1997	12	16.55421932729774698500
1998	1	16.41464335122550439400
1998	2	16.87029670910321358000
1998	3	16.50210637999493070100
1998	4	16.58156565617124292200
1998	5	16.52455811778765344800
1998	6	16.91064445080181596000
1998	7	16.41994128388103658000
1998	8	16.68852539826017267900
1998	9	17.09202013546535216900
1998	10	16.78205702634769694400
1998	11	16.24602727211131404600
1998	12	0.000000000000000000000000.

Fix the query.
Use all the predicates used in the ***seed query***.
"""
text_2_sql_prompt = """Give me SQL for the following text:
The Query determines what percentage of the revenue in a given year and month was derived from
promotional parts. The query considers only parts actually shipped in that month and gives the percentage. Revenue
is defined as (extended price * (1-discount)).

The output should be:
16.9227056452702565

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
4. Use all the predicates of the following query in your SQL.

Refine the following ***seed query*** SQL to reach to the final query:

 (Select Sum(0) as promo_revenue 
 From part, store_lineitem 
 Where part.p_partkey = store_lineitem.sl_partkey
 and store_lineitem.sl_shipdate between '1995-01-01' and '1995-01-31')
 UNION ALL  
 (Select Sum(0) as promo_revenue 
 From part, web_lineitem 
 Where part.p_partkey = web_lineitem.wl_partkey
 and web_lineitem.wl_shipdate between '1995-01-01' and '1995-01-31');      
"""


def count_tokens(text):
    encoding = tiktoken.encoding_for_model("gpt-4o")
    tokens = encoding.encode(text)
    return len(tokens)

shot_2 = """You formulathe the following query:
SELECT 
    EXTRACT(YEAR FROM shipdate) AS year,
    EXTRACT(MONTH FROM shipdate) AS month,
    SUM(CASE WHEN p_type LIKE 'PROMO%' THEN extendedprice * (1 - discount) ELSE 0 END) / SUM(extendedprice * (1 - discount)) * 100 AS promo_revenue_percentage
FROM (
    SELECT sl_shipdate AS shipdate, sl_extendedprice AS extendedprice, sl_discount AS discount, p_type
    FROM store_lineitem
    JOIN part ON store_lineitem.sl_partkey = part.p_partkey
    WHERE sl_shipdate BETWEEN '1995-01-01' AND '1995-01-31'
    UNION ALL
    SELECT wl_shipdate AS shipdate, wl_extendedprice AS extendedprice, wl_discount AS discount, p_type
    FROM web_lineitem
    JOIN part ON web_lineitem.wl_partkey = part.p_partkey
    WHERE wl_shipdate BETWEEN '1995-01-01' AND '1995-01-31'
) AS all_lineitems
GROUP BY EXTRACT(YEAR FROM shipdate), EXTRACT(MONTH FROM shipdate);

Its output is as follows:
1995	1	16.92270564527025649000.

Fix the query, but matching all the projections."""

def one_round():
    text = f"{text_2_sql_prompt}\n{shot_2}"
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
