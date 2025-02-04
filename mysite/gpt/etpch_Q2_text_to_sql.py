import sys

import tiktoken
from openai import OpenAI

# gets API Key from environment variable OPENAI_API_KEY
client = OpenAI()

text_2_sql_prompt = """Give me SQL for the following text:

The Query finds, in a given region, for each part of a certain type and size, the supplier who
can supply it at minimum cost. If several suppliers in that region offer the desired part type and size at the same
(minimum) cost, the query lists the parts from suppliers with the 100 highest account balances. For each supplier,
the query lists the supplier's account balance, name and nation; the part's number and manufacturer; the supplier's
address, phone number and comment information.

Give only the SQL, do not add any explaination.
MANDATORY RULE: Never use any window function. Never use RANK OVER PARTITION BY.
Put the SQL within python style comment quotes.

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
   


Mandatory instructions on query formulation:
Additionally, use the following guidelines:
1. Do not use redundant join conditions.
2. Do not use any predicate with place holder parameter.
3. No attribute in the database has NULL value.
4. Do not use predicate on any other attribute than that are used in the following query:

 Select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment 
 From nation, part, partsupp, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and part.p_partkey = partsupp.ps_partkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and part.p_size = 15
 and region.r_name = 'EUROPE'
 and part.p_type LIKE '%BRASS' 
 Order By s_acctbal desc, n_name asc, s_name asc, p_partkey asc
Limit 100;   

5. The attributes present in projections are accurate, use them in projection. 
6. Also use the projection aliases used in the query.
7. Use the tables present in the FROM clause of the query in your query. No table appears more than once in the query.
8. Order by of the above query is accurate, reuse it.
9. Text-based filter predicates of the above query are accurate, however they may be 
scattered around subqueries in the expected query.
"""

next_prompt = """The query you produced is as follows:
SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
FROM (
    SELECT supplier.s_suppkey, supplier.s_acctbal, supplier.s_name, nation.n_name, part.p_partkey, part.p_mfgr, supplier.s_address, supplier.s_phone, supplier.s_comment, partsupp.ps_supplycost
    FROM nation, part, partsupp, region, supplier
    WHERE nation.n_nationkey = supplier.s_nationkey
      AND nation.n_regionkey = region.r_regionkey
      AND part.p_partkey = partsupp.ps_partkey
      AND partsupp.ps_suppkey = supplier.s_suppkey
      AND part.p_size = 15
      AND region.r_name = 'EUROPE'
      AND part.p_type LIKE '%BRASS'
) AS subquery
JOIN (
    SELECT ps_partkey, MIN(ps_supplycost) AS min_cost
    FROM partsupp
    GROUP BY ps_partkey
) AS min_costs
ON subquery.p_partkey = min_costs.ps_partkey AND subquery.ps_supplycost = min_costs.min_cost
ORDER BY s_acctbal DESC, n_name ASC, s_name ASC, p_partkey ASC
LIMIT 100;

Try duplicating filter predicates present in the outer query into the inner query as well. 
Adjust the tables and joins accordingly.
"""

next_prompt_output = """
Its output is as follows:
9938.53	"Supplier#000005359       "	"UNITED KINGDOM           "	185358	"Manufacturer#4           "	"QKuHYh,vZGiwu2FWEJoLDx04"	"33-429-790-6131"	"uriously regular requests hag"
9938.53	"Supplier#000005359       "	"UNITED KINGDOM           "	185358	"Manufacturer#4           "	"QKuHYh,vZGiwu2FWEJoLDx04"	"33-429-790-6131"	"uriously regular requests hag"
9817.10	"Supplier#000002352       "	"RUSSIA                   "	124815	"Manufacturer#2           "	"4LfoHUZjgjEbAKw TgdKcgOc4D4uCYw"	"32-551-831-1437"	"wake carefully alongside of the carefully final ex"
9817.10	"Supplier#000002352       "	"RUSSIA                   "	124815	"Manufacturer#2           "	"4LfoHUZjgjEbAKw TgdKcgOc4D4uCYw"	"32-551-831-1437"	"wake carefully alongside of the carefully final ex"
9721.95	"Supplier#000008757       "	"UNITED KINGDOM           "	156241	"Manufacturer#3           "	"Atg6GnM4dT2"	"33-821-407-2995"	"eep furiously sauternes; quickl"
9721.95	"Supplier#000008757       "	"UNITED KINGDOM           "	156241	"Manufacturer#3           "	"Atg6GnM4dT2"	"33-821-407-2995"	"eep furiously sauternes; quickl"
9643.55	"Supplier#000005148       "	"ROMANIA                  "	107617	"Manufacturer#1           "	"kT4ciVFslx9z4s79p Js825"	"29-252-617-4850"	"final excuses. final ideas boost quickly furiously speci"
9643.55	"Supplier#000005148       "	"ROMANIA                  "	107617	"Manufacturer#1           "	"kT4ciVFslx9z4s79p Js825"	"29-252-617-4850"	"final excuses. final ideas boost quickly furiously speci"
9612.94	"Supplier#000003228       "	"ROMANIA                  "	198189	"Manufacturer#4           "	"KDdpNKN3cWu7ZSrbdqp7AfSLxx,qWB"	"29-325-784-8187"	"warhorses. quickly even deposits sublate daringly ironic instructions. slyly blithe t"
9612.94	"Supplier#000003228       "	"ROMANIA                  "	198189	"Manufacturer#4           "	"KDdpNKN3cWu7ZSrbdqp7AfSLxx,qWB"	"29-325-784-8187"	"warhorses. quickly even deposits sublate daringly ironic instructions. slyly blithe t"
9571.83	"Supplier#000004305       "	"ROMANIA                  "	179270	"Manufacturer#2           "	"qNHZ7WmCzygwMPRDO9Ps"	"29-973-481-1831"	"kly carefully express asymptotes. furiou"
9571.83	"Supplier#000004305       "	"ROMANIA                  "	179270	"Manufacturer#2           "	"qNHZ7WmCzygwMPRDO9Ps"	"29-973-481-1831"	"kly carefully express asymptotes. furiou"
9558.10	"Supplier#000003532       "	"UNITED KINGDOM           "	88515	"Manufacturer#4           "	"EOeuiiOn21OVpTlGguufFDFsbN1p0lhpxHp"	"33-152-301-2164"	" foxes. quickly even excuses use. slyly special foxes nag bl"
9558.10	"Supplier#000003532       "	"UNITED KINGDOM           "	88515	"Manufacturer#4           "	"EOeuiiOn21OVpTlGguufFDFsbN1p0lhpxHp"	"33-152-301-2164"	" foxes. quickly even excuses use. slyly special foxes nag bl"
9280.27	"Supplier#000007194       "	"ROMANIA                  "	47193	"Manufacturer#3           "	"zhRUQkBSrFYxIAXTfInj vyGRQjeK"	"29-318-454-2133"	"o beans haggle after the furiously unusual deposits. carefully silent dolphins cajole carefully"
9280.27	"Supplier#000007194       "	"ROMANIA                  "	47193	"Manufacturer#3           "	"zhRUQkBSrFYxIAXTfInj vyGRQjeK"	"29-318-454-2133"	"o beans haggle after the furiously unusual deposits. carefully silent dolphins cajole carefully"
9249.35	"Supplier#000003973       "	"FRANCE                   "	26466	"Manufacturer#1           "	"d18GiDsL6Wm2IsGXM,RZf1jCsgZAOjNYVThTRP4"	"16-722-866-1658"	"uests are furiously. regular tithes through the regular, final accounts cajole furiously above the q"
9249.35	"Supplier#000003973       "	"FRANCE                   "	26466	"Manufacturer#1           "	"d18GiDsL6Wm2IsGXM,RZf1jCsgZAOjNYVThTRP4"	"16-722-866-1658"	"uests are furiously. regular tithes through the regular, final accounts cajole furiously above the q"
9249.35	"Supplier#000003973       "	"FRANCE                   "	33972	"Manufacturer#1           "	"d18GiDsL6Wm2IsGXM,RZf1jCsgZAOjNYVThTRP4"	"16-722-866-1658"	"uests are furiously. regular tithes through the regular, final accounts cajole furiously above the q"
9249.35	"Supplier#000003973       "	"FRANCE                   "	33972	"Manufacturer#1           "	"d18GiDsL6Wm2IsGXM,RZf1jCsgZAOjNYVThTRP4"	"16-722-866-1658"	"uests are furiously. regular tithes through the regular, final accounts cajole furiously above the q"
9201.47	"Supplier#000009690       "	"UNITED KINGDOM           "	67183	"Manufacturer#5           "	"CB BnUTlmi5zdeEl7R7"	"33-121-267-9529"	"e even, even foxes. blithely ironic packages cajole regular packages. slyly final ide"
9201.47	"Supplier#000009690       "	"UNITED KINGDOM           "	67183	"Manufacturer#5           "	"CB BnUTlmi5zdeEl7R7"	"33-121-267-9529"	"e even, even foxes. blithely ironic packages cajole regular packages. slyly final ide"
9094.57	"Supplier#000004582       "	"RUSSIA                   "	39575	"Manufacturer#1           "	"WB0XkCSG3r,mnQ n,h9VIxjjr9ARHFvKgMDf"	"32-587-577-1351"	"jole. regular accounts sleep blithely frets. final pinto beans play furiously past the "
9094.57	"Supplier#000004582       "	"RUSSIA                   "	39575	"Manufacturer#1           "	"WB0XkCSG3r,mnQ n,h9VIxjjr9ARHFvKgMDf"	"32-587-577-1351"	"jole. regular accounts sleep blithely frets. final pinto beans play furiously past the "
8968.42	"Supplier#000010000       "	"ROMANIA                  "	119999	"Manufacturer#5           "	"aTGLEusCiL4F PDBdv665XBJhPyCOB0i"	"29-578-432-2146"	"ly regular foxes boost slyly. quickly special waters boost carefully ironi"
8968.42	"Supplier#000010000       "	"ROMANIA                  "	119999	"Manufacturer#5           "	"aTGLEusCiL4F PDBdv665XBJhPyCOB0i"	"29-578-432-2146"	"ly regular foxes boost slyly. quickly special waters boost carefully ironi"
8936.82	"Supplier#000007043       "	"UNITED KINGDOM           "	109512	"Manufacturer#1           "	"FVajceZInZdbJE6Z9XsRUxrUEpiwHDrOXi,1Rz"	"33-784-177-8208"	"efully regular courts. furiousl"
8936.82	"Supplier#000007043       "	"UNITED KINGDOM           "	109512	"Manufacturer#1           "	"FVajceZInZdbJE6Z9XsRUxrUEpiwHDrOXi,1Rz"	"33-784-177-8208"	"efully regular courts. furiousl"
8862.24	"Supplier#000003323       "	"ROMANIA                  "	73322	"Manufacturer#3           "	"W9 lYcsC9FwBqk3ItL"	"29-736-951-3710"	"ly pending ideas sleep about the furiously unu"
8862.24	"Supplier#000003323       "	"ROMANIA                  "	73322	"Manufacturer#3           "	"W9 lYcsC9FwBqk3ItL"	"29-736-951-3710"	"ly pending ideas sleep about the furiously unu"
8841.59	"Supplier#000005750       "	"ROMANIA                  "	100729	"Manufacturer#5           "	"Erx3lAgu0g62iaHF9x50uMH4EgeN9hEG"	"29-344-502-5481"	"gainst the pinto beans. fluffily unusual dependencies affix slyly even deposits."
8841.59	"Supplier#000005750       "	"ROMANIA                  "	100729	"Manufacturer#5           "	"Erx3lAgu0g62iaHF9x50uMH4EgeN9hEG"	"29-344-502-5481"	"gainst the pinto beans. fluffily unusual dependencies affix slyly even deposits."
8691.06	"Supplier#000004429       "	"UNITED KINGDOM           "	126892	"Manufacturer#2           "	"k,BQms5UhoAF1B2Asi,fLib"	"33-964-337-5038"	"efully express deposits kindle after the deposits. final "
8691.06	"Supplier#000004429       "	"UNITED KINGDOM           "	126892	"Manufacturer#2           "	"k,BQms5UhoAF1B2Asi,fLib"	"33-964-337-5038"	"efully express deposits kindle after the deposits. final "
8569.52	"Supplier#000005936       "	"RUSSIA                   "	5935	"Manufacturer#5           "	"jXaNZ6vwnEWJ2ksLZJpjtgt0bY2a3AU"	"32-644-251-7916"	". regular foxes nag carefully atop the regular, silent deposits. quickly regular packages "
8569.52	"Supplier#000005936       "	"RUSSIA                   "	5935	"Manufacturer#5           "	"jXaNZ6vwnEWJ2ksLZJpjtgt0bY2a3AU"	"32-644-251-7916"	". regular foxes nag carefully atop the regular, silent deposits. quickly regular packages "
8517.23	"Supplier#000009529       "	"RUSSIA                   "	37025	"Manufacturer#5           "	"e44R8o7JAIS9iMcr"	"32-565-297-8775"	"ove the even courts. furiously special platelets "
8517.23	"Supplier#000009529       "	"RUSSIA                   "	37025	"Manufacturer#5           "	"e44R8o7JAIS9iMcr"	"32-565-297-8775"	"ove the even courts. furiously special platelets "
8517.23	"Supplier#000009529       "	"RUSSIA                   "	59528	"Manufacturer#2           "	"e44R8o7JAIS9iMcr"	"32-565-297-8775"	"ove the even courts. furiously special platelets "
8517.23	"Supplier#000009529       "	"RUSSIA                   "	59528	"Manufacturer#2           "	"e44R8o7JAIS9iMcr"	"32-565-297-8775"	"ove the even courts. furiously special platelets "
8457.09	"Supplier#000009456       "	"UNITED KINGDOM           "	19455	"Manufacturer#1           "	"7SBhZs8gP1cJjT0Qf433YBk"	"33-858-440-4349"	"cing requests along the furiously unusual deposits promise among the furiously unus"
8457.09	"Supplier#000009456       "	"UNITED KINGDOM           "	19455	"Manufacturer#1           "	"7SBhZs8gP1cJjT0Qf433YBk"	"33-858-440-4349"	"cing requests along the furiously unusual deposits promise among the furiously unus"
8441.40	"Supplier#000003817       "	"FRANCE                   "	141302	"Manufacturer#2           "	"hU3fz3xL78"	"16-339-356-5115"	"ely even ideas. ideas wake slyly furiously unusual instructions. pinto beans sleep ag"
8441.40	"Supplier#000003817       "	"FRANCE                   "	141302	"Manufacturer#2           "	"hU3fz3xL78"	"16-339-356-5115"	"ely even ideas. ideas wake slyly furiously unusual instructions. pinto beans sleep ag"
8431.40	"Supplier#000002675       "	"ROMANIA                  "	5174	"Manufacturer#1           "	"HJFStOu9R5NGPOegKhgbzBdyvrG2yh8w"	"29-474-643-1443"	"ithely express pinto beans. blithely even foxes haggle. furiously regular theodol"
8431.40	"Supplier#000002675       "	"ROMANIA                  "	5174	"Manufacturer#1           "	"HJFStOu9R5NGPOegKhgbzBdyvrG2yh8w"	"29-474-643-1443"	"ithely express pinto beans. blithely even foxes haggle. furiously regular theodol"
8348.74	"Supplier#000008851       "	"FRANCE                   "	66344	"Manufacturer#4           "	"nWxi7GwEbjhw1"	"16-796-240-2472"	" boldly final deposits. regular, even instructions detect slyly. fluffily unusual pinto bea"
8348.74	"Supplier#000008851       "	"FRANCE                   "	66344	"Manufacturer#4           "	"nWxi7GwEbjhw1"	"16-796-240-2472"	" boldly final deposits. regular, even instructions detect slyly. fluffily unusual pinto bea"
8231.61	"Supplier#000009558       "	"RUSSIA                   "	192000	"Manufacturer#2           "	"mcdgen,yT1iJDHDS5fV"	"32-762-137-5858"	" foxes according to the furi"
8231.61	"Supplier#000009558       "	"RUSSIA                   "	192000	"Manufacturer#2           "	"mcdgen,yT1iJDHDS5fV"	"32-762-137-5858"	" foxes according to the furi"
8102.62	"Supplier#000003347       "	"UNITED KINGDOM           "	18344	"Manufacturer#5           "	"m CtXS2S16i"	"33-454-274-8532"	"egrate with the slyly bold instructions. special foxes haggle silently among the"
8102.62	"Supplier#000003347       "	"UNITED KINGDOM           "	18344	"Manufacturer#5           "	"m CtXS2S16i"	"33-454-274-8532"	"egrate with the slyly bold instructions. special foxes haggle silently among the"
8042.09	"Supplier#000003245       "	"RUSSIA                   "	135705	"Manufacturer#4           "	"Dh8Ikg39onrbOL4DyTfGw8a9oKUX3d9Y"	"32-836-132-8872"	"osits. packages cajole slyly. furiously regular deposits cajole slyly. q"
8042.09	"Supplier#000003245       "	"RUSSIA                   "	135705	"Manufacturer#4           "	"Dh8Ikg39onrbOL4DyTfGw8a9oKUX3d9Y"	"32-836-132-8872"	"osits. packages cajole slyly. furiously regular deposits cajole slyly. q"
7950.37	"Supplier#000008101       "	"GERMANY                  "	33094	"Manufacturer#5           "	"kkYvL6IuvojJgTNG IKkaXQDYgx8ILohj"	"17-627-663-8014"	"arefully unusual requests x-ray above the quickly final deposits. "
7950.37	"Supplier#000008101       "	"GERMANY                  "	33094	"Manufacturer#5           "	"kkYvL6IuvojJgTNG IKkaXQDYgx8ILohj"	"17-627-663-8014"	"arefully unusual requests x-ray above the quickly final deposits. "
7937.93	"Supplier#000009012       "	"ROMANIA                  "	83995	"Manufacturer#2           "	"iUiTziH,Ek3i4lwSgunXMgrcTzwdb"	"29-250-925-9690"	"to the blithely ironic deposits nag sly"
7937.93	"Supplier#000009012       "	"ROMANIA                  "	83995	"Manufacturer#2           "	"iUiTziH,Ek3i4lwSgunXMgrcTzwdb"	"29-250-925-9690"	"to the blithely ironic deposits nag sly"
7852.45	"Supplier#000005864       "	"RUSSIA                   "	8363	"Manufacturer#4           "	"WCNfBPZeSXh3h,c"	"32-454-883-3821"	"usly unusual pinto beans. brave ideas sleep carefully quickly ironi"
7852.45	"Supplier#000005864       "	"RUSSIA                   "	8363	"Manufacturer#4           "	"WCNfBPZeSXh3h,c"	"32-454-883-3821"	"usly unusual pinto beans. brave ideas sleep carefully quickly ironi"
7843.52	"Supplier#000006683       "	"FRANCE                   "	11680	"Manufacturer#4           "	"2Z0JGkiv01Y00oCFwUGfviIbhzCdy"	"16-464-517-8943"	" express, final pinto beans x-ray slyly asymptotes. unusual, unusual"
7843.52	"Supplier#000006683       "	"FRANCE                   "	11680	"Manufacturer#4           "	"2Z0JGkiv01Y00oCFwUGfviIbhzCdy"	"16-464-517-8943"	" express, final pinto beans x-ray slyly asymptotes. unusual, unusual"
7763.74	"Supplier#000002162       "	"FRANCE                   "	37155	"Manufacturer#4           "	"6ya g3MW991n9JfhxSrvgM"	"16-859-508-4893"	"eep slyly ironic accounts."
7763.74	"Supplier#000002162       "	"FRANCE                   "	37155	"Manufacturer#4           "	"6ya g3MW991n9JfhxSrvgM"	"16-859-508-4893"	"eep slyly ironic accounts."
7721.78	"Supplier#000008438       "	"GERMANY                  "	53427	"Manufacturer#3           "	"MHX2cideiqjxZgCyenirqSChO"	"17-510-783-5625"	"es nod slyly furiously final ideas. blithely daring packages sleep bravely f"
7721.78	"Supplier#000008438       "	"GERMANY                  "	53427	"Manufacturer#3           "	"MHX2cideiqjxZgCyenirqSChO"	"17-510-783-5625"	"es nod slyly furiously final ideas. blithely daring packages sleep bravely f"
7669.38	"Supplier#000006016       "	"FRANCE                   "	1015	"Manufacturer#4           "	"OmiSL2cwQ6YGQncYNAj8WZAFgz"	"16-757-121-2301"	"ffily even pinto beans grow ruthlessly pac"
7669.38	"Supplier#000006016       "	"FRANCE                   "	1015	"Manufacturer#4           "	"OmiSL2cwQ6YGQncYNAj8WZAFgz"	"16-757-121-2301"	"ffily even pinto beans grow ruthlessly pac"
7627.42	"Supplier#000009880       "	"FRANCE                   "	182325	"Manufacturer#3           "	"CQzqP0YiUFIvgwHsVPbbq"	"16-486-273-8984"	"structions nag quickly carefully daring requests. fluffily unusua"
7627.42	"Supplier#000009880       "	"FRANCE                   "	182325	"Manufacturer#3           "	"CQzqP0YiUFIvgwHsVPbbq"	"16-486-273-8984"	"structions nag quickly carefully daring requests. fluffily unusua"
7585.24	"Supplier#000006632       "	"ROMANIA                  "	111609	"Manufacturer#2           "	"TkcuZHSWRFtos 0fylpyqk"	"29-554-139-5114"	"s across the furiously sil"
7585.24	"Supplier#000006632       "	"ROMANIA                  "	111609	"Manufacturer#2           "	"TkcuZHSWRFtos 0fylpyqk"	"29-554-139-5114"	"s across the furiously sil"
7469.73	"Supplier#000008756       "	"ROMANIA                  "	86247	"Manufacturer#2           "	"cs50kLQEky4gv"	"29-880-355-6540"	"r requests nag against the sly"
7469.73	"Supplier#000008756       "	"ROMANIA                  "	86247	"Manufacturer#2           "	"cs50kLQEky4gv"	"29-880-355-6540"	"r requests nag against the sly"
7392.78	"Supplier#000000170       "	"UNITED KINGDOM           "	120169	"Manufacturer#4           "	"RtsXQ,SunkA XHy9"	"33-803-340-5398"	"ake carefully across the quickly"
7392.78	"Supplier#000000170       "	"UNITED KINGDOM           "	120169	"Manufacturer#4           "	"RtsXQ,SunkA XHy9"	"33-803-340-5398"	"ake carefully across the quickly"
7241.31	"Supplier#000000809       "	"RUSSIA                   "	80808	"Manufacturer#1           "	"dPqPaxh,IbS"	"32-172-990-2830"	" accounts. express dolphin"
7241.31	"Supplier#000000809       "	"RUSSIA                   "	80808	"Manufacturer#1           "	"dPqPaxh,IbS"	"32-172-990-2830"	" accounts. express dolphin"
7148.26	"Supplier#000005680       "	"UNITED KINGDOM           "	5679	"Manufacturer#3           "	"hWkoAtOkvn"	"33-547-203-1846"	"d, even ideas sleep slyly. silent"
7148.26	"Supplier#000005680       "	"UNITED KINGDOM           "	5679	"Manufacturer#3           "	"hWkoAtOkvn"	"33-547-203-1846"	"d, even ideas sleep slyly. silent"
6955.39	"Supplier#000003665       "	"ROMANIA                  "	21162	"Manufacturer#4           "	"vQEsRjcsJukdwIQ6F7A0g8WYj74LNFMu"	"29-931-790-4275"	"eposits play furiously ideas. th"
6955.39	"Supplier#000003665       "	"ROMANIA                  "	21162	"Manufacturer#4           "	"vQEsRjcsJukdwIQ6F7A0g8WYj74LNFMu"	"29-931-790-4275"	"eposits play furiously ideas. th"
6820.97	"Supplier#000002845       "	"UNITED KINGDOM           "	160328	"Manufacturer#1           "	"ZOlKEPI,8ftemk3cAGokylKstRcZiBT0sc"	"33-639-575-6452"	" furiously ironic requests. carefully final pinto beans after the blithely ironic orbi"
6820.97	"Supplier#000002845       "	"UNITED KINGDOM           "	160328	"Manufacturer#1           "	"ZOlKEPI,8ftemk3cAGokylKstRcZiBT0sc"	"33-639-575-6452"	" furiously ironic requests. carefully final pinto beans after the blithely ironic orbi"
6806.27	"Supplier#000004258       "	"UNITED KINGDOM           "	124257	"Manufacturer#2           "	"oXKtTTKlpcYIbuiMgfnP0sWD2P2Ngas"	"33-173-309-5477"	". ironic, even requests above the regular, final"
6806.27	"Supplier#000004258       "	"UNITED KINGDOM           "	124257	"Manufacturer#2           "	"oXKtTTKlpcYIbuiMgfnP0sWD2P2Ngas"	"33-173-309-5477"	". ironic, even requests above the regular, final"
6806.27	"Supplier#000004258       "	"UNITED KINGDOM           "	149229	"Manufacturer#1           "	"oXKtTTKlpcYIbuiMgfnP0sWD2P2Ngas"	"33-173-309-5477"	". ironic, even requests above the regular, final"
6806.27	"Supplier#000004258       "	"UNITED KINGDOM           "	149229	"Manufacturer#1           "	"oXKtTTKlpcYIbuiMgfnP0sWD2P2Ngas"	"33-173-309-5477"	". ironic, even requests above the regular, final"
6778.63	"Supplier#000008421       "	"FRANCE                   "	160872	"Manufacturer#4           "	"x2sr5EHkwDOimr0n9uWd,cDEXyIEXngBLI"	"16-554-443-4756"	" according to the pinto beans use above the carefully ironic foxes. pinto beans na"
6778.63	"Supplier#000008421       "	"FRANCE                   "	160872	"Manufacturer#4           "	"x2sr5EHkwDOimr0n9uWd,cDEXyIEXngBLI"	"16-554-443-4756"	" according to the pinto beans use above the carefully ironic foxes. pinto beans na"
6611.85	"Supplier#000001189       "	"GERMANY                  "	126164	"Manufacturer#2           "	"xYOLJtZstk3lh 2O8H231cTkSQ8rKbNCC,i9vZY"	"17-828-994-2511"	"ithely even platelets. quickly express packages boost. slyly regular deposits above th"
6611.85	"Supplier#000001189       "	"GERMANY                  "	126164	"Manufacturer#2           "	"xYOLJtZstk3lh 2O8H231cTkSQ8rKbNCC,i9vZY"	"17-828-994-2511"	"ithely even platelets. quickly express packages boost. slyly regular deposits above th"
6565.69	"Supplier#000001644       "	"ROMANIA                  "	121643	"Manufacturer#2           "	"ChjhHjLPsOyLPxmE"	"29-474-678-9070"	"furiously unusual pinto beans: final pinto beans wake furiously above the packages. account"
6565.69	"Supplier#000001644       "	"ROMANIA                  "	121643	"Manufacturer#2           "	"ChjhHjLPsOyLPxmE"	"29-474-678-9070"	"furiously unusual pinto beans: final pinto beans wake furiously above the packages. account"
6505.25	"Supplier#000009747       "	"ROMANIA                  "	189746	"Manufacturer#5           "	"jdub6FZMEJIwV3uO"	"29-910-833-4121"	"nts are furiously. blithely unusual requests accordin"
6505.25	"Supplier#000009747       "	"ROMANIA                  "	189746	"Manufacturer#5           "	"jdub6FZMEJIwV3uO"	"29-910-833-4121"	"nts are furiously. blithely unusual requests accordin"
6502.62	"Supplier#000008632       "	"ROMANIA                  "	48631	"Manufacturer#5           "	"QqHtvzhTWJlr7SJm1n,bqauRGd5XFIlO"	"29-543-253-5866"	" deposits cajole fluffily ironic packages. furio"
6502.62	"Supplier#000008632       "	"ROMANIA                  "	48631	"Manufacturer#5           "	"QqHtvzhTWJlr7SJm1n,bqauRGd5XFIlO"	"29-543-253-5866"	" deposits cajole fluffily ironic packages. furio"
6107.04	"Supplier#000007163       "	"RUSSIA                   "	69644	"Manufacturer#5           "	"9jzdDoHPLZ6gMt7GzSLqP Sdn10zYViXoNTT8XO"	"32-457-558-8569"	"sts are. instructions sleep carefully across the ironic foxes. carefully qu"
6107.04	"Supplier#000007163       "	"RUSSIA                   "	69644	"Manufacturer#5           "	"9jzdDoHPLZ6gMt7GzSLqP Sdn10zYViXoNTT8XO"	"32-457-558-8569"	"sts are. instructions sleep carefully across the ironic foxes. carefully qu"
"""

next_shot = """

The output produced by the expected query should be:
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
9847.57	"Supplier#000006345       "	"FRANCE                   "	86344	"Manufacturer#1           "	"VSt3rzk3qG698u6ld8HhOByvrTcSTSvQlDQDag"	"16-886-766-7945"	"ges. slyly regular requests are. ruthless, express excuses cajole blithely across the unu"
9847.57	"Supplier#000006345       "	"FRANCE                   "	86344	"Manufacturer#1           "	"VSt3rzk3qG698u6ld8HhOByvrTcSTSvQlDQDag"	"16-886-766-7945"	"ges. slyly regular requests are. ruthless, express excuses cajole blithely across the unu"
9847.57	"Supplier#000006345       "	"FRANCE                   "	173827	"Manufacturer#2           "	"VSt3rzk3qG698u6ld8HhOByvrTcSTSvQlDQDag"	"16-886-766-7945"	"ges. slyly regular requests are. ruthless, express excuses cajole blithely across the unu"
9847.57	"Supplier#000006345       "	"FRANCE                   "	173827	"Manufacturer#2           "	"VSt3rzk3qG698u6ld8HhOByvrTcSTSvQlDQDag"	"16-886-766-7945"	"ges. slyly regular requests are. ruthless, express excuses cajole blithely across the unu"
9836.93	"Supplier#000007342       "	"RUSSIA                   "	4841	"Manufacturer#4           "	"JOlK7C1,7xrEZSSOw"	"32-399-414-5385"	"blithely carefully bold theodolites. fur"
9836.93	"Supplier#000007342       "	"RUSSIA                   "	4841	"Manufacturer#4           "	"JOlK7C1,7xrEZSSOw"	"32-399-414-5385"	"blithely carefully bold theodolites. fur"
9817.10	"Supplier#000002352       "	"RUSSIA                   "	124815	"Manufacturer#2           "	"4LfoHUZjgjEbAKw TgdKcgOc4D4uCYw"	"32-551-831-1437"	"wake carefully alongside of the carefully final ex"
9817.10	"Supplier#000002352       "	"RUSSIA                   "	124815	"Manufacturer#2           "	"4LfoHUZjgjEbAKw TgdKcgOc4D4uCYw"	"32-551-831-1437"	"wake carefully alongside of the carefully final ex"
9817.10	"Supplier#000002352       "	"RUSSIA                   "	152351	"Manufacturer#3           "	"4LfoHUZjgjEbAKw TgdKcgOc4D4uCYw"	"32-551-831-1437"	"wake carefully alongside of the carefully final ex"
9817.10	"Supplier#000002352       "	"RUSSIA                   "	152351	"Manufacturer#3           "	"4LfoHUZjgjEbAKw TgdKcgOc4D4uCYw"	"32-551-831-1437"	"wake carefully alongside of the carefully final ex"
9739.86	"Supplier#000003384       "	"FRANCE                   "	138357	"Manufacturer#2           "	"o,Z3v4POifevE k9U1b 6J1ucX,I"	"16-494-913-5925"	"s after the furiously bold packages sleep fluffily idly final requests: quickly final"
9739.86	"Supplier#000003384       "	"FRANCE                   "	138357	"Manufacturer#2           "	"o,Z3v4POifevE k9U1b 6J1ucX,I"	"16-494-913-5925"	"s after the furiously bold packages sleep fluffily idly final requests: quickly final"
9721.95	"Supplier#000008757       "	"UNITED KINGDOM           "	156241	"Manufacturer#3           "	"Atg6GnM4dT2"	"33-821-407-2995"	"eep furiously sauternes; quickl"
9721.95	"Supplier#000008757       "	"UNITED KINGDOM           "	156241	"Manufacturer#3           "	"Atg6GnM4dT2"	"33-821-407-2995"	"eep furiously sauternes; quickl"
9681.33	"Supplier#000008406       "	"RUSSIA                   "	78405	"Manufacturer#1           "	",qUuXcftUl"	"32-139-873-8571"	"haggle slyly regular excuses. quic"
9681.33	"Supplier#000008406       "	"RUSSIA                   "	78405	"Manufacturer#1           "	",qUuXcftUl"	"32-139-873-8571"	"haggle slyly regular excuses. quic"
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
9357.45	"Supplier#000006188       "	"UNITED KINGDOM           "	138648	"Manufacturer#1           "	"g801,ssP8wpTk4Hm"	"33-583-607-1633"	"ously always regular packages. fluffily even accounts beneath the furiously final pack"
9357.45	"Supplier#000006188       "	"UNITED KINGDOM           "	138648	"Manufacturer#1           "	"g801,ssP8wpTk4Hm"	"33-583-607-1633"	"ously always regular packages. fluffily even accounts beneath the furiously final pack"
9352.04	"Supplier#000003439       "	"GERMANY                  "	170921	"Manufacturer#4           "	"qYPDgoiBGhCYxjgC"	"17-128-996-4650"	" according to the carefully bold ideas"
9352.04	"Supplier#000003439       "	"GERMANY                  "	170921	"Manufacturer#4           "	"qYPDgoiBGhCYxjgC"	"17-128-996-4650"	" according to the carefully bold ideas"
9312.97	"Supplier#000007807       "	"RUSSIA                   "	90279	"Manufacturer#5           "	"oGYMPCk9XHGB2PBfKRnHA"	"32-673-872-5854"	"ecial packages among the pending, even requests use regula"
9312.97	"Supplier#000007807       "	"RUSSIA                   "	90279	"Manufacturer#5           "	"oGYMPCk9XHGB2PBfKRnHA"	"32-673-872-5854"	"ecial packages among the pending, even requests use regula"
9312.97	"Supplier#000007807       "	"RUSSIA                   "	100276	"Manufacturer#5           "	"oGYMPCk9XHGB2PBfKRnHA"	"32-673-872-5854"	"ecial packages among the pending, even requests use regula"
9312.97	"Supplier#000007807       "	"RUSSIA                   "	100276	"Manufacturer#5           "	"oGYMPCk9XHGB2PBfKRnHA"	"32-673-872-5854"	"ecial packages among the pending, even requests use regula"
9280.27	"Supplier#000007194       "	"ROMANIA                  "	47193	"Manufacturer#3           "	"zhRUQkBSrFYxIAXTfInj vyGRQjeK"	"29-318-454-2133"	"o beans haggle after the furiously unusual deposits. carefully silent dolphins cajole carefully"
9280.27	"Supplier#000007194       "	"ROMANIA                  "	47193	"Manufacturer#3           "	"zhRUQkBSrFYxIAXTfInj vyGRQjeK"	"29-318-454-2133"	"o beans haggle after the furiously unusual deposits. carefully silent dolphins cajole carefully"
9274.80	"Supplier#000008854       "	"RUSSIA                   "	76346	"Manufacturer#3           "	"1xhLoOUM7I3mZ1mKnerw OSqdbb4QbGa"	"32-524-148-5221"	"y. courts do wake slyly. carefully ironic platelets haggle above the slyly regular the"
9274.80	"Supplier#000008854       "	"RUSSIA                   "	76346	"Manufacturer#3           "	"1xhLoOUM7I3mZ1mKnerw OSqdbb4QbGa"	"32-524-148-5221"	"y. courts do wake slyly. carefully ironic platelets haggle above the slyly regular the"
9249.35	"Supplier#000003973       "	"FRANCE                   "	26466	"Manufacturer#1           "	"d18GiDsL6Wm2IsGXM,RZf1jCsgZAOjNYVThTRP4"	"16-722-866-1658"	"uests are furiously. regular tithes through the regular, final accounts cajole furiously above the q"
9249.35	"Supplier#000003973       "	"FRANCE                   "	26466	"Manufacturer#1           "	"d18GiDsL6Wm2IsGXM,RZf1jCsgZAOjNYVThTRP4"	"16-722-866-1658"	"uests are furiously. regular tithes through the regular, final accounts cajole furiously above the q"
9249.35	"Supplier#000003973       "	"FRANCE                   "	33972	"Manufacturer#1           "	"d18GiDsL6Wm2IsGXM,RZf1jCsgZAOjNYVThTRP4"	"16-722-866-1658"	"uests are furiously. regular tithes through the regular, final accounts cajole furiously above the q"
9249.35	"Supplier#000003973       "	"FRANCE                   "	33972	"Manufacturer#1           "	"d18GiDsL6Wm2IsGXM,RZf1jCsgZAOjNYVThTRP4"	"16-722-866-1658"	"uests are furiously. regular tithes through the regular, final accounts cajole furiously above the q"
9208.70	"Supplier#000007769       "	"ROMANIA                  "	40256	"Manufacturer#5           "	"rsimdze 5o9P Ht7xS"	"29-964-424-9649"	"lites was quickly above the furiously ironic requests. slyly even foxes against the blithely bold "
9208.70	"Supplier#000007769       "	"ROMANIA                  "	40256	"Manufacturer#5           "	"rsimdze 5o9P Ht7xS"	"29-964-424-9649"	"lites was quickly above the furiously ironic requests. slyly even foxes against the blithely bold "
9201.47	"Supplier#000009690       "	"UNITED KINGDOM           "	67183	"Manufacturer#5           "	"CB BnUTlmi5zdeEl7R7"	"33-121-267-9529"	"e even, even foxes. blithely ironic packages cajole regular packages. slyly final ide"
9201.47	"Supplier#000009690       "	"UNITED KINGDOM           "	67183	"Manufacturer#5           "	"CB BnUTlmi5zdeEl7R7"	"33-121-267-9529"	"e even, even foxes. blithely ironic packages cajole regular packages. slyly final ide"
9192.10	"Supplier#000000115       "	"UNITED KINGDOM           "	85098	"Manufacturer#3           "	"nJ 2t0f7Ve,wL1,6WzGBJLNBUCKlsV"	"33-597-248-1220"	"es across the carefully express accounts boost caref"
9192.10	"Supplier#000000115       "	"UNITED KINGDOM           "	85098	"Manufacturer#3           "	"nJ 2t0f7Ve,wL1,6WzGBJLNBUCKlsV"	"33-597-248-1220"	"es across the carefully express accounts boost caref"
9189.98	"Supplier#000001226       "	"GERMANY                  "	21225	"Manufacturer#4           "	"qsLCqSvLyZfuXIpjz"	"17-725-903-1381"	" deposits. blithely bold excuses about the slyly bold forges wake "
9189.98	"Supplier#000001226       "	"GERMANY                  "	21225	"Manufacturer#4           "	"qsLCqSvLyZfuXIpjz"	"17-725-903-1381"	" deposits. blithely bold excuses about the slyly bold forges wake "
9128.97	"Supplier#000004311       "	"RUSSIA                   "	146768	"Manufacturer#5           "	"I8IjnXd7NSJRs594RxsRR0"	"32-155-440-7120"	"refully. blithely unusual asymptotes haggle "
9128.97	"Supplier#000004311       "	"RUSSIA                   "	146768	"Manufacturer#5           "	"I8IjnXd7NSJRs594RxsRR0"	"32-155-440-7120"	"refully. blithely unusual asymptotes haggle "
9104.83	"Supplier#000008520       "	"GERMANY                  "	150974	"Manufacturer#4           "	"RqRVDgD0ER J9 b41vR2,3"	"17-728-804-1793"	"ly about the blithely ironic depths. slyly final theodolites among the fluffily bold ideas print"
9104.83	"Supplier#000008520       "	"GERMANY                  "	150974	"Manufacturer#4           "	"RqRVDgD0ER J9 b41vR2,3"	"17-728-804-1793"	"ly about the blithely ironic depths. slyly final theodolites among the fluffily bold ideas print"
9101.00	"Supplier#000005791       "	"ROMANIA                  "	128254	"Manufacturer#5           "	"zub2zCV,jhHPPQqi,P2INAjE1zI n66cOEoXFG"	"29-549-251-5384"	"ts. notornis detect blithely above the carefully bold requests. blithely even package"
9101.00	"Supplier#000005791       "	"ROMANIA                  "	128254	"Manufacturer#5           "	"zub2zCV,jhHPPQqi,P2INAjE1zI n66cOEoXFG"	"29-549-251-5384"	"ts. notornis detect blithely above the carefully bold requests. blithely even package"
9094.57	"Supplier#000004582       "	"RUSSIA                   "	39575	"Manufacturer#1           "	"WB0XkCSG3r,mnQ n,h9VIxjjr9ARHFvKgMDf"	"32-587-577-1351"	"jole. regular accounts sleep blithely frets. final pinto beans play furiously past the "
9094.57	"Supplier#000004582       "	"RUSSIA                   "	39575	"Manufacturer#1           "	"WB0XkCSG3r,mnQ n,h9VIxjjr9ARHFvKgMDf"	"32-587-577-1351"	"jole. regular accounts sleep blithely frets. final pinto beans play furiously past the "
8996.87	"Supplier#000004702       "	"FRANCE                   "	102191	"Manufacturer#5           "	"8XVcQK23akp"	"16-811-269-8946"	"ickly final packages along the express plat"
8996.87	"Supplier#000004702       "	"FRANCE                   "	102191	"Manufacturer#5           "	"8XVcQK23akp"	"16-811-269-8946"	"ickly final packages along the express plat"
8996.14	"Supplier#000009814       "	"ROMANIA                  "	139813	"Manufacturer#2           "	"af0O5pg83lPU4IDVmEylXZVqYZQzSDlYLAmR"	"29-995-571-8781"	" dependencies boost quickly across the furiously pending requests! unusual dolphins play sl"
8996.14	"Supplier#000009814       "	"ROMANIA                  "	139813	"Manufacturer#2           "	"af0O5pg83lPU4IDVmEylXZVqYZQzSDlYLAmR"	"29-995-571-8781"	" dependencies boost quickly across the furiously pending requests! unusual dolphins play sl"
8968.42	"Supplier#000010000       "	"ROMANIA                  "	119999	"Manufacturer#5           "	"aTGLEusCiL4F PDBdv665XBJhPyCOB0i"	"29-578-432-2146"	"ly regular foxes boost slyly. quickly special waters boost carefully ironi"
8968.42	"Supplier#000010000       "	"ROMANIA                  "	119999	"Manufacturer#5           "	"aTGLEusCiL4F PDBdv665XBJhPyCOB0i"	"29-578-432-2146"	"ly regular foxes boost slyly. quickly special waters boost carefully ironi"
8936.82	"Supplier#000007043       "	"UNITED KINGDOM           "	109512	"Manufacturer#1           "	"FVajceZInZdbJE6Z9XsRUxrUEpiwHDrOXi,1Rz"	"33-784-177-8208"	"efully regular courts. furiousl"
8936.82	"Supplier#000007043       "	"UNITED KINGDOM           "	109512	"Manufacturer#1           "	"FVajceZInZdbJE6Z9XsRUxrUEpiwHDrOXi,1Rz"	"33-784-177-8208"	"efully regular courts. furiousl"
8929.42	"Supplier#000008770       "	"FRANCE                   "	173735	"Manufacturer#4           "	"R7cG26TtXrHAP9 HckhfRi"	"16-242-746-9248"	"cajole furiously unusual requests. quickly stealthy requests are. "
8929.42	"Supplier#000008770       "	"FRANCE                   "	173735	"Manufacturer#4           "	"R7cG26TtXrHAP9 HckhfRi"	"16-242-746-9248"	"cajole furiously unusual requests. quickly stealthy requests are. "
Fix the query."""

next_shot1 = """
You formulated the following query, which is giving different result:

SELECT s.s_sarabharajudarakhata AS s_acctbal, 
       s.s_sarabharajudaranama AS s_name, 
       r.r_rashtranama AS n_name, 
       v.v_vastukramank AS p_partkey, 
       v.vastubranda AS p_mfgr, 
       s.s_sarabharajudarathikana AS s_address, 
       s.s_sarabharajudaravyavahari AS s_phone, 
       s.s_sarabharajudaramaahiti AS s_comment
FROM Rashtra AS r
JOIN Supplier AS s ON r.r_rashtrakramank = s.s_rashtrakramank
JOIN Suppliervastu AS sv ON sv.sv_sarabharajudarakramank = s.s_sarabharajudarakramank
JOIN Vastuvivara AS v ON sv.sv_vastukramank = v.v_vastukramank
JOIN Pradesh AS p ON v.v_pradeshakramank = p.p_pradeshakramank
WHERE v.v_vastupaddhati = '15'
  AND v.v_vastunama LIKE '%BRASS'
  AND sv.sv_vastubelav = (
    SELECT MIN(sv2.sv_vastubelav)
    FROM Suppliervastu AS sv2
    JOIN Supplier AS s2 ON sv2.sv_sarabharajudarakramank = s2.s_sarabharajudarakramank
    WHERE sv2.sv_vastukramank = v.v_vastukramank
      AND s2.s_rashtrakramank = r.r_rashtrakramank
  )
ORDER BY s.s_sarabharajudarakhata DESC
LIMIT 100;

Fix the query.

You formulated the following query, which is giving different result:

SELECT s.s_sarabharajudarakhata AS s_acctbal, 
       s.s_sarabharajudaranama AS s_name, 
       r.r_rashtranama AS n_name, 
       v.v_vastukramank AS p_partkey, 
       v.vastubranda AS p_mfgr, 
       s.s_sarabharajudarathikana AS s_address, 
       s.s_sarabharajudaravyavahari AS s_phone, 
       s.s_sarabharajudaramaahiti AS s_comment
FROM Rashtra AS r
JOIN Supplier AS s ON r.r_rashtrakramank = s.s_rashtrakramank
JOIN Suppliervastu AS sv ON sv.sv_sarabharajudarakramank = s.s_sarabharajudarakramank
JOIN Vastuvivara AS v ON sv.sv_vastukramank = v.v_vastukramank
JOIN Pradesh AS p ON v.v_pradeshakramank = p.p_pradeshakramank
WHERE v.v_vastupaddhati = '15'
  AND v.v_vastunama LIKE '%BRASS'
  AND sv.sv_vastubelav = (
    SELECT MIN(sv2.sv_vastubelav)
    FROM Suppliervastu AS sv2
    JOIN Supplier AS s2 ON sv2.sv_sarabharajudarakramank = s2.s_sarabharajudarakramank
    WHERE sv2.sv_vastukramank = v.v_vastukramank
      AND s2.s_rashtrakramank = r.r_rashtrakramank and v.v_vastupaddhati = '15'
  AND v.v_vastunama LIKE '%BRASS'
  )
ORDER BY s.s_sarabharajudarakhata DESC
LIMIT 100;

Fix the query.

You formulated the following query, which is giving different result:

SELECT s.s_sarabharajudarakhata AS s_acctbal, 
       s.s_sarabharajudaranama AS s_name, 
       r.r_rashtranama AS n_name, 
       v.v_vastukramank AS p_partkey, 
       v.vastubranda AS p_mfgr, 
       s.s_sarabharajudarathikana AS s_address, 
       s.s_sarabharajudaravyavahari AS s_phone, 
       s.s_sarabharajudaramaahiti AS s_comment
FROM Rashtra AS r
JOIN Supplier AS s ON r.r_rashtrakramank = s.s_rashtrakramank
JOIN Suppliervastu AS sv ON sv.sv_sarabharajudarakramank = s.s_sarabharajudarakramank
JOIN Vastuvivara AS v ON sv.sv_vastukramank = v.v_vastukramank
JOIN Pradesh AS p ON v.v_pradeshakramank = p.p_pradeshakramank
WHERE v.v_vastupaddhati = '15'
  AND v.v_vastunama LIKE '%BRASS'
  AND sv.sv_vastubelav = (
    SELECT MIN(sv2.sv_vastubelav)
    FROM Suppliervastu AS sv2
    JOIN Supplier AS s2 ON sv2.sv_sarabharajudarakramank = s2.s_sarabharajudarakramank
    JOIN Vastuvivara AS v2 ON sv2.sv_vastukramank = v2.v_vastukramank
    WHERE v2.v_vastupaddhati = '15'
      AND v2.v_vastunama LIKE '%BRASS'
      AND s2.s_rashtrakramank = r.r_rashtrakramank
      AND sv2.sv_vastukramank = v.v_vastukramank
  )
ORDER BY s.s_sarabharajudarakhata DESC
LIMIT 100;
Fix the query.

You formulated the following query, which is giving different result:

SELECT s.s_sarabharajudarakhata AS s_acctbal, 
       s.s_sarabharajudaranama AS s_name, 
       r.r_rashtranama AS n_name, 
       v.v_vastukramank AS p_partkey, 
       v.vastubranda AS p_mfgr, 
       s.s_sarabharajudarathikana AS s_address, 
       s.s_sarabharajudaravyavahari AS s_phone, 
       s.s_sarabharajudaramaahiti AS s_comment
FROM Rashtra AS r
JOIN Supplier AS s ON r.r_rashtrakramank = s.s_rashtrakramank
JOIN Suppliervastu AS sv ON sv.sv_sarabharajudarakramank = s.s_sarabharajudarakramank
JOIN Vastuvivara AS v ON sv.sv_vastukramank = v.v_vastukramank
JOIN Pradesh AS p ON v.v_pradeshakramank = p.p_pradeshakramank
WHERE v.v_vastupaddhati = '15'
  AND v.v_vastunama LIKE '%BRASS'
  AND r.r_rashtranama = 'EUROPE'
  AND sv.sv_vastubelav = (
    SELECT MIN(sv2.sv_vastubelav)
    FROM Suppliervastu AS sv2
    JOIN Supplier AS s2 ON sv2.sv_sarabharajudarakramank = s2.s_sarabharajudarakramank
    JOIN Vastuvivara AS v2 ON sv2.sv_vastukramank = v2.v_vastukramank
    WHERE v2.v_vastupaddhati = '15'
      AND v2.v_vastunama LIKE '%BRASS'
      AND s2.s_rashtrakramank = r.r_rashtrakramank
      AND sv2.sv_vastukramank = v.v_vastukramank
  )
ORDER BY s.s_sarabharajudarakhata DESC
LIMIT 100;
Fix the query.

SELECT s.s_sarabharajudarakhata AS s_acctbal, 
       s.s_sarabharajudaranama AS s_name, 
       r.r_rashtranama AS n_name, 
       v.v_vastukramank AS p_partkey, 
       v.vastubranda AS p_mfgr, 
       s.s_sarabharajudarathikana AS s_address, 
       s.s_sarabharajudaravyavahari AS s_phone, 
       s.s_sarabharajudaramaahiti AS s_comment
FROM Rashtra AS r
JOIN Supplier AS s ON r.r_rashtrakramank = s.s_rashtrakramank
JOIN Suppliervastu AS sv ON sv.sv_sarabharajudarakramank = s.s_sarabharajudarakramank
JOIN Vastuvivara AS v ON sv.sv_vastukramank = v.v_vastukramank
JOIN Pradesh AS p ON v.v_pradeshakramank = p.p_pradeshakramank
WHERE v.v_vastupaddhati = '15'
  AND v.v_vastunama LIKE '%BRASS'
  AND r.r_rashtranama = 'EUROPE'
  AND sv.sv_vastubelav = (
    SELECT MIN(sv2.sv_vastubelav)
    FROM Suppliervastu AS sv2
    JOIN Supplier AS s2 ON sv2.sv_sarabharajudarakramank = s2.s_sarabharajudarakramank
    WHERE sv2.sv_vastukramank = v.v_vastukramank
      AND s2.s_rashtrakramank = r.r_rashtrakramank
  )
ORDER BY s.s_sarabharajudarakhata DESC
LIMIT 100;
fix the query.

SELECT s.s_sarabharajudarakhata AS s_acctbal, 
       s.s_sarabharajudaranama AS s_name, 
       r.r_rashtranama AS n_name, 
       v.v_vastukramank AS p_partkey, 
       v.v_vastubranda AS p_mfgr, 
       s.s_sarabharajudarathikana AS s_address, 
       s.s_sarabharajudaravyavahari AS s_phone, 
       s.s_sarabharajudaramaahiti AS s_comment
FROM Rashtra AS r
JOIN Supplier AS s ON r.r_rashtrakramank = s.s_rashtrakramank
JOIN Suppliervastu AS sv ON sv.sv_sarabharajudarakramank = s.s_sarabharajudarakramank
JOIN Vastuvivara AS v ON sv.sv_vastukramank = v.v_vastukramank
JOIN Pradesh AS p ON v.v_pradeshakramank = p.p_pradeshakramank
WHERE v.v_vastupaddhati = '15'
  AND v.v_vastunama LIKE '%BRASS'
  AND r.r_rashtranama = 'EUROPE'
  AND sv.sv_vastubelav = (
    SELECT MIN(sv2.sv_vastubelav)
    FROM Suppliervastu AS sv2
    JOIN Supplier AS s2 ON sv2.sv_sarabharajudarakramank = s2.s_sarabharajudarakramank
    WHERE sv2.sv_vastukramank = v.v_vastukramank
      AND s2.s_rashtrakramank = r.r_rashtrakramank
      AND v.v_vastupaddhati = '15'
      AND v.v_vastunama LIKE '%BRASS'
  )
ORDER BY s.s_sarabharajudarakhata DESC
LIMIT 100;
fix the query.

"""


def count_tokens(text):
    encoding = tiktoken.encoding_for_model("gpt-4o")
    tokens = encoding.encode(text)
    return len(tokens)


def one_round():
    text = f"{text_2_sql_prompt}\n{next_prompt}\n {next_shot}"
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
