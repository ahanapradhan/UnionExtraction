PART --> VAG
NATION --> DESH
REGION --> MOHADESH
SUPPLIER --> BIKRETA
MKTSEGMENT --> PROKAR
SUPP --> BIK
LINEITEM --> JINIS
CUSTOMER --> KRETA
ORDERS --> HUKUM
ORDER --> HU

Original Query:
select
        o_year,
        sum(case
                when nation = 'INDIA' then volume
                else 0
        end) / sum(volume) as mkt_share
from
        (
                select
                        extract(year from o_orderdate) as o_year,
                        l_extendedprice * (1 - l_discount) as volume,
                        n2.n_name as nation
                from
                        part,
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation n2,
                        region
                where
                        p_partkey = l_partkey
                        and s_suppkey = l_suppkey
                        and l_orderkey = o_orderkey
                        and o_custkey = c_custkey
                        and c_nationkey = n1.n_nationkey
                        and n1.n_regionkey = r_regionkey
                        and r_name = 'ASIA'
                        and s_nationkey = n2.n_nationkey
                        and o_orderdate between date '1995-01-01' and date '1996-12-31'
                        and p_type = 'ECONOMY ANODIZED STEEL'
        ) as all_nations
group by
        o_year
order by
        o_year;

Give me SQL for the following text:

The market share for DESH 'INDIA' within MOHADESH 'ASIA' is defined as the fraction of the revenue,
the sum of [j_extendedprice * (1-j_discount)], from the products of 'ECONOMY ANODIZED STEEL' type in that
MOHADESH that was supplied by BIKRETAS from 'INDIA'. The query determines
this for the years 1995 and 1996 presented in this order.

Consider the following schema while formulating the SQL:

CREATE TABLE VAG (

    V_VAGKEY        SERIAL,
    V_NAME            VARCHAR(55),
    V_MFGR            CHAR(25),
    V_BRAND            CHAR(10),
    V_TYPE            VARCHAR(25),
    V_SIZE            INTEGER,
    V_CONTAINER        CHAR(10),
    V_RETAILPRICE    DECIMAL,
    V_COMMENT        VARCHAR(23)
);

CREATE TABLE BIKRETA (
    B_BIKKEY        SERIAL,
    B_NAME            CHAR(25),
    B_ADDRESS        VARCHAR(40),
    B_DESHKEY        INTEGER NOT NULL, -- references D_DESHKEY
    B_PHONE            CHAR(15),
    B_ACCTBAL        DECIMAL,
    B_COMMENT        VARCHAR(101)
);

CREATE TABLE VAGBIK (
    PB_VAGKEY        INTEGER NOT NULL, -- references V_VAGKEY
    PB_BIKKEY        INTEGER NOT NULL, -- references B_BIKKEY
    PB_AVAILQTY        INTEGER,
    PB_BIKLYCOST    DECIMAL,
    PB_COMMENT        VARCHAR(199)
);

CREATE TABLE KRETA (
    K_CUSTKEY        SERIAL,
    K_NAME            VARCHAR(25),
    K_ADDRESS        VARCHAR(40),
    K_DESHKEY        INTEGER NOT NULL, -- references D_DESHKEY
    K_PHONE            CHAR(15),
    K_ACCTBAL        DECIMAL,
    K_PROKAR    CHAR(10),
    K_COMMENT        VARCHAR(117)
);

CREATE TABLE HUKUM (
    H_HUKEY        SERIAL,
    H_CUSTKEY        INTEGER NOT NULL, -- references K_CUSTKEY
    H_HUKUMTATUS    CHAR(1),
    H_TOTALPRICE    DECIMAL,
    H_HUDATE        DATE,
    H_HUPRIORITY    CHAR(15),
    H_CLERK            CHAR(15),
    H_SHIPPRIORITY    INTEGER,
    H_COMMENT        VARCHAR(79)
);

CREATE TABLE JINIS (
    J_HUKEY        INTEGER NOT NULL, -- references H_HUKEY
    J_VAGKEY        INTEGER NOT NULL, -- references V_VAGKEY (compound fk to VAGBIK)
    J_BIKKEY        INTEGER NOT NULL, -- references B_BIKKEY (compound fk to VAGBIK)
    J_LINENUMBER    INTEGER,
    J_QUANTITY        DECIMAL,
    J_EXTENDEDPRICE    DECIMAL,
    J_DISCOUNT        DECIMAL,
    J_TAX            DECIMAL,
    J_RETURNFLAG    CHAR(1),
    J_LINESTATUS    CHAR(1),
    J_SHIPDATE        DATE,
    J_COMMITDATE    DATE,
    J_RECEIPTDATE    DATE,
    J_SHIPINSTRUCT    CHAR(25),
    J_SHIPMODE        CHAR(10),
    J_COMMENT        VARCHAR(44)
);

CREATE TABLE DESH (
    D_DESHKEY        SERIAL,
    D_NAME            CHAR(25),
    D_MOHADESHKEY        INTEGER NOT NULL,  -- references MH_MOHADESHKEY
    D_COMMENT        VARCHAR(152)
);

CREATE TABLE MOHADESH (
    MH_MOHADESHKEY    SERIAL,
    MH_NAME        CHAR(25),
    MH_COMMENT    VARCHAR(152)
);

Hint:
All the tables in the schema are used, except VAGBIK.
Table DESH is used in this query more than once, the other tables are used only once.



SELECT 
    EXTRACT(YEAR FROM l_shipdate) AS YEAR,
    SUM(
        CASE
            WHEN DB.n_name = 'INDIA' THEN l_EXTENDEDPRICE * (1 - l_DISCOUNT)
            ELSE 0
        END
    ) / SUM(l_EXTENDEDPRICE * (1 - l_DISCOUNT)) AS MARKET_SHARE
FROM lineitem
JOIN orders ON l_orderkey = o_orderkey
JOIN customer K ON o_custkey = K.c_CUSTKEY
JOIN nation DK ON K.c_nationkey = DK.n_nationkey
JOIN supplier B ON l_suppkey = B.s_suppkey
JOIN nation DB ON B.s_nationkey = DB.n_nationkey
JOIN region MH ON DK.n_regionkey = MH.r_regionkey
JOIN part V ON l_partkey = V.p_partkey
WHERE MH.r_name = 'ASIA'
  AND V.p_TYPE = 'ECONOMY ANODIZED STEEL'
  AND EXTRACT(YEAR FROM l_shipdate) IN (1995, 1996)
GROUP BY EXTRACT(YEAR FROM l_shipdate)
ORDER BY YEAR;
