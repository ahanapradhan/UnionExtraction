PART --> TUKDA
NATION --> PLACE
REGION --> AREA
SUPPLIER --> DIMITRI
MKTSEGMENT --> SECTOR
SUPP --> DIM
LINEITEM --> Chees
CUSTOMER --> INTERESTED
CUST --> intrst
ORDERS --> ADESH
ORDER --> HU

Original Query:
select
        supp_nation,
        cust_nation,
        l_year,
        sum(volume) as revenue
from
        (
                select
                        n1.n_name as supp_nation,
                        n2.n_name as cust_nation,
                        extract(year from l_shipdate) as l_year,
                        l_extendedprice * (1 - l_discount) as volume
                from
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation n2
                where
                        s_suppkey = l_suppkey
                        and o_orderkey = l_orderkey
                        and c_custkey = o_custkey
                        and s_nationkey = n1.n_nationkey
                        and c_nationkey = n2.n_nationkey
                        and (
                                (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
                                or (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                        )
                        and l_shipdate between date '1995-01-01' and date '1996-12-31'
        ) as shipping
group by
        supp_nation,
        cust_nation,
        l_year
order by
        supp_nation,
        cust_nation,
        l_year;

Give me SQL for the following text:

The Query finds, for two given places 'GERMANY' and 'FRANCE',
the gross discounted revenues derived from Cheess in
which TUKDAs were shipped from a dimitri in either place to an interested
in the other place during 1995 and 1996.
The query lists the dimitri place, the interested place,
the year, and the revenue from shipments that took place in
that year. The query orders the answer by dimitri place, interested place, and year (all ascending).

Consider the following schema while formulating the SQL:


CREATE TABLE place
(
    p_placekey  INTEGER not null,
    p_name       CHAR(25) not null,
    p_areakey  INTEGER not null,
    p_comment    VARCHAR(152)
);

CREATE TABLE area
(
    a_areakey  INTEGER not null,
    a_name       CHAR(25) not null,
    a_comment    VARCHAR(152)
);

CREATE TABLE TUKDA
(
    t_TUKDAkey     BIGINT not null,
    t_name        VARCHAR(55) not null,
    t_mfgr        CHAR(25) not null,
    t_brand       CHAR(10) not null,
    t_type        VARCHAR(25) not null,
    t_size        INTEGER not null,
    t_container   CHAR(10) not null,
    t_retailprice DOUBLE PRECISION not null,
    t_comment     VARCHAR(23) not null
);

CREATE TABLE dimitri
(
    d_dimkey     BIGINT not null,
    d_name        CHAR(25) not null,
    d_address     VARCHAR(40) not null,
    d_placekey   INTEGER not null,
    d_phone       CHAR(15) not null,
    d_acctbal     DOUBLE PRECISION not null,
    d_comment     VARCHAR(101) not null
);

CREATE TABLE TUKDAdim
(
    ps_TUKDAkey     BIGINT not null,
    ps_dimkey     BIGINT not null,
    ps_availqty    BIGINT not null,
    ps_dimlycost  DOUBLE PRECISION  not null,
    ps_comment     VARCHAR(199) not null
);

CREATE TABLE interested
(
    i_intrstkey     BIGINT not null,
    i_name        VARCHAR(25) not null,
    i_address     VARCHAR(40) not null,
    i_placekey   INTEGER not null,
    i_phone       CHAR(15) not null,
    i_acctbal     DOUBLE PRECISION   not null,
    i_SECTOR  CHAR(10) not null,
    i_comment     VARCHAR(117) not null
);

CREATE TABLE adesh
(
    o_adeshkey       BIGINT not null,
    o_intrstkey        BIGINT not null,
    o_adeshtatus    CHAR(1) not null,
    o_totalprice     DOUBLE PRECISION not null,
    o_adeshdate      DATE not null,
    o_adeshpriority  CHAR(15) not null,
    o_clerk          CHAR(15) not null,
    o_shippriority   INTEGER not null,
    o_comment        VARCHAR(79) not null
);

CREATE TABLE Chees
(
    Ch_adeshkey    BIGINT not null,
    Ch_TUKDAkey     BIGINT not null,
    Ch_dimkey     BIGINT not null,
    Ch_linenumber  BIGINT not null,
    Ch_quantity    DOUBLE PRECISION not null,
    Ch_extendedprice  DOUBLE PRECISION not null,
    Ch_discount    DOUBLE PRECISION not null,
    Ch_tax         DOUBLE PRECISION not null,
    Ch_returnflag  CHAR(1) not null,
    Ch_linestatus  CHAR(1) not null,
    Ch_shipdate    DATE not null,
    Ch_commitdate  DATE not null,
    Ch_receiptdate DATE not null,
    Ch_shipinstruct CHAR(25) not null,
    Ch_shipmode     CHAR(10) not null,
    Ch_comment      VARCHAR(44) not null
);
Hint:
Place table is used twice in the query

-- one more prompt regarding result mismatch caused using l_shipdate instead of initial o_orderdate,
--which resulted in the correct output

SELECT 
    d_place.n_name AS dimitri_place,
    i_place.n_name AS interested_place,
    EXTRACT(YEAR FROM c.l_shipdate) AS year,
    SUM(c.l_extendedprice * (1 - c.l_discount)) AS revenue
FROM
    lineitem c
JOIN orders a ON c.l_orderkey = a.o_orderkey
JOIN customer i ON a.o_custkey = i.c_custkey
JOIN nation i_place ON i.c_nationkey = i_place.n_nationkey
JOIN supplier d ON c.l_suppkey = d.s_suppkey
JOIN nation d_place ON d.s_nationkey = d_place.n_nationkey
WHERE
    d_place.n_name IN ('GERMANY', 'FRANCE')
    AND i_place.n_name IN ('GERMANY', 'FRANCE')
    AND d_place.n_name <> i_place.n_name
    AND c.l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
GROUP BY
    d_place.n_name, i_place.n_name, EXTRACT(YEAR FROM c.l_shipdate)
ORDER BY 
    d_place.n_name ASC, 
    i_place.n_name ASC, 
    year ASC;