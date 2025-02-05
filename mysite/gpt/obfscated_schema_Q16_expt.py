import sys
import tiktoken
from openai import OpenAI

# gets API Key from environment variable OPENAI_API_KEY
client = OpenAI()
"""
PART --> ITEM
NATION --> COUNTRY
REGION --> CONTINENT
SUPPLIER --> VENDOR
MKTSEGMENT --> SECTOR
SUPP --> VEN
LINEITEM --> PRODUCT
CUSTOMER --> CLIENT
ORDERS --> REQUIREMENTS
ORDER --> REQ
"""
text_2_sql_prompt = """
Give me SQL for the following text:

The Query counts the number of vendors who can provide items that satisfy a particular
clients's interest.
The client is interested in items of sizes 1, 4, and 7 as long as
they are not medium polished, not of Brand no. 45,
and not from a vendor who has had complaints registered at the Better Business Bureau.
Results must be presented in descending count and ascending brand, type, and size.

Consider the following schema while formulating the SQL:

CREATE TABLE ITEM (

    I_ITEMKEY        BIGINT,
    I_NAME            VARCHAR(55),
    I_MFGR            CHAR(25),
    I_BRAND            CHAR(10),
    I_TYPE            VARCHAR(25),
    I_SIZE            INTEGER,
    I_CONTAINER        CHAR(10),
    I_RETAILPRICE    DECIMAL,
    I_COMMENT        VARCHAR(23)
);

CREATE TABLE VENDOR (
    V_VENKEY        BIGINT,
    V_NAME            CHAR(25),
    V_ADDRESS        VARCHAR(40),
    V_COUNTRYKEY        INTEGER NOT NULL, -- references CR_COUNTRYKEY
    V_PHONE            CHAR(15),
    V_ACCTBAL        DECIMAL,
    V_COMMENT        VARCHAR(101)
);

CREATE TABLE ITEMVEN (
    IV_ITEMKEY        INTEGER NOT NULL, -- references I_ITEMKEY
    IV_VENKEY        INTEGER NOT NULL, -- references V_VENKEY
    IV_AVAILQTY        INTEGER,
    IV_VENLYCOST    DECIMAL,
    IV_COMMENT        VARCHAR(199)
);

CREATE TABLE CLIENT (
    C_CUSTKEY        BIGINT,
    C_NAME            VARCHAR(25),
    C_ADDRESS        VARCHAR(40),
    C_COUNTRYKEY        INTEGER NOT NULL, -- references C_COUNTRYKEY
    C_PHONE            CHAR(15),
    C_ACCTBAL        DECIMAL,
    C_SECTOR    CHAR(10),
    C_COMMENT        VARCHAR(117)
);

CREATE TABLE REQUIREMENTS (
    R_REQKEY        BIGINT,
    R_CUSTKEY        INTEGER NOT NULL, -- references C_CUSTKEY
    R_REQUIREMENTSTATUS    CHAR(1),
    R_TOTALPRICE    DECIMAL,
    R_REQDATE        DATE,
    R_REQPRIORITY    CHAR(15),
    R_CLERK            CHAR(15),
    R_SHIPPRIORITY    INTEGER,
    R_COMMENT        VARCHAR(79)
);

CREATE TABLE online_PRODUCT (
    OP_REQKEY        INTEGER NOT NULL, -- references R_REQKEY
    OP_ITEMKEY        INTEGER NOT NULL, -- references I_ITEMKEY (compound fk to ITEMVEN)
    OP_VENKEY        INTEGER NOT NULL, -- references V_VENKEY (compound fk to ITEMVEN)
    OP_LINENUMBER    INTEGER,
    OP_QUANTITY        DECIMAL,
    OP_EXTENDEDPRICE    DECIMAL,
    OP_DISCOUNT        DECIMAL,
    OP_TAX            DECIMAL,
    OP_RETURNFLAG    CHAR(1),
    OP_LINESTATUS    CHAR(1),
    OP_SHIPDATE        DATE,
    OP_COMMITDATE    DATE,
    OP_RECEIPTDATE    DATE,
    OP_SHIPINSTRUCT    CHAR(25),
    OP_SHIPMODE        CHAR(10),
    OP_COMMENT        VARCHAR(44)
);

CREATE TABLE store_PRODUCT (
    SP_REQKEY        INTEGER NOT NULL, -- references R_REQKEY
    SP_ITEMKEY        INTEGER NOT NULL, -- references I_ITEMKEY (compound fk to ITEMVEN)
    SP_VENKEY        INTEGER NOT NULL, -- references V_VENKEY (compound fk to ITEMVEN)
    SP_LINENUMBER    INTEGER,
    SP_QUANTITY        DECIMAL,
    SP_EXTENDEDPRICE    DECIMAL,
    SP_DISCOUNT        DECIMAL,
    SP_TAX            DECIMAL,
    SP_RETURNFLAG    CHAR(1),
    SP_LINESTATUS    CHAR(1),
    SP_SHIPDATE        DATE,
    SP_COMMITDATE    DATE,
    SP_RECEIPTDATE    DATE,
    SP_SHIPINSTRUCT    CHAR(25),
    SP_SHIPMODE        CHAR(10),
    SP_COMMENT        VARCHAR(44)
);


CREATE TABLE COUNTRY (
    CR_COUNTRYKEY        BIGINT,
    CR_NAME            CHAR(25),
    CR_CONTINENTKEY        INTEGER NOT NULL,  -- references C_CONTINENTKEY
    CR_COMMENT        VARCHAR(152)
);

CREATE TABLE CONTINENT (
    CN_CONTINENTKEY    BIGINT,
    CN_NAME        CHAR(25),
    CN_COMMENT    VARCHAR(152)
);

Mandatory instructions on SQL query formulation:
1. Do not use redundant join conditions.
2. Do not use any predicate with place holder parameter.
3. No attribute in the database has NULL value.
4. ITEM, VENDOR and ITEMVEN tables are used in the query.
"""

"""
Give sample of data to refine the filters.
The following is the best it can do.
--Best by GPT:
SELECT
    i.I_BRAND AS item_brand,
    i.I_TYPE AS item_type,
    i.I_SIZE AS item_size,
    COUNT(DISTINCT v.V_VENKEY) AS vendor_count
FROM
    VENDOR v
JOIN
    ITEMVEN iv ON v.V_VENKEY = iv.IV_VENKEY
JOIN
    ITEM i ON iv.IV_ITEMKEY = i.I_ITEMKEY
WHERE
    i.I_SIZE IN (1, 4, 7) -- Items of sizes 1, 4, and 7
    AND i.I_TYPE NOT LIKE '%MEDIUM POLISHED%' -- Exclude 'Medium Polished' types
    AND i.I_BRAND != 'Brand#45' -- Exclude Brand#45
    AND v.V_COMMENT NOT LIKE '%Complaint%' -- Exclude vendors with 'Complaint'
GROUP BY
    i.I_BRAND, i.I_TYPE, i.I_SIZE
ORDER BY
    vendor_count DESC,
    i.I_BRAND ASC,
    i.I_TYPE ASC,
    i.I_SIZE ASC;
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
