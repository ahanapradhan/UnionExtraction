import sys

import tiktoken
from openai import OpenAI

# gets API Key from environment variable OPENAI_API_KEY
client = OpenAI()

text_2_sql_prompt = """
Give me SQL for the following text: The market share for DESH 'INDIA' within MOHADESH 'ASIA' is defined as the fraction of the revenue, the sum of [j_extendedprice * (1-j_discount)], from the products of 'ECONOMY ANODIZED STEEL' type in that MOHADESH that was supplied by BIKRETAS from 'INDIA' against online orders. The query determines this for the years 1995 and 1996 presented in this order. Consider the following schema while formulating the SQL: CREATE TABLE VAG ( V_VAGKEY SERIAL, V_NAME VARCHAR(55), V_MFGR CHAR(25), V_BRAND CHAR(10), V_TYPE VARCHAR(25), V_SIZE INTEGER, V_CONTAINER CHAR(10), V_RETAILPRICE DECIMAL, V_COMMENT VARCHAR(23) ); CREATE TABLE BIKRETA ( B_BIKKEY SERIAL, B_NAME CHAR(25), B_ADDRESS VARCHAR(40), B_DESHKEY INTEGER NOT NULL, -- references D_DESHKEY B_PHONE CHAR(15), B_ACCTBAL DECIMAL, B_COMMENT VARCHAR(101) ); CREATE TABLE VAGBIK ( PB_VAGKEY INTEGER NOT NULL, -- references V_VAGKEY PB_BIKKEY INTEGER NOT NULL, -- references B_BIKKEY PB_AVAILQTY INTEGER, PB_BIKLYCOST DECIMAL, PB_COMMENT VARCHAR(199) ); CREATE TABLE KRETA ( K_CUSTKEY SERIAL, K_NAME VARCHAR(25), K_ADDRESS VARCHAR(40), K_DESHKEY INTEGER NOT NULL, -- references D_DESHKEY K_PHONE CHAR(15), K_ACCTBAL DECIMAL, K_PROKAR CHAR(10), K_COMMENT VARCHAR(117) ); CREATE TABLE HUKUM ( H_HUKEY SERIAL, H_CUSTKEY INTEGER NOT NULL, -- references K_CUSTKEY H_HUKUMTATUS CHAR(1), H_TOTALPRICE DECIMAL, H_HUDATE DATE, H_HUPRIORITY CHAR(15), H_CLERK CHAR(15), H_SHIPPRIORITY INTEGER, H_COMMENT VARCHAR(79) ); CREATE TABLE JINIS ( J_HUKEY INTEGER NOT NULL, -- references H_HUKEY J_VAGKEY INTEGER NOT NULL, -- references V_VAGKEY (compound fk to VAGBIK) J_BIKKEY INTEGER NOT NULL, -- references B_BIKKEY (compound fk to VAGBIK) J_LINENUMBER INTEGER, J_QUANTITY DECIMAL, J_EXTENDEDPRICE DECIMAL, J_DISCOUNT DECIMAL, J_TAX DECIMAL, J_RETURNFLAG CHAR(1), J_LINESTATUS CHAR(1), J_SHIPDATE DATE, J_COMMITDATE DATE, J_RECEIPTDATE DATE, J_SHIPINSTRUCT CHAR(25), J_SHIPMODE CHAR(10), J_COMMENT VARCHAR(44) ); CREATE TABLE DESH ( D_DESHKEY SERIAL, D_NAME CHAR(25), D_MOHADESHKEY INTEGER NOT NULL, -- references MH_MOHADESHKEY D_COMMENT VARCHAR(152) ); CREATE TABLE MOHADESH ( MH_MOHADESHKEY SERIAL, MH_NAME CHAR(25), MH_COMMENT VARCHAR(152) ); 
Hint: All the tables in the schema are used, except VAGBIK. Table DESH is used in this query more than once, the other tables are used only once.
"""
shot_2 = """
SELECT 
    EXTRACT(YEAR FROM J.J_SHIPDATE) AS YEAR,
    SUM(J.J_EXTENDEDPRICE * (1 - J.J_DISCOUNT)) / 
    (SELECT SUM(J2.J_EXTENDEDPRICE * (1 - J2.J_DISCOUNT))
     FROM JINIS J2
     JOIN HUKUM H2 ON J2.J_HUKEY = H2.H_HUKEY
     JOIN KRETA K2 ON H2.H_CUSTKEY = K2.K_CUSTKEY
     JOIN DESH D2 ON K2.K_DESHKEY = D2.D_DESHKEY
     JOIN MOHADESH MH2 ON D2.D_MOHADESHKEY = MH2.MH_MOHADESHKEY
     JOIN VAG V2 ON J2.J_VAGKEY = V2.V_VAGKEY
     WHERE MH2.MH_NAME = 'ASIA'
       AND V2.V_TYPE = 'ECONOMY ANODIZED STEEL'
       AND EXTRACT(YEAR FROM J2.J_SHIPDATE) IN (1995, 1996)
    ) AS MARKET_SHARE
FROM JINIS J
JOIN HUKUM H ON J.J_HUKEY = H.H_HUKEY
JOIN KRETA K ON H.H_CUSTKEY = K.K_CUSTKEY
JOIN DESH D ON K.K_DESHKEY = D.D_DESHKEY
JOIN BIKRETA B ON J.J_BIKKEY = B.B_BIKKEY
JOIN DESH D_BIK ON B.B_DESHKEY = D_BIK.D_DESHKEY
JOIN MOHADESH MH ON D.D_MOHADESHKEY = MH.MH_MOHADESHKEY
JOIN VAG V ON J.J_VAGKEY = V.V_VAGKEY
WHERE MH.MH_NAME = 'ASIA'
  AND D_BIK.D_NAME = 'INDIA'
  AND V.V_TYPE = 'ECONOMY ANODIZED STEEL'
  AND EXTRACT(YEAR FROM J.J_SHIPDATE) IN (1995, 1996)
GROUP BY EXTRACT(YEAR FROM J.J_SHIPDATE)
ORDER BY YEAR;
This query is taking too long to evaluate. Simplify the joins and the calculations.

"""
shot_3 = """
SELECT 
    EXTRACT(YEAR FROM J.J_SHIPDATE) AS YEAR,
    SUM(J.J_EXTENDEDPRICE * (1 - J.J_DISCOUNT)) AS INDIA_REVENUE,
    SUM(SUM(J.J_EXTENDEDPRICE * (1 - J.J_DISCOUNT))) OVER (PARTITION BY EXTRACT(YEAR FROM J.J_SHIPDATE)) AS TOTAL_ASIA_REVENUE,
    SUM(J.J_EXTENDEDPRICE * (1 - J.J_DISCOUNT)) / 
    SUM(SUM(J.J_EXTENDEDPRICE * (1 - J.J_DISCOUNT))) OVER (PARTITION BY EXTRACT(YEAR FROM J.J_SHIPDATE)) AS MARKET_SHARE
FROM JINIS J
JOIN HUKUM H ON J.J_HUKEY = H.H_HUKEY
JOIN KRETA K ON H.H_CUSTKEY = K.K_CUSTKEY
JOIN DESH DK ON K.K_DESHKEY = DK.D_DESHKEY
JOIN BIKRETA B ON J.J_BIKKEY = B.B_BIKKEY
JOIN DESH DB ON B.B_DESHKEY = DB.D_DESHKEY
JOIN MOHADESH MH ON DK.D_MOHADESHKEY = MH.MH_MOHADESHKEY
JOIN VAG V ON J.J_VAGKEY = V.V_VAGKEY
WHERE MH.MH_NAME = 'ASIA'
  AND V.V_TYPE = 'ECONOMY ANODIZED STEEL'
  AND EXTRACT(YEAR FROM J.J_SHIPDATE) IN (1995, 1996)
GROUP BY EXTRACT(YEAR FROM J.J_SHIPDATE)
ORDER BY YEAR;
INDIA keyword is not used in the above query. Fix it.

"""
shot_4 = """
SELECT
    EXTRACT(YEAR FROM J.J_SHIPDATE) AS YEAR,
    SUM(J.J_EXTENDEDPRICE * (1 - J.J_DISCOUNT)) AS INDIA_REVENUE,
    SUM(SUM(J.J_EXTENDEDPRICE * (1 - J.J_DISCOUNT))) 
        OVER (PARTITION BY EXTRACT(YEAR FROM J.J_SHIPDATE)) AS TOTAL_ASIA_REVENUE,
    SUM(J.J_EXTENDEDPRICE * (1 - J.J_DISCOUNT)) / 
    SUM(SUM(J.J_EXTENDEDPRICE * (1 - J.J_DISCOUNT))) 
        OVER (PARTITION BY EXTRACT(YEAR FROM J.J_SHIPDATE)) AS MARKET_SHARE
FROM JINIS J
JOIN HUKUM H ON J.J_HUKEY = H.H_HUKEY
JOIN KRETA K ON H.H_CUSTKEY = K.K_CUSTKEY
JOIN DESH DK ON K.K_DESHKEY = DK.D_DESHKEY
JOIN BIKRETA B ON J.J_BIKKEY = B.B_BIKKEY
JOIN DESH DB ON B.B_DESHKEY = DB.D_DESHKEY
JOIN MOHADESH MH ON DK.D_MOHADESHKEY = MH.MH_MOHADESHKEY
JOIN VAG V ON J.J_VAGKEY = V.V_VAGKEY
WHERE MH.MH_NAME = 'ASIA'
  AND DB.D_NAME = 'INDIA'  -- Filter suppliers from INDIA
  AND V.V_TYPE = 'ECONOMY ANODIZED STEEL'
  AND EXTRACT(YEAR FROM J.J_SHIPDATE) IN (1995, 1996)
GROUP BY EXTRACT(YEAR FROM J.J_SHIPDATE)
ORDER BY YEAR;
The result produced by the above query is not matching.

"""
shot_5 = """
SELECT 
    EXTRACT(YEAR FROM J.J_SHIPDATE) AS YEAR,
    SUM(CASE 
            WHEN DB.D_NAME = 'INDIA' THEN J.J_EXTENDEDPRICE * (1 - J.J_DISCOUNT) 
            ELSE 0 
        END) AS INDIA_REVENUE,
    SUM(J.J_EXTENDEDPRICE * (1 - J.J_DISCOUNT)) AS TOTAL_ASIA_REVENUE,
    SUM(CASE 
            WHEN DB.D_NAME = 'INDIA' THEN J.J_EXTENDEDPRICE * (1 - J.J_DISCOUNT) 
            ELSE 0 
        END) / SUM(J.J_EXTENDEDPRICE * (1 - J.J_DISCOUNT)) AS MARKET_SHARE
FROM JINIS J
JOIN HUKUM H ON J.J_HUKEY = H.H_HUKEY
JOIN KRETA K ON H.H_CUSTKEY = K.K_CUSTKEY
JOIN DESH DK ON K.K_DESHKEY = DK.D_DESHKEY
JOIN BIKRETA B ON J.J_BIKKEY = B.B_BIKKEY
JOIN DESH DB ON B.B_DESHKEY = DB.D_DESHKEY
JOIN MOHADESH MH ON DK.D_MOHADESHKEY = MH.MH_MOHADESHKEY
JOIN VAG V ON J.J_VAGKEY = V.V_VAGKEY
WHERE MH.MH_NAME = 'ASIA'
  AND V.V_TYPE = 'ECONOMY ANODIZED STEEL'
  AND EXTRACT(YEAR FROM J.J_SHIPDATE) IN (1995, 1996)
GROUP BY EXTRACT(YEAR FROM J.J_SHIPDATE)
ORDER BY YEAR;
Why do you have duplicate projections?

remove redundant projections. Output has only 2 projections, year and mkt_share.
"""
shot_6 = """
SELECT 
    EXTRACT(YEAR FROM J.J_SHIPDATE) AS YEAR,
    SUM(
        CASE 
            WHEN DB.D_NAME = 'INDIA' THEN J.J_EXTENDEDPRICE * (1 - J.J_DISCOUNT)
            ELSE 0 
        END
    ) / SUM(J.J_EXTENDEDPRICE * (1 - J.J_DISCOUNT)) AS MARKET_SHARE
FROM JINIS J
JOIN HUKUM H ON J.J_HUKEY = H.H_HUKEY
JOIN KRETA K ON H.H_CUSTKEY = K.K_CUSTKEY
JOIN DESH DK ON K.K_DESHKEY = DK.D_DESHKEY
JOIN BIKRETA B ON J.J_BIKKEY = B.B_BIKKEY
JOIN DESH DB ON B.B_DESHKEY = DB.D_DESHKEY
JOIN MOHADESH MH ON DK.D_MOHADESHKEY = MH.MH_MOHADESHKEY
JOIN VAG V ON J.J_VAGKEY = V.V_VAGKEY
WHERE MH.MH_NAME = 'ASIA'
  AND V.V_TYPE = 'ECONOMY ANODIZED STEEL'
  AND EXTRACT(YEAR FROM J.J_SHIPDATE) IN (1995, 1996)
GROUP BY EXTRACT(YEAR FROM J.J_SHIPDATE)
ORDER BY YEAR;
expected output: o_year, mkt_share: 1995        0.03548762227781919462 1996        0.03570071939890738017, actual output: 1995        0.03493674522524001898 1996        0.03976510262570184479. Fix this.
"""

def count_tokens(text):
    encoding = tiktoken.encoding_for_model("gpt-4o")
    tokens = encoding.encode(text)
    return len(tokens)


def one_round():
    text = f"{text_2_sql_prompt}"
    c_token = count_tokens(text) + 50
    print(f"\nToken count = {c_token}\n")
    text = f"{text_2_sql_prompt}"
    c_token = count_tokens(text) + 50
    print(f"\nToken count = {c_token}\n")
    text = f"{text_2_sql_prompt}\n{shot_2}"
    c_token = count_tokens(text) + 50
    print(f"\nToken count = {c_token}\n")
    text = f"{text_2_sql_prompt}\n{shot_2}\n{shot_3}"
    c_token = count_tokens(text) + 50
    print(f"\nToken count = {c_token}\n")
    text = f"{text_2_sql_prompt}\n{shot_2}\n{shot_3}\n{shot_4}"
    c_token = count_tokens(text) + 50
    print(f"\nToken count = {c_token}\n")
    text = f"{text_2_sql_prompt}\n{shot_2}\n{shot_3}\n{shot_4}\n{shot_5}"
    c_token = count_tokens(text) + 50
    print(f"\nToken count = {c_token}\n")
    text = f"{text_2_sql_prompt}\n{shot_2}\n{shot_3}\n{shot_4}\n{shot_5}\n{shot_6}"
    c_token = count_tokens(text) + 50
    print(f"\nToken count = {c_token}\n")
    """
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
    """


orig_out = sys.stdout
f = open('chatgpt_tpch_sql.py', 'w')
sys.stdout = f
one_round()
sys.stdout = orig_out
f.close()