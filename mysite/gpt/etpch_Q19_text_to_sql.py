import sys

import tiktoken
from openai import OpenAI

# gets API Key from environment variable OPENAI_API_KEY
client = OpenAI()

text_2_sql_prompt = """Give me SQL for the following text: 
The query finds the gross discounted revenue for all online biddings for three different types of traps that were shipped by air and delivered in person. Traps are selected based on the combination of specific brands, a list of containers, and a range of sizes. Consider the following schema while formulating the SQL: CREATE TABLE TRAP ( T_TRAPKEY SERIAL, T_NAME VARCHAR(55), T_MFGR CHAR(25), T_BRAND CHAR(10), T_TYPE VARCHAR(25), T_SIZE INTEGER, T_CONTAINER CHAR(10), T_RETAILPRICE DECIMAL, T_COMMENT VARCHAR(23) ); CREATE TABLE SELLER ( SE_SELKEY SERIAL, SE_NAME CHAR(25), SE_ADDRESS VARCHAR(40), SE_DESHKEY INTEGER NOT NULL, -- references D_DESHKEY SE_PHONE CHAR(15), SE_ACCTBAL DECIMAL, SE_COMMENT VARCHAR(101) ); CREATE TABLE TRAPSEL ( PSE_TRAPKEY INTEGER NOT NULL, -- references T_TRAPKEY PSE_SELKEY INTEGER NOT NULL, -- references SE_SELKEY PSE_AVAILQTY INTEGER, PSE_SELLYCOST DECIMAL, PSE_COMMENT VARCHAR(199) ); CREATE TABLE KRETA ( K_CUSTKEY SERIAL, K_NAME VARCHAR(25), K_ADDRESS VARCHAR(40), K_DESHKEY INTEGER NOT NULL, -- references D_DESHKEY K_PHONE CHAR(15), K_ACCTBAL DECIMAL, K_TYPE CHAR(10), K_COMMENT VARCHAR(117) ); CREATE TABLE BIDDINGS ( BI_HUKEY SERIAL, BI_CUSTKEY INTEGER NOT NULL, -- references K_CUSTKEY BI_BIDDINGSTATUS CHAR(1), BI_TOTALPRICE DECIMAL, BI_HUDATE DATE, BI_HUPRIORITY CHAR(15), BI_CLERK CHAR(15), BI_SHIPPRIORITY INTEGER, BI_COMMENT VARCHAR(79) ); CREATE TABLE SAMAN ( S_HUKEY INTEGER NOT NULL, -- references BI_HUKEY S_TRAPKEY INTEGER NOT NULL, -- references T_TRAPKEY (compound fk to TRAPSEL) S_SELKEY INTEGER NOT NULL, -- references SE_SELKEY (compound fk to TRAPSEL) S_SAMNUMBER INTEGER, S_QUANTITY DECIMAL, S_EXTENDEDPRICE DECIMAL, S_DISCOUNT DECIMAL, S_TAX DECIMAL, S_RETURNFLAG CHAR(1), S_SAMSTATUS CHAR(1), S_SHIPDATE DATE, S_COMMITDATE DATE, S_RECEIPTDATE DATE, S_SHIPINSTRUCT CHAR(25), S_SHIPMODE CHAR(10), S_COMMENT VARCHAR(44) ); CREATE TABLE DESH ( D_DESHKEY SERIAL, D_NAME CHAR(25), D_MOHADESHKEY INTEGER NOT NULL, -- references MBI_MOHADESHKEY D_COMMENT VARCHAR(152) ); CREATE TABLE MOHADESH ( MBI_MOHADESHKEY SERIAL, MBI_NAME CHAR(25), MBI_COMMENT VARCHAR(152) );
"""

next_shot="""

Following query is a hint query, which is incorrect. But the attributes and tables and predicates present in the query are correct. Refine the query. Consider the possibility of having nested query. Select sum(s_extendedprice*(1 - s_discount)) as revenue From saman, trap Where saman.s_trapkey = trap.t_trapkey and (saman.s_quantity between 1.00 and 11.00 OR saman.s_quantity between 10.00 and 20.00 OR saman.s_quantity between 20.00 and 30.00) and trap.t_brand IN ('Brand#12', 'Brand#23', 'Brand#34') and trap.t_container IN ('LG BOX', 'LG CASE', 'LG PACK', 'LG PKG', 'MED BAG', 'MED BOX', 'MED PACK', 'MED PKG', 'SM BOX', 'SM CASE', 'SM PACK', 'SM PKG') and (trap.t_size between 1 and 5 or trap.t_size between 1 and 10 OR trap.t_size between 1 and 15) and saman.s_shipinstruct = 'DELIVER IN PERSON' and saman.s_shipmode = 'AIR';

SELECT 
    T.T_TYPE AS trap_type,
    SUM(S.S_EXTENDEDPRICE * (1 - S.S_DISCOUNT)) AS gross_discounted_revenue
FROM 
    TRAP T
JOIN 
    SAMAN S ON T.T_TRAPKEY = S.S_TRAPKEY
WHERE 
    T.T_TYPE IN ('Type1', 'Type2', 'Type3') -- Replace with actual types
    AND T.T_BRAND IN ('Brand#12', 'Brand#23', 'Brand#34') -- Replace with actual brands
    AND T.T_CONTAINER IN ('LG BOX', 'LG CASE', 'LG PACK', 'LG PKG', 
                          'MED BAG', 'MED BOX', 'MED PACK', 'MED PKG', 
                          'SM BOX', 'SM CASE', 'SM PACK', 'SM PKG') -- Specific containers
    AND (
        T.T_SIZE BETWEEN 1 AND 5 
        OR T.T_SIZE BETWEEN 1 AND 10 
        OR T.T_SIZE BETWEEN 1 AND 15
    ) -- Specific size ranges
    AND (
        S.S_QUANTITY BETWEEN 1.00 AND 11.00 
        OR S.S_QUANTITY BETWEEN 10.00 AND 20.00 
        OR S.S_QUANTITY BETWEEN 20.00 AND 30.00
    ) -- Specific quantity ranges
    AND S.S_SHIPINSTRUCT = 'DELIVER IN PERSON'
    AND S.S_SHIPMODE = 'AIR'
GROUP BY 
    T.T_TYPE
ORDER BY 
    gross_discounted_revenue DESC;
"""



shot_2="""
actual output: 1530886.9627, But the query is giving output: 14234349.7563. Fix the query.
SELECT 
    SUM(S.S_EXTENDEDPRICE * (1 - S.S_DISCOUNT)) AS gross_discounted_revenue
FROM 
    SAMAN S
JOIN 
    TRAP T ON S.S_TRAPKEY = T.T_TRAPKEY
WHERE 
    T.T_BRAND IN ('Brand#12', 'Brand#23', 'Brand#34')
    AND T.T_CONTAINER IN (
        'LG BOX', 'LG CASE', 'LG PACK', 'LG PKG', 
        'MED BAG', 'MED BOX', 'MED PACK', 'MED PKG', 
        'SM BOX', 'SM CASE', 'SM PACK', 'SM PKG'
    )
    AND (
        T.T_SIZE BETWEEN 1 AND 5 
        OR T.T_SIZE BETWEEN 1 AND 10 
        OR T.T_SIZE BETWEEN 1 AND 15
    )
    AND (
        S.S_QUANTITY BETWEEN 1.00 AND 11.00 
        OR S.S_QUANTITY BETWEEN 10.00 AND 20.00 
        OR S.S_QUANTITY BETWEEN 20.00 AND 30.00
    )
    AND S.S_SHIPINSTRUCT = 'DELIVER IN PERSON'
    AND S.S_SHIPMODE = 'AIR'
    AND S.S_EXTENDEDPRICE IS NOT NULL
    AND S.S_DISCOUNT IS NOT NULL;
"""

shot_3 = """

"Three different types" text is not mapped to any element of the query from the text: The query finds the gross discounted revenue for all biddings for three different types of traps that were shipped by air and delivered in person. Traps are selected based on the combination of specific brands, a list of containers, and a range of sizes.. Fix this.
"""

shot_4 = """
This will cause 3-row result. I want 1 row 1 column result. t.t_type does not have a filter in the query. Do not use such bogus predicates.
"""
shot_5="""
The following is a grouping amond the predicates. () denotes an earlier exact predicate in the list. Can you revise the predicates of the query based on the following groupings: [ [('saman', 's_quantity', 'range', 10.0, 20.0), ('saman', 's_shipinstruct', 'equal', 'DELIVER IN PERSON', 'DELIVER IN PERSON'), ('saman', 's_shipmode', 'equal', 'AIR', 'AIR'), ('trap', 't_brand', 'equal', 'Brand#23', 'Brand#23'), ('trap','t_container', 'equal', 'MED BOX', 'MED BOX'), ('trap', 't_size', 'range', 1, 10)], [('saman', 's_quantity', 'range', 20.0, 30.0), ('saman', 's_shipinstruct', 'equal', 'DELIVER IN PERSON', 'DELIVER IN PERSON'), ('saman', 's_shipmode', 'equal', 'AIR', 'AIR'), ('trap', 't_brand', 'equal', 'Brand#34', 'Brand#34'), ('trap', 't_container', 'equal', 'MED PACK', 'MED PACK'), ('trap', 't_size', 'range', 1, 15)], [('saman', 's_quantity', 'range', 1.0, 11.0), ('saman', 's_shipinstruct', 'equal', 'DELIVER IN PERSON', 'DELIVER IN PERSON'), ('saman', 's_shipmode', 'equal', 'AIR', 'AIR'), ('trap', 't_brand', 'equal', 'Brand#12', 'Brand#12'), ('trap', 't_container', 'equal', 'LG PKG', 'LG PKG'), ('trap', 't_size', 'range', 1, 5)], [(), (), (), (), ('trap', 't_container', 'equal', 'LG PACK', 'LG PACK'), ()], [(), (), (), (), ('trap', 't_container', 'equal', 'SM CASE', 'SM CASE'), ()], [(), (), (), (), ('trap', 't_container', 'equal', 'SM PACK', 'SM PACK'), ()], [(), (), (), (), ('trap', 't_container', 'equal', 'MED PKG', 'MED PKG'), ()], [(), (), (), (), ('trap', 't_container', 'equal', 'MED BAG', 'MED BAG'), ()], [(), (), (), (), ('trap', 't_container', 'equal', 'LG BOX', 'LG BOX'), ()], [(), (), (), (), ('trap', 't_container', 'equal', 'LG CASE', 'LG CASE'), ()], [(), (), (), (), ('trap', 't_container', 'equal', 'SM PKG', 'SM PKG'), ()], [(), (), (), (), ('trap', 't_container', 'equal', 'SM BOX', 'SM BOX'), ()] ]
"""
shot_6="""
Put all similar categoried containers into the same group.
"""

def count_tokens(text):
    encoding = tiktoken.encoding_for_model("gpt-4o")
    tokens = encoding.encode(text)
    return len(tokens)


def one_round():
    text = f"{text_2_sql_prompt}"
    c_token = count_tokens(text) + 50
    print(f"\nToken count = {c_token}\n")
    text = f"{text_2_sql_prompt}\n{next_shot}"
    c_token = count_tokens(text) + 50
    print(f"\nToken count = {c_token}\n")
    text = f"{text_2_sql_prompt}\n{next_shot}\n{shot_2}"
    c_token = count_tokens(text) + 50
    print(f"\nToken count = {c_token}\n")
    text = f"{text_2_sql_prompt}\n{next_shot}\n{shot_2}\n{shot_3}"
    c_token = count_tokens(text) + 50
    print(f"\nToken count = {c_token}\n")
    text = f"{text_2_sql_prompt}\n{next_shot}\n{shot_2}\n{shot_3}\n{shot_4}"
    c_token = count_tokens(text) + 50
    print(f"\nToken count = {c_token}\n")
    text = f"{text_2_sql_prompt}\n{next_shot}\n{shot_2}\n{shot_3}\n{shot_4}\n{shot_5}"
    c_token = count_tokens(text) + 50
    print(f"\nToken count = {c_token}\n")
    text = f"{text_2_sql_prompt}\n{next_shot}\n{shot_2}\n{shot_3}\n{shot_4}\n{shot_5}\n{shot_6}"
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