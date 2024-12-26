import sys

from openai import OpenAI

# gets API Key from environment variable OPENAI_API_KEY
client = OpenAI()

text_2_sql_prompt = """Translate the following SQL to use TPCH table and column based english names 
(do not add explainations, just give the SQL. Put the SQL within double quotes):"""


def one_round(query):
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "user",
                "content": f"{text_2_sql_prompt} {query}",
            },
        ], temperature=0, stream=False
    )
    reply = response.choices[0].message.content
    print(reply)


orig_out = sys.stdout
f = open('chatgpt_tpch_sql.py', 'w')
sys.stdout = f
q = """
SELECT s.s_sarabharajudarakhata AS s_acctbal, 
       s.s_sarabharajudaranama AS s_name, 
       r.r_rashtranama AS n_name, 
       v.v_vastukramank AS p_partkey, 
       v.vastubranda AS p_mfgr, 
       s.s_sarabharajudarathikana AS s_address, 
       s.s_sarabharajudaravyavahari AS s_phone, 
       s.s_sarabharajudaramaahiti AS s_comment
FROM Rashtra AS r
JOIN Sarabharajudara AS s ON r.r_rashtrakramank = s.s_rashtrakramank
JOIN Sarabharajudaravastu AS sv ON sv.sv_sarabharajudarakramank = s.s_sarabharajudarakramank
JOIN Vastuvivara AS v ON sv.sv_vastukramank = v.v_vastukramank
JOIN Pradesh AS p ON v.v_pradeshakramank = p.p_pradeshakramank
WHERE v.v_vastupaddhati = '15'
  AND v.v_vastunama LIKE '%BRASS'
  AND sv.sv_vastubelav = (
    SELECT MIN(sv2.sv_vastubelav)
    FROM Sarabharajudaravastu AS sv2
    JOIN Sarabharajudara AS s2 ON sv2.sv_sarabharajudarakramank = s2.s_sarabharajudarakramank
    WHERE sv2.sv_vastukramank = v.v_vastukramank
      AND s2.s_rashtrakramank = r.r_rashtrakramank
  )
ORDER BY s.s_sarabharajudarakhata DESC
LIMIT 100;
"""

one_round(q)
sys.stdout = orig_out
f.close()
