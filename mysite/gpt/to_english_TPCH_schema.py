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
SELECT s.s_sarabharajudaranama
FROM Sarabharajudara s
JOIN Rashtra r ON s.s_rashtrakramank = r.r_rashtrakramank
JOIN Sarabharajudaravastu sv ON s.s_sarabharajudarakramank = sv.sv_sarabharajudarakramank
JOIN Vastuvivara v ON sv.sv_vastukramank = v.v_vastukramank
JOIN Aajnavastu av ON v.v_vastukramank = av.av_vastukramank AND s.s_sarabharajudarakramank = av.av_sarabharajudarakramank
WHERE r.r_rashtranama = 'FRANCE'
  AND v.v_vastunama LIKE '%ivory%'
  AND av.av_vastusankhya > 0.5 * (
    SELECT SUM(av2.av_vastusankhya)
    FROM Aajnavastu av2
    JOIN Aajna a ON av2.av_aajnakramank = a.a_aajnakramank
    WHERE av2.av_sarabharajudarakramank = s.s_sarabharajudarakramank
      AND av2.av_vastukramank = v.v_vastukramank
      AND EXTRACT(YEAR FROM a.a_aajnatharikh) = 1995
  );
"""

one_round(q)
sys.stdout = orig_out
f.close()
