import sys

from openai import OpenAI

from .tpcds_chatgpt_text import Q4_CTE_TEXT, Q5_CTE_TEXT, Q11_CTE_TEXT, Q71_subquery_TEXT, Q2_CTE_TEXT, \
    Q54_subquery_TEXT, Q33_subquery_TEXT, Q60_subquery_TEXT, q56_text_full, Q56_hqe_seed, Q75_text, Q75_hqe_seed, \
    Q76_text_full, Q76_hqe_seed, Q76_text_chatgpt, Q77_text, Q77_hint, Q80_text, Q80_hqe_seed, Q14_subquery_text

# gets API Key from environment variable OPENAI_API_KEY
client = OpenAI()

text_2_sql_prompt = "You are an Text-2-SQL expert. You are good at translating text to their equivalent SQLs." \
                    "Considering TPCDS schema, give me SQL query (only the SQL, " \
                    "without any further explaination) which fits best with the following business question: "

seed_q = """use the following hint SQL, based on which you should formulate your SQL: """
def one_round(text):
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "user",
                "content": f"{text_2_sql_prompt} {text}\n",
                #"content": f"{text_2_sql_prompt} {text}\n {seed_q} {Q80_hqe_seed}",
            },
        ], temperature=0, stream=False
    )
    reply = response.choices[0].message.content
    print(reply)


orig_out = sys.stdout
f = open('chatgpt_tpcds_sql.py', 'w')
sys.stdout = f
one_round(Q14_subquery_text)
sys.stdout = orig_out
f.close()