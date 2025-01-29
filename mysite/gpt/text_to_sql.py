import sys

from openai import OpenAI

from .tpcds_chatgpt_text import Q4_CTE_TEXT, Q5_CTE_TEXT, Q11_CTE_TEXT, Q71_subquery_TEXT, Q2_CTE_TEXT, \
    Q54_subquery_TEXT, Q33_subquery_TEXT, Q60_subquery_TEXT

# gets API Key from environment variable OPENAI_API_KEY
client = OpenAI()

text_2_sql_prompt = "Considering TPCDS schema, give me SQL query (only the SQL, " \
                    "without any further explaination) which fits best with the following business question: "


def one_round(text):
    print("----- streaming request -----")
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "user",
                "content": f"{text_2_sql_prompt} {text}",
            },
        ], temperature=0, stream=False
    )
    for chunk in response:
        if not chunk.choices:
            continue

        print(chunk.choices[0].delta.content, end="")
        print("")


orig_out = sys.stdout
f = open('./mysite/gpt/chatgpt_tpcds_sql.py', 'a')
sys.stdout = f
for query in [Q5_CTE_TEXT]:
    one_round(query)
sys.stdout = orig_out
f.close()
