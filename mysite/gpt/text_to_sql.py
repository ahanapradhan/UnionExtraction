import sys

from openai import OpenAI

from .tpcds_chatgpt_text import Q4_CTE_TEXT

# gets API Key from environment variable OPENAI_API_KEY
client = OpenAI()

text_2_sql_prompt = "Considering TPCDS schema, give me SQL query (only the SQL, " \
                    "without any further explaination) which fits best with the following business question: "

sql_2_text_prompt = "Considering TPCDS schema, give me the business question in text (enclosed within \"\")" \
                    "(only the text description in a paragraph, not any explaination) " \
                    "which best fits with the following SQL query:"


def one_round(text):
    print("----- streaming request -----")
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "user",
                "content": f"{text_2_sql_prompt} {text}",
            },
        ], temperature=0, stream=True
    )
    for chunk in response:
        if not chunk.choices:
            continue

        print(chunk.choices[0].delta.content, end="")
        print("")


orig_out = sys.stdout
f = open('./mysite/gpt/chatgpt_tpcds_sql.py', 'w')
sys.stdout = f
for query in [Q4_CTE_TEXT]:
    one_round(query)
sys.stdout = orig_out
f.close()
