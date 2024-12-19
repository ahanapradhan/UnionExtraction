import sys

from openai import OpenAI

from .tpcds_benchmark_queries import Q5_CTE, Q11_CTE, Q71_subquery, Q2_subquery, Q54_subquery, Q33_subquery

# gets API Key from environment variable OPENAI_API_KEY
client = OpenAI()

sql_2_text_prompt = "Considering TPCDS schema, give me the business question in text (enclosed within \"\")" \
                    "(only the text description in a paragraph, not any explaination) " \
                    "which best fits with the following SQL query:"

tpcds_q1 = "Find customers who have returned items more than 20% more often than the average customer returns for " \
           "store in a given state for a given year."

tpcds_q2 = "Report the ratios of weekly web and catalog sales increases from one year to the next year " \
           "for each week. That is, compute the increase of Monday, Tuesday, ... Sunday sales from one year " \
           "to the following."

tpcds_q4 = "Find customers who spend more money via catalog than in stores. " \
           "Identify preferred customers and their " \
           "country of origin."


def one_round(query):
    print("----- streaming request -----")
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "user",
                "content": f"{sql_2_text_prompt} {query}",
            },
        ], temperature=0, stream=True
    )
    for chunk in response:
        if not chunk.choices:
            continue

        print(chunk.choices[0].delta.content, end="")
        print("")


orig_out = sys.stdout
f = open('tpcds_chatgpt_text.py', 'a')
sys.stdout = f
for query in [Q33_subquery]:
    one_round(query)
sys.stdout = orig_out
f.close()
