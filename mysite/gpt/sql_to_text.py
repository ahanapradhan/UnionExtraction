import sys

from openai import OpenAI

from ..gpt.tpch_benchmark_queries import Q1, Q3, Q4, Q5, Q6, Q9, Q10, Q12, Q14, Q15

# gets API Key from environment variable OPENAI_API_KEY
client = OpenAI()

text_2_sql_prompt = "Considering TPCH schema, give me SQL query (only the SQL, " \
                    "without any further explaination) which fits best with the following business question: "

sql_2_text_prompt = "Considering TPCH schema, give me the business question in text (enclosed within \"\")" \
                    "(only the text description in a paragraph, not any explaination) " \
                    "which best fits with the following SQL query:"

tpch_q3 = "select l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue,o_orderdate,o_shippriority " \
          "from customer,orders,lineitem where c_mktsegment = 'BUILDING' " \
          "and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date '1995-01-01' " \
          "and l_shipdate > date '1995-01-01' group by l_orderkey, o_orderdate,o_shippriority order by " \
          "revenue desc,o_orderdate;"
business_qtn_Q1 = "The Pricing Summary Report Query provides a summary pricing report for all lineitems shipped as of a given date. " \
                  "The date is within 100 days of the greatest ship date contained in the database. The query lists totals " \
                  "for extended price, discounted extended price, discounted extended price plus tax, average quantity, average extended " \
                  "price, and average discount. These aggregates are grouped by RETURNFLAG and LINESTATUS, and listed in " \
                  "ascending order of RETURNFLAG and LINESTATUS. A count of the number of lineitems in each group is included."

business_qtn_Q2 = "The Minimum Cost Supplier Query finds, in a given region, for each part of a certain type and size, the supplier who " \
                  "can supply it at minimum cost. If several suppliers in that region offer the desired part type and size at the same " \
                  "(minimum) cost, the query lists the parts from suppliers with the 100 highest account balances. For each supplier, " \
                  "the query lists the supplier's account balance, name and nation; the part's number and manufacturer; the supplier's " \
                  "address, phone number and comment information."

business_qtn_Q3 = "The Shipping Priority Query retrieves the shipping priority and potential revenue, defined as the sum of " \
                  "l_extendedprice * (1-l_discount), of the orders having the largest revenue among those that had not been shipped as " \
                  "of January 1, 2000. Orders are listed in increasing order of the shipping pirority. If more than 17 unshipped orders exist, only the 18 " \
                  "orders with the largest revenue are listed."

original_q3 = "The Shipping Priority Query retrieves the shipping priority and potential revenue, defined as the sum of " \
              "l_extendedprice * (1-l_discount), of the orders having the largest revenue among those that had not been shipped as " \
              "of a given date. Orders are listed in decreasing order of revenue. If more than 10 unshipped orders exist, only the 10 " \
              "orders with the largest revenue are listed."

business_qtn_Q4 = "The Order Priority Checking Query counts the number of orders ordered in a given quarter of year 2005 in which " \
                  "at least one lineitem was received by the customer later than its committed date. The query lists the count of such " \
                  "orders for each order priority sorted in ascending priority order."

business_qtn_Q9 = "The Product Type Profit Measure Query finds, for each nation and each year, the profit for all parts ordered in that " \
                  "year that contain string \"co\" in their names and that were filled by a supplier in that nation. The profit is " \
                  "defined as the sum of [(l_extendedprice*(1-l_discount)) - (ps_supplycost * l_quantity)] for all lineitems describing " \
                  "parts in the specified line. The query lists the nations in ascending alphabetical order and, for each nation, the year " \
                  "and profit in descending order by year (most recent first)."

business_qtn_Q11 = "The Important Stock Identification Query finds, from scanning the available stock of suppliers in a given nation, all " \
                   "the parts that represent a significant percentage of the total value of all available parts. The query displays the part " \
                   "number and the value of those parts in descending order of value."

business_qtn_Q19 = "The Discounted Revenue query finds the gross discounted revenue for all orders for three different types of parts " \
                   "that were shipped by air and delivered in person. Parts are selected based on the combination of specific brands, a " \
                   "list of containers, and a range of sizes."
union_T5 = "This query retrieves a list of names and their associated nations based on specific conditions. " \
           "It selects customers whose market segment is \"BUILDING\" and whose account balance is at least 100.01, linking their information to the corresponding nation. Additionally, it includes suppliers " \
           "with an account balance of at least 4000.01, associating their details with their respective nations. " \
           "The result presents the names of either customers or suppliers alongside the names of their nations. " \
           "This query focuses on filtering individuals or entities that meet these account balance and market segment " \
           "criteria."
union_U2 = "This query retrieves a list of key identifiers and names based on specific conditions and orders " \
           "them accordingly. It first selects key identifiers and names where it is of urgent priority, " \
           "arranging the results in ascending order of the key and descending order of the name, with " \
           "a maximum of 10 entries. Additionally, it selects key identifiers and names linked to 'GERMANY', " \
           "ordering these results by the key in descending order and the name in ascending order, " \
           "with a limit of 12 entries."
demo = "It relates to identifying prime customers who have purchased items at low prices similar to those " \
       "offered by small non-European suppliers."

tpcds_q7 = "Compute the average quantity, list price, discount, and sales price for promotional items sold in stores where the " \
           "promotion is not offered by mail or a special event. Restrict the results to a specific gender, marital and " \
           "educational status."

tpcds_q1 = "Find customers who have returned items more than 20% more often than the average customer returns for " \
           "store in a given state for a given year."

tpcds_q2 = "Report the ratios of weekly web and catalog sales increases from one year to the next year for each week. That is, compute the increase of Monday, Tuesday, ... Sunday sales from one year to the following."

tpcds_q4 = "Find customers who spend more money via catalog than in stores. " \
           "Identify preferred customers and their " \
           "country of origin."
chatgpt_text_tpch_q3 = "What are the most profitable orders for customers in the 'BUILDING' market segment, " \
                       "based on the total revenue generated from line items, for orders placed before " \
                       "January 1, 1995, and shipped after January 1, 1995, and how do these orders " \
                       "rank in terms of revenue and order date?"


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
f = open('tpch_chatgpt_text.py', 'w')
sys.stdout = f
for query in [Q1, Q3, Q4, Q5, Q6, Q9, Q10, Q12, Q14, Q15]:
    one_round(query)
sys.stdout = orig_out
f.close()
