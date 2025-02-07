import sys

import tiktoken
from openai import OpenAI

# gets API Key from environment variable OPENAI_API_KEY
client = OpenAI()


text_2_sql_prompt = """Give me SQL for the following text: 
This query determines the distribution of kretas by the number of hukums they have made, 
including kretas who have no record of hukums, in past or present. 
It counts and reports how many kretas have no hukums, how many have 1, 2, 3, etc. 
A check is made to ensure that the hukums counted do not fall into one of several special categories of hukums. Special categories are identified in the hukums comment column by looking for a '%special%requests%' pattern. Consider the following schema while formulating the SQL: 



Following tables are used in the query: KRETA, HUKUM. 
The following join predicates are present in the query: H_HUKEY = K_CUSTKEY. 
The following projections are present in the query: count(*), H_HUDATE. 
The following attributes are present in the group by clause of the query: H_HUDATE. 
The query may have nested inner query.
"""


shot_2 = """
The query has 3 projections, with their names as c_count, c_orderdate and custdist. 
hukum_date corresponds to c_count. 
kreta_count corresponds to count(*). 
Find out custdist to match the text.
"""
shot_3 ="""
c_count column has values 1, 2, 3 etc in the result. 
Fix your query to match this.

"""
shot_4 ="""
Fix the following SQL to match the input-output. 
h_hudate attribute should be used in the projection and group by clauses of the final query. 
SELECT hukum_count AS c_count, -- Number of valid hukums per kreta 
COUNT(*) AS custdist -- Number of kretas with the same hukum count 
FROM ( SELECT c_custkey, COUNT(o_orderkey) AS hukum_count 
FROM customer LEFT JOIN orders ON c_custkey = o_custkey 
AND o_comment NOT LIKE '%special%requests%' GROUP BY c_custkey ) 
AS customer_hukum_counts GROUP BY hukum_count ORDER BY hukum_count ASC;
 
schema: 
CREATE TABLE KRETA ( K_CUSTKEY SERIAL, K_NAME VARCHAR(25), K_ADDRESS VARCHAR(40), K_DESHKEY INTEGER NOT NULL, -- references D_DESHKEY K_PHONE CHAR(15), K_ACCTBAL DECIMAL, K_PROKAR CHAR(10), K_COMMENT VARCHAR(117) ); 
CREATE TABLE HUKUM ( H_HUKEY SERIAL, H_CUSTKEY INTEGER NOT NULL, -- references K_CUSTKEY H_HUKUMTATUS CHAR(1), H_TOTALPRICE DECIMAL, H_HUDATE DATE, H_HUPRIORITY CHAR(15), H_CLERK CHAR(15), H_SHIPPRIORITY INTEGER, H_COMMENT VARCHAR(79) ); 
input: table kreta: 
1        "Customer#000000001"        "IVhzIApeRb ot,c,E"        15        "25-989-741-2988"        711.56        "BUILDING "        "to the even, regular platelets. regular, ironic epitaphs nag e" 
2        "Customer#000000002"        "XSTf4,NCwDVaWNe6tEgvwfmRchLXak"        13        "23-768-687-3665"        121.65        "AUTOMOBILE"        "l accounts. blithely ironic theodolites integrate boldly: caref" 
3        "Customer#000000003"        "MG9kdTD2WBHm"        1        "11-719-748-3364"        7498.12        "AUTOMOBILE"        " deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov" 
4        "Customer#000000004"        "XxVSJsLAGtn"        4        "14-128-190-5944"        2866.83        "MACHINERY "        " requests. final, regular ideas sleep final accou" 
5        "Customer#000000005"        "KvpyuHCplrB84WgAiGV6sYpZq7Tj"        3        "13-750-942-6364"        794.47        "HOUSEHOLD "        "n accounts will have to unwind. foxes cajole accor" 
6        "Customer#000000006"        "sKZz0CsnMD7mp4Xd0YrBvx,LREYKUWAh yVn"        20        "30-114-968-4951"        7638.57        "AUTOMOBILE"        "tions. even deposits boost according to the slyly bold packages. final accounts cajole requests. furious" 
7        "Customer#000000007"        "TcGe5gaZNgVePxU5kRrvXBfkasDTea"        18        "28-190-982-9759"        9561.95        "AUTOMOBILE"        "ainst the ironic, express theodolites. express, even pinto beans among the exp" 
8        "Customer#000000008"        "I0B10bB0AymmC, 0PrRYBCP1yGJ8xcBPmWhl5"        17        "27-147-574-9335"        6819.74        "BUILDING "        "among the slyly regular theodolites kindle blithely courts. carefully even theodolites haggle slyly along the ide" 
9        "Customer#000000009"        "xKiAFTjUsCuxfeleNqefumTrjS"        8        "18-338-906-3675"        8324.07        "FURNITURE "        "r theodolites according to the requests wake thinly excuses: pending requests haggle furiousl" 
10        "Customer#000000010"        "6LrEaV6KR6PLVcgl2ArL Q3rqzLzcT1 v2"        5        "15-741-346-9870"        2753.54        "HOUSEHOLD "        "es regular deposits haggle. fur" 
9173        "Customer#000009173"        "J,DDGgvPAYPtVVmrJEf9P0qmIsMa9nX"        21        "31-295-102-6784"        7825.85        "MACHINERY "        "refully? express, regular pinto beans according to the final dependencies integrate regu" 
65003        "Customer#000065003"        "Uvc6EL2aXpbYmM4cqo3Zl15,El1"        10        "20-344-332-1611"        3057.31        "MACHINERY "        "efully silent requests wake fluffily above the furiously ironic instructions. regul" 
69907        "Customer#000069907"        " wsJD2vI0ykDND7YBV7d6TbLvv TZj06SU,L"        16        "26-451-896-8813"        134.81        "FURNITURE "        "e alongside of the always even accounts. brave deposits nag fluffil" 
92101        "Customer#000092101"        "KNM8yon1cK9YwVYVZl h6iU"        19        "29-517-223-9433"        9555.81        "HOUSEHOLD "        "e ideas haggle slyly after the ironic, even foxes. bold tithes around the regular id" 
97762        "Customer#000097762"        "KPAKqViTQ79aGimDDL"        2        "12-847-650-5180"        1791.51        "AUTOMOBILE"        " blithely ironic platelets haggle carefully slyly blithe foxes. brave, regular pinto beans after the blithely pendin" 
108329        "Customer#000108329"        "onQupjdagLioEDzvx a7,"        15        "25-966-528-5654"        1348.15        "MACHINERY "        "ccording to the furiously bold pack" 110239        "Customer#000110239"        "arQFcOQHO4mCv4AS7ia,wSbtALVonjoPDgzGfh"        15        "25-636-569-5568"        513.76        "AUTOMOBILE"        " dolphins haggle slyly against the carefully ironic asympt" 
114913        "Customer#000114913"        "o4cYmrBfHu8TMPPFy7"        21        "31-641-361-8112"        6362.24        "AUTOMOBILE"        "ily regular pinto beans. regular deposits against the unusual packages engage furiously special ideas. thinl" 
114980        "Customer#000114980"        "hEMrRKrbaT 3w0Ujh5yNFQZl"        6        "16-913-755-5631"        4327.40        "FURNITURE "        "ecial foxes: furiously unusual reques" 
131048        "Customer#000131048"        "okJsLKaD77E4n5 JM2JWYqcrT"        12        "22-812-281-1179"        4211.04        "FURNITURE "        "refully enticing excuses cajole fluffily. blithely final pinto beans promise quickly slyly final sheaves. carefully " 

table hukums: 
1798564        114913        "F"        17568.26        "1992-02-11"        "2-HIGH "        "Clerk#000000373"        0        "counts. carefully unusual gifts use quickly along the slyly bold foxes. q" 
1798565        92101        "F"        142767.20        "1992-06-17"        "2-HIGH "        "Clerk#000000517"        0        "dolites wake daringly around the even pains. f" 1798566        69907        "F"        145736.06        "1994-07-06"        "2-HIGH "        "Clerk#000000944"        0        "ggle. regular, ironic packages cajole bl" 1798567        97762        "F"        124611.17        "1995-01-31"        "3-MEDIUM "        "Clerk#000000508"        0        " wake. packages poac" 1798592        110239        "O"        265909.17        "1998-01-06"        "5-LOW "        "Clerk#000000698"        0        " quickly. carefully ironic instructions cajole quickly al" 1798593        114980        "O"        145706.20        "1998-04-26"        "1-URGENT "        "Clerk#000000891"        0        " quickly regular pinto beans. slyly ironic platelets maintain furi" 1798594        9173        "O"        146467.34        "1996-01-27"        "2-HIGH "        "Clerk#000000801"        0        " packages. thin, silent f" 1798595        108329        "O"        319689.68        "1996-03-20"        "2-HIGH "        "Clerk#000000917"        0        " around the final packages wake ironic, regular accounts. regular, even co" 1798596        131048        "O"        290838.48        "1998-07-09"        "4-NOT SPECIFIED"        "Clerk#000000258"        0        "ross the special accounts. blithely unusual deposit" 1798597        65003        "O"        152586.37        "1997-12-28"        "3-MEDIUM "        "Clerk#000000007"        0        "ests wake dependencies. furiously unusual re" Output:         0        10 "1998-07-09"        1        1 "1998-04-26"        1        1 "1998-01-06"        1        1 "1997-12-28"        1        1 "1996-03-20"        1        1 "1996-01-27"        1        1 "1995-01-31"        1        1 "1994-07-06"        1        1 "1992-06-17"        1        1 "1992-02-11"        1        1
do not remove nesting of the query.

"""
shot_5 ="""
Order By c_count desc, custdist asc; and validate the output

"""
shot_6 ="""
Fix the following SQL to match the input-output. 
h_hudate attribute should be added in the projection and group by clauses of the final query. 
SELECT hukum_count AS c_count, -- Number of valid hukums per kreta COUNT(*) AS custdist -- Number of kretas with the same hukum count FROM ( SELECT c_custkey, COUNT(o_orderkey) AS hukum_count FROM customer LEFT JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%' GROUP BY c_custkey ) AS customer_hukum_counts GROUP BY hukum_count ORDER BY hukum_count ASC; schema: CREATE TABLE KRETA ( K_CUSTKEY SERIAL, K_NAME VARCHAR(25), K_ADDRESS VARCHAR(40), K_DESHKEY INTEGER NOT NULL, -- references D_DESHKEY K_PHONE CHAR(15), K_ACCTBAL DECIMAL, K_PROKAR CHAR(10), K_COMMENT VARCHAR(117) ); CREATE TABLE HUKUM ( H_HUKEY SERIAL, H_CUSTKEY INTEGER NOT NULL, -- references K_CUSTKEY H_HUKUMTATUS CHAR(1), H_TOTALPRICE DECIMAL, H_HUDATE DATE, H_HUPRIORITY CHAR(15), H_CLERK CHAR(15), H_SHIPPRIORITY INTEGER, H_COMMENT VARCHAR(79) ); input: table kreta: 1        "Customer#000000001"        "IVhzIApeRb ot,c,E"        15        "25-989-741-2988"        711.56        "BUILDING "        "to the even, regular platelets. regular, ironic epitaphs nag e" 2        "Customer#000000002"        "XSTf4,NCwDVaWNe6tEgvwfmRchLXak"        13        "23-768-687-3665"        121.65        "AUTOMOBILE"        "l accounts. blithely ironic theodolites integrate boldly: caref" 3        "Customer#000000003"        "MG9kdTD2WBHm"        1        "11-719-748-3364"        7498.12        "AUTOMOBILE"        " deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov" 4        "Customer#000000004"        "XxVSJsLAGtn"        4        "14-128-190-5944"        2866.83        "MACHINERY "        " requests. final, regular ideas sleep final accou" 5        "Customer#000000005"        "KvpyuHCplrB84WgAiGV6sYpZq7Tj"        3        "13-750-942-6364"        794.47        "HOUSEHOLD "        "n accounts will have to unwind. foxes cajole accor" 6        "Customer#000000006"        "sKZz0CsnMD7mp4Xd0YrBvx,LREYKUWAh yVn"        20        "30-114-968-4951"        7638.57        "AUTOMOBILE"        "tions. even deposits boost according to the slyly bold packages. final accounts cajole requests. furious" 7        "Customer#000000007"        "TcGe5gaZNgVePxU5kRrvXBfkasDTea"        18        "28-190-982-9759"        9561.95        "AUTOMOBILE"        "ainst the ironic, express theodolites. express, even pinto beans among the exp" 8        "Customer#000000008"        "I0B10bB0AymmC, 0PrRYBCP1yGJ8xcBPmWhl5"        17        "27-147-574-9335"        6819.74        "BUILDING "        "among the slyly regular theodolites kindle blithely courts. carefully even theodolites haggle slyly along the ide" 9        "Customer#000000009"        "xKiAFTjUsCuxfeleNqefumTrjS"        8        "18-338-906-3675"        8324.07        "FURNITURE "        "r theodolites according to the requests wake thinly excuses: pending requests haggle furiousl" 10        "Customer#000000010"        "6LrEaV6KR6PLVcgl2ArL Q3rqzLzcT1 v2"        5        "15-741-346-9870"        2753.54        "HOUSEHOLD "        "es regular deposits haggle. fur" 9173        "Customer#000009173"        "J,DDGgvPAYPtVVmrJEf9P0qmIsMa9nX"        21        "31-295-102-6784"        7825.85        "MACHINERY "        "refully? express, regular pinto beans according to the final dependencies integrate regu" 65003        "Customer#000065003"        "Uvc6EL2aXpbYmM4cqo3Zl15,El1"        10        "20-344-332-1611"        3057.31        "MACHINERY "        "efully silent requests wake fluffily above the furiously ironic instructions. regul" 69907        "Customer#000069907"        " wsJD2vI0ykDND7YBV7d6TbLvv TZj06SU,L"        16        "26-451-896-8813"        134.81        "FURNITURE "        "e alongside of the always even accounts. brave deposits nag fluffil" 92101        "Customer#000092101"        "KNM8yon1cK9YwVYVZl h6iU"        19        "29-517-223-9433"        9555.81        "HOUSEHOLD "        "e ideas haggle slyly after the ironic, even foxes. bold tithes around the regular id" 97762        "Customer#000097762"        "KPAKqViTQ79aGimDDL"        2        "12-847-650-5180"        1791.51        "AUTOMOBILE"        " blithely ironic platelets haggle carefully slyly blithe foxes. brave, regular pinto beans after the blithely pendin" 108329        "Customer#000108329"        "onQupjdagLioEDzvx a7,"        15        "25-966-528-5654"        1348.15        "MACHINERY "        "ccording to the furiously bold pack" 110239        "Customer#000110239"        "arQFcOQHO4mCv4AS7ia,wSbtALVonjoPDgzGfh"        15        "25-636-569-5568"        513.76        "AUTOMOBILE"        " dolphins haggle slyly against the carefully ironic asympt" 114913        "Customer#000114913"        "o4cYmrBfHu8TMPPFy7"        21        "31-641-361-8112"        6362.24        "AUTOMOBILE"        "ily regular pinto beans. regular deposits against the unusual packages engage furiously special ideas. thinl" 114980        "Customer#000114980"        "hEMrRKrbaT 3w0Ujh5yNFQZl"        6        "16-913-755-5631"        4327.40        "FURNITURE "        "ecial foxes: furiously unusual reques" 131048        "Customer#000131048"        "okJsLKaD77E4n5 JM2JWYqcrT"        12        "22-812-281-1179"        4211.04        "FURNITURE "        "refully enticing excuses cajole fluffily. blithely final pinto beans promise quickly slyly final sheaves. carefully " table hukums: 1798564        114913        "F"        17568.26        "1992-02-11"        "2-HIGH "        "Clerk#000000373"        0        "counts. carefully unusual gifts use quickly along the slyly bold foxes. q" 1798565        92101        "F"        142767.20        "1992-06-17"        "2-HIGH "        "Clerk#000000517"        0        "dolites wake daringly around the even pains. f" 1798566        69907        "F"        145736.06        "1994-07-06"        "2-HIGH "        "Clerk#000000944"        0        "ggle. regular, ironic packages cajole bl" 1798567        97762        "F"        124611.17        "1995-01-31"        "3-MEDIUM "        "Clerk#000000508"        0        " wake. packages poac" 1798592        110239        "O"        265909.17        "1998-01-06"        "5-LOW "        "Clerk#000000698"        0        " quickly. carefully ironic instructions cajole quickly al" 1798593        114980        "O"        145706.20        "1998-04-26"        "1-URGENT "        "Clerk#000000891"        0        " quickly regular pinto beans. slyly ironic platelets maintain furi" 1798594        9173        "O"        146467.34        "1996-01-27"        "2-HIGH "        "Clerk#000000801"        0        " packages. thin, silent f" 1798595        108329        "O"        319689.68        "1996-03-20"        "2-HIGH "        "Clerk#000000917"        0        " around the final packages wake ironic, regular accounts. regular, even co" 1798596        131048        "O"        290838.48        "1998-07-09"        "4-NOT SPECIFIED"        "Clerk#000000258"        0        "ross the special accounts. blithely unusual deposit" 1798597        65003        "O"        152586.37        "1997-12-28"        "3-MEDIUM "        "Clerk#000000007"        0        "ests wake dependencies. furiously unusual re" Output:         0        10 "1998-07-09"        1        1 "1998-04-26"        1        1 "1998-01-06"        1        1 "1997-12-28"        1        1 "1996-03-20"        1        1 "1996-01-27"        1        1 "1995-01-31"        1        1 "1994-07-06"        1        1 "1992-06-17"        1        1 "1992-02-11"        1        1

"""


def count_tokens(text):
    encoding = tiktoken.encoding_for_model("gpt-4o")
    tokens = encoding.encode(text)
    return len(tokens)


def one_round():
    text = f"{text_2_sql_prompt}"
    c_token = count_tokens(text)
    print(f"\nToken count = {c_token}\n")
    text = f"{text_2_sql_prompt}\n{shot_2}"
    c_token = count_tokens(text)
    print(f"\nToken count = {c_token}\n")
    text = f"{text_2_sql_prompt}\n{shot_2}\n{shot_3}"
    c_token = count_tokens(text)
    print(f"\nToken count = {c_token}\n")
    text = f"{text_2_sql_prompt}\n{shot_2}\n{shot_3}\n{shot_4}"
    c_token = count_tokens(text)
    print(f"\nToken count = {c_token}\n")
    text = f"{text_2_sql_prompt}\n{shot_2}\n{shot_3}\n{shot_4}\n{shot_5}"
    c_token = count_tokens(text)
    print(f"\nToken count = {c_token}\n")
    text = f"{text_2_sql_prompt}\n{shot_2}\n{shot_3}\n{shot_4}\n{shot_5}\n{shot_6}"
    c_token = count_tokens(text)
    print(f"\nToken count = {c_token}\n")


orig_out = sys.stdout
f = open('chatgpt_tpch_sql.py', 'w')
sys.stdout = f
one_round()
sys.stdout = orig_out
f.close()
