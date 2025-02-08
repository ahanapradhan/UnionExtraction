import sys

import tiktoken
from openai import OpenAI

# gets API Key from environment variable OPENAI_API_KEY
client = OpenAI()

text_2_sql_prompt = """Give me SQL for the following text:

This query counts how many Graahaka within Rashtra codes among 
'13', '31', '23', '29', '30', '18', and '17' have 
not placed Aajna for 7 years but who have a greater than average “positive” g_mulyam. 
It also reflects the magnitude of that g_mulyam.  
Rashtra code is defined as the first two characters of g_vishaya.

Give only the SQL, do not add any explaination.
Put the SQL within double quotes.
Consider the following schema while formulating the SQL query:

CREATE TABLE Rekhaanasha (
    r_aajnarekha SERIAL PRIMARY KEY,
    r_aajnakila INT REFERENCES Aajna(a_aajnakila),
    r_anshakila INT REFERENCES Ansha(a_anshakila),
    r_pradatakila INT REFERENCES Pradata(p_pradatakila),
    r_parimaanam INT,
    r_vistaritamulyam DECIMAL(12,2),
    r_vilopam DECIMAL(12,2),
    r_vahanadivasa DATE NOT NULL,
    r_engagement_divasa DATE NOT NULL
);

CREATE TABLE Rashtra (
    r_rashtrakila SERIAL PRIMARY KEY,
    r_nama VARCHAR(50) NOT NULL,
    r_kshatrakila INT REFERENCES Kshetra(k_kshatrakila),
    r_pravritti TEXT
);

CREATE TABLE Pradata (
    p_pradatakila SERIAL PRIMARY KEY,
    p_nama VARCHAR(50) NOT NULL,
    p_vishaya TEXT,
    p_rashtrakila INT REFERENCES Rashtra(r_rashtrakila),
    p_mulyam DECIMAL(12,2)
);

CREATE TABLE Kshetra (
    k_kshatrakila SERIAL PRIMARY KEY,
    k_nama VARCHAR(50) NOT NULL,
    k_pravritti TEXT
);

CREATE TABLE Graahaka (
    g_graahakakila SERIAL PRIMARY KEY,
    g_nama VARCHAR(50) NOT NULL,
    g_rashtrakila INT REFERENCES Rashtra(r_rashtrakila),
    g_vishaya TEXT,
    g_mulyam DECIMAL(12,2)
);

CREATE TABLE AnshaPradata (
    ap_anshakila INT REFERENCES Ansha(a_anshakila),
    ap_pradatakila INT REFERENCES Pradata(p_pradatakila),
    ap_pravritti TEXT,
    ap_mulyam DECIMAL(12,2),
    PRIMARY KEY (a_anshakila, p_pradatakila)
);

CREATE TABLE Ansha (
    a_anshakila SERIAL PRIMARY KEY,
    a_nama VARCHAR(50) NOT NULL,
    a_prakriti VARCHAR(50),
    a_mulyam DECIMAL(12,2)
);

CREATE TABLE Aajna (
    a_aajnakila SERIAL PRIMARY KEY,
    a_graahakakila INT REFERENCES Graahaka(g_graahakakila),
    a_aajnastithi CHAR(1) NOT NULL,
    a_tarikh DATE NOT NULL,
    a_mulyam DECIMAL(12,2)
);


Further instructions on query formulation:
1. Do not use redundant join conditions.
2. Do not use any predicate with place holder parameter.
3. No attribute in the database has NULL value.
    Here is a query:
    SELECT 
    '30' AS rashtra_code,
    COUNT(*) AS graahaka_count,
    SUM(c1.g_mulyam) AS total_g_mulyam
    FROM Graahaka c1, Graahaka c2, Aajna a
    WHERE c2.g_mulyam < c1.g_mulyam
    AND c1.g_vishaya LIKE '30%'
    AND c2.g_vishaya LIKE '31%'
    AND c2.g_mulyam < 0.01;

    The intended query has predicates whose attributes are present in the above query.
 4. Do not use predicate on any other attribute than that are used in the above query. 
Strictly follow the above instructions while formulating the query.

Sample Data: Table Aajna:
4722624	214	"O"	79702.53	"1997-10-22"	"2-HIGH         "	"Clerk#000000453"	0	"dle theodolites cajole fluffily above the c"
4728867	136	"F"	208445.77	"1992-06-18"	"1-URGENT       "	"Clerk#000000956"	0	"ts use carefully slyly pending "
4746082	314	"F"	235799.31	"1992-09-22"	"5-LOW          "	"Clerk#000000793"	0	"nusual pinto beans use furiously ac"
4752803	100	"F"	272934.83	"1993-05-11"	"3-MEDIUM       "	"Clerk#000000285"	0	"ss requests. quiet, ironic asymptotes cajole d"
4764293	13	"F"	52585.81	"1992-03-15"	"5-LOW          "	"Clerk#000000262"	0	"e express, final instructions integrate fluffily attainments. theo"
4769798	227	"O"	112046.85	"1995-05-23"	"3-MEDIUM       "	"Clerk#000000359"	0	"r, ironic requests nag pending, even dep"
4776288	28	"O"	54474.14	"1996-02-22"	"5-LOW          "	"Clerk#000000124"	0	"lly special pinto beans was against the even"
4782050	211	"F"	96478.84	"1993-01-24"	"4-NOT SPECIFIED"	"Clerk#000000905"	0	"oxes boost. furiously fi"
4787427	136	"O"	7078.71	"1996-08-19"	"5-LOW          "	"Clerk#000000391"	0	"against the furiously final courts. requests should haggle fluffily across t"
4789701	251	"F"	154129.68	"1993-02-25"	"5-LOW          "	"Clerk#000000677"	0	"quickly silent ideas unwind busily for the care"
4791936	247	"P"	150016.93	"1995-06-03"	"1-URGENT       "	"Clerk#000000353"	0	"ccounts around the fluffily s"
4793058	176	"O"	154975.16	"1995-11-13"	"2-HIGH         "	"Clerk#000000268"	0	"ct furiously. regular excuses"
4794213	22	"F"	184300.17	"1995-01-10"	"5-LOW          "	"Clerk#000000846"	0	"ccounts serve. blithely bold foxes detect slyly regular, fina"
4797633	154	"O"	299716.94	"1997-04-24"	"1-URGENT       "	"Clerk#000000048"	0	"nstructions. deposits cajole blithely around"
4809989	301	"F"	337072.14	"1992-06-15"	"1-URGENT       "	"Clerk#000000786"	0	"endencies use slyly quickly regular deposits! furiously p"
4812838	161	"F"	210231.56	"1994-11-05"	"5-LOW          "	"Clerk#000000386"	0	"ts promise enticingly above t"
4838755	178	"F"	19588.32	"1993-11-04"	"3-MEDIUM       "	"Clerk#000000109"	0	"ounts cajole of the "
4821953	316	"O"	177228.72	"1997-02-17"	"3-MEDIUM       "	"Clerk#000000977"	0	"s. ironic pinto beans"
4823748	100	"F"	102948.74	"1995-02-21"	"1-URGENT       "	"Clerk#000000717"	0	"blithely thin foxes cajole b"
4848672	335	"F"	250436.22	"1994-12-28"	"1-URGENT       "	"Clerk#000000216"	0	"haggle blithely. slyl"
4866019	268	"O"	190335.79	"1996-12-11"	"5-LOW          "	"Clerk#000000558"	0	"ng the deposits are above the close, ironic ideas. final deposits about the"
4872388	37	"F"	223412.01	"1993-10-16"	"4-NOT SPECIFIED"	"Clerk#000000223"	0	"arefully final asymptotes haggle carefully f"
4877444	145	"F"	105165.08	"1992-01-07"	"5-LOW          "	"Clerk#000000180"	0	"lyly ironic pinto beans cajole blithely ironic court"
4880870	227	"O"	149491.06	"1997-12-15"	"1-URGENT       "	"Clerk#000000555"	0	"furiously. blithely bold platelets affix slyly regular, express packages. "
4881061	28	"O"	37996.67	"1997-02-19"	"3-MEDIUM       "	"Clerk#000000039"	0	"e accounts are carefully quickly reg"
4888039	64	"F"	340617.19	"1993-01-15"	"2-HIGH         "	"Clerk#000000975"	0	"e blithely pending pinto beans. slyly express requests during the blithe"
4883810	226	"F"	40753.89	"1992-03-12"	"2-HIGH         "	"Clerk#000000310"	0	"es. quickly silent instructions are carefully around the acc"
4892388	178	"F"	227521.12	"1992-02-19"	"4-NOT SPECIFIED"	"Clerk#000000182"	0	" carefully. fluffily even excuses affix. attainm"
4893408	301	"O"	142355.37	"1995-05-10"	"4-NOT SPECIFIED"	"Clerk#000000844"	0	". blithely regular foxes are blithely bold requests. quickly regular"
4902112	145	"O"	334416.48	"1997-09-25"	"4-NOT SPECIFIED"	"Clerk#000000949"	0	"ongside of the furiously close requests. slyly unusual requests are quickly b"
4895588	307	"O"	104787.09	"1997-07-04"	"5-LOW          "	"Clerk#000000119"	0	" slyly unusual accounts. bravely express deposits nag final asymptotes. bl"
4910659	163	"O"	174729.08	"1997-07-24"	"1-URGENT       "	"Clerk#000000225"	0	" cajole; ironic instructions after the carefully even deposits cajole abo"
4911424	278	"F"	106123.88	"1992-01-24"	"4-NOT SPECIFIED"	"Clerk#000000194"	0	"endencies breach quickly against the carefully express deposit"
4917250	22	"F"	27723.76	"1993-09-07"	"3-MEDIUM       "	"Clerk#000000028"	0	"regular deposits use regular, furious packages. unusual ideas cajole"
4927587	278	"O"	208674.38	"1997-10-10"	"3-MEDIUM       "	"Clerk#000000305"	0	"ogs doze blithely. quickly"
4928418	214	"O"	49977.15	"1995-04-28"	"4-NOT SPECIFIED"	"Clerk#000000805"	0	"he blithely unusual packages. furi"
4932935	22	"F"	174378.24	"1995-01-01"	"2-HIGH         "	"Clerk#000000935"	0	"e blithely. carefully bold excuses nag among the "
4937442	112	"O"	48569.67	"1997-08-06"	"2-HIGH         "	"Clerk#000000063"	0	"hely express pinto beans. requests are s"
4938240	136	"F"	140701.83	"1992-06-15"	"5-LOW          "	"Clerk#000000032"	0	"e accounts are slyly"
4940704	100	"O"	215953.77	"1995-10-12"	"2-HIGH         "	"Clerk#000000666"	0	"ironic ideas wake blithely unusual requests. express foxes along"
4942119	40	"F"	142224.11	"1992-10-21"	"2-HIGH         "	"Clerk#000000656"	0	"ate permanently across the quickly final instructions. regular req"
4942403	184	"F"	124475.16	"1994-05-21"	"1-URGENT       "	"Clerk#000000799"	0	" detect fluffily special deposits-- theodolites nag carefully during the ironi"
4950721	37	"P"	255097.56	"1995-03-10"	"3-MEDIUM       "	"Clerk#000000156"	0	"to beans! bold, silent"
4952870	146	"O"	235135.43	"1997-12-13"	"4-NOT SPECIFIED"	"Clerk#000000546"	0	"he regular ideas boost silent deposits. special fox"
4956131	233	"O"	231755.45	"1995-11-20"	"3-MEDIUM       "	"Clerk#000000372"	0	"hely even foxes wake. furiously"
4959972	307	"F"	232059.61	"1992-08-22"	"4-NOT SPECIFIED"	"Clerk#000000942"	0	"after the blithely ironic accounts. carefully regular packag"
4962533	257	"O"	175130.63	"1997-09-27"	"1-URGENT       "	"Clerk#000000247"	0	"s use-- bold requests are blithely. final packages p"
4964323	202	"F"	163997.80	"1994-10-21"	"4-NOT SPECIFIED"	"Clerk#000000447"	0	"ly unusual instructions. slyly silent accounts along the"
4991395	298	"F"	88482.66	"1993-01-04"	"2-HIGH         "	"Clerk#000000958"	0	"ckages affix carefully ironic "
4993412	149	"F"	46936.23	"1992-09-23"	"4-NOT SPECIFIED"	"Clerk#000000927"	0	"lar foxes detect fluffily regular courts. fluf"
4993670	160	"F"	66151.77	"1993-10-07"	"1-URGENT       "	"Clerk#000000401"	0	"t the furiously regular accounts. accounts nag sl"
5003527	125	"O"	29455.11	"1995-08-15"	"2-HIGH         "	"Clerk#000000691"	0	"l packages. carefully bold i"
5006500	223	"O"	26755.40	"1997-08-24"	"5-LOW          "	"Clerk#000000890"	0	"ending requests. quickly unusual theodolites cajole. final packages doze sly"
5020326	298	"F"	82804.73	"1992-11-02"	"4-NOT SPECIFIED"	"Clerk#000000157"	0	" mold quickly quickly even asy"
5032935	184	"O"	246418.28	"1996-11-17"	"3-MEDIUM       "	"Clerk#000000574"	0	" foxes! slyly bold requests are up the busy, unusual"
5023782	184	"F"	142086.65	"1992-08-17"	"4-NOT SPECIFIED"	"Clerk#000000572"	0	"ously regular pinto beans wake blithely"
5026788	274	"O"	312406.75	"1995-10-18"	"1-URGENT       "	"Clerk#000000400"	0	"bold pinto beans boost carefully furiously unusual acco"
5027840	43	"O"	226873.09	"1998-05-27"	"1-URGENT       "	"Clerk#000000750"	0	" alongside of the enticing theodolite"
5030849	274	"F"	210832.39	"1992-01-08"	"5-LOW          "	"Clerk#000000040"	0	"ites nag carefully even packages."
5033986	316	"F"	98843.73	"1992-03-23"	"1-URGENT       "	"Clerk#000000159"	0	"ajole carefully permanent deposits. blithely bold asymp"
5065285	235	"P"	227289.56	"1995-04-30"	"5-LOW          "	"Clerk#000000668"	0	"nts. slyly pending ideas wake fluffily about the"
5051588	233	"O"	58820.83	"1997-01-30"	"2-HIGH         "	"Clerk#000000043"	0	"lly after the carefully "
5096449	146	"O"	14065.02	"1996-02-21"	"4-NOT SPECIFIED"	"Clerk#000000865"	0	"al packages boost after the even packages. carefully careful foxes abo"
5066240	335	"F"	114991.39	"1992-04-04"	"4-NOT SPECIFIED"	"Clerk#000000905"	0	"ts haggle furiously "
5072068	211	"F"	355829.69	"1992-03-23"	"1-URGENT       "	"Clerk#000000337"	0	"thin the requests. carefully exp"
5087169	154	"F"	132866.76	"1994-12-07"	"1-URGENT       "	"Clerk#000000336"	0	"fily pending deposi"
5089057	307	"F"	173973.36	"1993-08-29"	"1-URGENT       "	"Clerk#000000383"	0	"nt deposits: slyly regular reque"
5094054	152	"F"	40810.23	"1993-07-19"	"1-URGENT       "	"Clerk#000000382"	0	"ng the foxes are after the slyly speci"
5100898	182	"F"	70896.59	"1992-02-07"	"2-HIGH         "	"Clerk#000000622"	0	"y silent packages. furiously express packages sleep. carefull"
5129313	43	"F"	128886.70	"1992-11-02"	"1-URGENT       "	"Clerk#000000666"	0	"g, final theodolites. furiously even grouches cajole c"
5103490	314	"F"	179200.82	"1992-12-06"	"4-NOT SPECIFIED"	"Clerk#000000980"	0	"s around the ironic deposits boost "
5124578	307	"O"	114639.97	"1997-04-14"	"4-NOT SPECIFIED"	"Clerk#000000964"	0	"y above the excuses. careful"
5169221	301	"F"	180188.70	"1993-09-19"	"2-HIGH         "	"Clerk#000000505"	0	"packages cajole. fluffily ironic instructions nag. regular hock"
5131911	202	"F"	155119.50	"1993-02-07"	"5-LOW          "	"Clerk#000000775"	0	"c platelets along the furiously special asymptotes wake"
5138502	274	"F"	216268.69	"1995-02-07"	"4-NOT SPECIFIED"	"Clerk#000000684"	0	"efully final packages haggle fluffily fluffily special pinto bea"
5153411	208	"O"	65956.80	"1995-11-30"	"1-URGENT       "	"Clerk#000000636"	0	"quests about the furio"
5169793	233	"F"	305402.24	"1992-05-06"	"3-MEDIUM       "	"Clerk#000000062"	0	"e fluffily final deposits. d"
5206661	64	"F"	152568.21	"1992-06-19"	"2-HIGH         "	"Clerk#000000653"	0	"y unusual accounts are quickly blithely express packages. bold ideas"
5182213	149	"F"	161694.60	"1995-03-01"	"2-HIGH         "	"Clerk#000000341"	0	"t deposits are slyly near the q"
5202403	62	"F"	195225.79	"1995-01-16"	"3-MEDIUM       "	"Clerk#000000113"	0	"to beans. pending sheaves nag furiously regular, ironic accounts. quickly ev"
5218916	163	"F"	307051.51	"1994-05-03"	"4-NOT SPECIFIED"	"Clerk#000000845"	0	"al courts. sly packages nag blithely upon the fluffily regul"
5207685	22	"F"	182572.12	"1994-05-28"	"2-HIGH         "	"Clerk#000000763"	0	"its. bold warthogs sleep slyly boldly ironic requests. even, regular requests "
5213157	251	"O"	125699.74	"1995-06-19"	"2-HIGH         "	"Clerk#000000636"	0	"ic pinto beans. even notornis hag"
5246498	235	"F"	115372.59	"1993-05-27"	"1-URGENT       "	"Clerk#000000493"	0	"yly final theodolites? furiously ironic dependencies ca"
5228454	37	"O"	51185.49	"1997-11-13"	"1-URGENT       "	"Clerk#000000498"	0	" express, even orbits. blithely fi"
5237730	40	"F"	196543.45	"1994-08-20"	"2-HIGH         "	"Clerk#000000121"	0	"pinto beans. carefully unusual theodolites are carefully a"
5277061	112	"O"	121283.82	"1995-10-01"	"4-NOT SPECIFIED"	"Clerk#000000015"	0	"y unusual epitaphs. carefully even deposits are slyly r"
5271586	115	"F"	110328.27	"1994-07-12"	"4-NOT SPECIFIED"	"Clerk#000000580"	0	"its along the courts wake carefully requests. qui"
5273862	112	"O"	161616.76	"1995-10-08"	"4-NOT SPECIFIED"	"Clerk#000000166"	0	" even packages. quickly fina"
5372992	64	"F"	85547.71	"1994-11-13"	"4-NOT SPECIFIED"	"Clerk#000000084"	0	"lar instructions cajole furiously blithely i"
5287012	22	"O"	74162.29	"1997-08-11"	"3-MEDIUM       "	"Clerk#000000093"	0	". express theodolites use blithely at"
5306340	314	"F"	52996.31	"1992-02-24"	"1-URGENT       "	"Clerk#000000021"	0	"he furiously final theodolites! furiously regul"
5315204	149	"F"	157214.66	"1993-08-05"	"4-NOT SPECIFIED"	"Clerk#000000618"	0	"l deposits after the ideas run furiously express deposits. express deposi"
5322918	119	"O"	175621.89	"1995-07-22"	"1-URGENT       "	"Clerk#000000257"	0	"ly final accounts affix qu"
5353057	149	"F"	336335.63	"1994-06-30"	"1-URGENT       "	"Clerk#000000197"	0	"ions. quickly even pinto beans cajole. foxes cajole furiously ar"
5361600	184	"F"	167428.26	"1993-09-27"	"3-MEDIUM       "	"Clerk#000000026"	0	"ggle carefully after the fluffily unusual packages. pending deposits use "
5303939	233	"O"	196051.44	"1997-12-08"	"5-LOW          "	"Clerk#000000506"	0	". slyly ironic decoys wake sly"
5331809	151	"O"	103433.29	"1996-12-15"	"5-LOW          "	"Clerk#000000632"	0	"nt requests nag ironically after the deposits. blithely f"
5335748	314	"O"	354803.98	"1997-12-27"	"3-MEDIUM       "	"Clerk#000000898"	0	"epitaphs wake slyly carefully iro"
5336519	112	"F"	202496.90	"1992-12-13"	"1-URGENT       "	"Clerk#000000023"	0	"uick requests: fluff"
5352006	151	"F"	186085.59	"1992-06-26"	"2-HIGH         "	"Clerk#000000797"	0	"ld, bold accounts. regular acco"

On this data, the query should output:
"13"	3	23756.91
"17"	3	26404.69
"18"	4	29679.14
"23"	2	18148.87
"29"	2	17195.08
"30"	6	44997.05
"31"	2	18155.91
"""

next_shot = """
The query produced by you is the following:
SELECT
    LEFT(g.g_vishaya, 2) AS rashtra_code,
    COUNT(g.g_graahakakila) AS graahaka_count,
    SUM(g.g_mulyam) AS total_g_mulyam
FROM Graahaka g
WHERE LEFT(g.g_vishaya, 2) IN (
    '13',
    '31',
    '23',
    '29',
    '30',
    '18',
    '17'
)
AND g.g_mulyam > (
    SELECT AVG(g2.g_mulyam)
    FROM Graahaka g2
    WHERE g2.g_mulyam > 0
)
AND NOT EXISTS (
    SELECT 1
    FROM Aajna a
    WHERE a.a_graahakakila = g.g_graahakakila
      AND a.a_tarikh > CURRENT_DATE - INTERVAL '7 years'
)
GROUP BY LEFT(g.g_vishaya, 2)
ORDER BY rashtra_code;

Output is:
"13"	6	47451.99
"17"	4	35709.74
"18"	5	37188.06
"23"	5	43961.58
"29"	6	47485.92
"30"	10	78480.62
"31"	4	34274.08

It is mismatching. 
Follow my earlier instructions strictly.
Fix the query.
"""

shot_3 = """
The last query formulated by you is as follows:
SELECT
    LEFT(g.g_vishaya, 2) AS rashtra_code,
    COUNT(g.g_graahakakila) AS graahaka_count,
    SUM(g.g_mulyam) AS total_g_mulyam

FROM Graahaka g

WHERE LEFT(g.g_vishaya, 2) IN (
    '13',
    '31',
    '23',
    '29',
    '30',
    '18',
    '17'
)
AND g.g_mulyam > (
    SELECT AVG(g2.g_mulyam)
    FROM Graahaka g2
    WHERE LEFT(g2.g_vishaya, 2) IN (
        '13',
        '31',
        '23',
        '29',
        '30',
        '18',
        '17'
    )
    AND g2.g_mulyam > 0
)
AND NOT EXISTS (
    SELECT 1
    FROM Aajna a
    WHERE a.a_graahakakila = g.g_graahakakila
    AND a.a_tarikh > CURRENT_DATE - INTERVAL '7 years'
)

GROUP BY LEFT(g.g_vishaya, 2)

ORDER BY rashtra_code;

It produces the following output:
"13"	3	23756.91
"17"	3	26404.69
"18"	4	29679.14
"23"	2	18148.87
"29"	2	17195.08
"30"	6	44997.05
"31"	2	18155.91

Results do not match. Fix the query.
"""


def count_tokens(text):
    encoding = tiktoken.encoding_for_model("gpt-4o")
    tokens = encoding.encode(text)
    return len(tokens)


def one_round():
    text = f"{text_2_sql_prompt}"
    c_token = count_tokens(text)
    print(f"\nToken count = {c_token}\n")
    text = f"{text_2_sql_prompt}\n{next_shot}"
    c_token = count_tokens(text)
    print(f"\nToken count = {c_token}\n")
    text = f"{text_2_sql_prompt}\n{next_shot}\n{shot_3}"
    c_token = count_tokens(text)
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
