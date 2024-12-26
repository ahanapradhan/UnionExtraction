import sys

from openai import OpenAI

# gets API Key from environment variable OPENAI_API_KEY
client = OpenAI()

text_2_sql_prompt = """Give me SQL for the following text:

The query identifies sarabharajudara who have an excess of a given vastuvivara available; 
an excess is defined to be more than 50% of the vastuvivara like the given 
vastuvivara that the sarabharajudara shipped in the year 1995 for a given rashtra 'FRANCE'. 
Only vastuvivara whose names have the pattern 'ivory' are considered.

Give only the SQL, do not add any explaination.
Put the SQL within python style comment quotes.

Consider the following schema while formulating the SQL query:

CREATE TABLE Rashtra (
    r_rashtrakramank INTEGER PRIMARY KEY,
    r_rashtranama VARCHAR(25),
    r_rashtramaahiti VARCHAR(152),
    r_pradeshakramank INTEGER
);

CREATE TABLE Pradesh (
    p_pradeshakramank INTEGER PRIMARY KEY,
    p_pradeshanama VARCHAR(25),
    p_pradeshamaahiti VARCHAR(152)
);

CREATE TABLE Graahaka (
    g_graahakakramank INTEGER PRIMARY KEY,
    g_graahakanama VARCHAR(25),
    g_graahakavyavahari VARCHAR(40),
    g_graahakathikana VARCHAR(40),
    g_graahakavishaya VARCHAR(15),
    g_graahakamaahiti VARCHAR(117),
    g_graahakakhata DOUBLE PRECISION,
    g_rashtrakramank INTEGER REFERENCES Rashtra(r_rashtrakramank)
);

CREATE TABLE Sarabharajudara (
    s_sarabharajudarakramank INTEGER PRIMARY KEY,
    s_sarabharajudaranama VARCHAR(25),
    s_sarabharajudaravyavahari VARCHAR(40),
    s_sarabharajudarathikana VARCHAR(40),
    s_sarabharajudaravishaya VARCHAR(15),
    s_sarabharajudaramaahiti VARCHAR(101),
    s_sarabharajudarakhata DOUBLE PRECISION,
    s_rashtrakramank INTEGER REFERENCES Rashtra(r_rashtrakramank)
);

CREATE TABLE Vastuvivara (
    v_vastukramank INTEGER PRIMARY KEY,
    v_vastunama VARCHAR(55),
    v_vastubranda VARCHAR(25),
    v_vastupaddhati VARCHAR(10),
    v_vastuvivara VARCHAR(23),
    v_vastukurita DOUBLE PRECISION,
    v_vastuyogyate VARCHAR(10),
    v_pradeshakramank INTEGER REFERENCES Pradesh(p_pradeshakramank)
);

CREATE TABLE Sarabharajudaravastu (
    sv_vastukramank INTEGER REFERENCES Vastuvivara(v_vastukramank),
    sv_sarabharajudarakramank INTEGER REFERENCES Sarabharajudara(s_sarabharajudarakramank),
    sv_vastubelav DOUBLE PRECISION,
    sv_vastukanishta INTEGER,
    sv_vastuvivara VARCHAR(199),
    PRIMARY KEY (sv_vastukramank, sv_sarabharajudarakramank)
);

CREATE TABLE Aajna (
    a_aajnakramank INTEGER PRIMARY KEY,
    a_graahakakramank INTEGER REFERENCES Graahaka(g_graahakakramank),
    a_aajnatharikh DATE,
    a_aajnathapradana DOUBLE PRECISION,
    a_aajnasthiti CHAR(1),
    a_aajnamahiti VARCHAR(79)
);

CREATE TABLE Aajnavastu (
    av_aajnakramank INTEGER REFERENCES Aajna(a_aajnakramank),
    av_vastukramank INTEGER REFERENCES Vastuvivara(v_vastukramank),
    av_sarabharajudarakramank INTEGER REFERENCES Sarabharajudara(s_sarabharajudarakramank),
    av_vastusankhya INTEGER,
    av_vastubela DOUBLE PRECISION,
    av_discount DOUBLE PRECISION,
    av_kar DOUBLE PRECISION,
    av_vastusanket CHAR(1),
    av_vastusamahita DATE,
    av_vastupoorakathe DATE,
    av_vastusamapti DATE,
    av_vastutharate CHAR(1),
    av_vastumahiti VARCHAR(44),
    PRIMARY KEY (av_aajnakramank, av_vastukramank, av_sarabharajudarakramank)
);

Mandatory instructions on query formulation:
0. Use the following tables, each only once, in the query: Aajnavastu, Vastuvivara, Sarabharajudaravastu, Sarabharajudara, Rashtra
1. Do not use redundant join conditions.
2. Do not use any predicate with place holder parameter.
3. No attribute in the database has NULL value.
Strictly follow the above instructions while formulating the query.

"""


def one_round():
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "user",
                "content": f"{text_2_sql_prompt}",
            },
        ], temperature=0, stream=False
    )
    reply = response.choices[0].message.content
    print(reply)
    """
    for chunk in response:
        if not chunk.choices:
            continue

        print(chunk.choices[0].delta.content, end="")
        print("")
    """


orig_out = sys.stdout
f = open('chatgpt_tpch_sql.py', 'w')
sys.stdout = f
one_round()
sys.stdout = orig_out
f.close()
