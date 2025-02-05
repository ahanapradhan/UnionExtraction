import sys

import tiktoken
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

CREATE TABLE web_Aajnavastu (
    wav_aajnakramank INTEGER REFERENCES Aajna(a_aajnakramank),
    wav_vastukramank INTEGER REFERENCES Vastuvivara(v_vastukramank),
    wav_sarabharajudarakramank INTEGER REFERENCES Sarabharajudara(s_sarabharajudarakramank),
    wav_vastusankhya INTEGER,
    wav_vastubela DOUBLE PRECISION,
    wav_discount DOUBLE PRECISION,
    wav_kar DOUBLE PRECISION,
    wav_vastusanket CHAR(1),
    wav_vastusamahita DATE,
    wav_vastupoorakathe DATE,
    wav_vastusamapti DATE,
    wav_vastutharate CHAR(1),
    wav_vastumahiti VARCHAR(44),
    PRIMARY KEY (wav_aajnakramank, wav_vastukramank, wav_sarabharajudarakramank)
);

CREATE TABLE store_Aajnavastu (
    sav_aajnakramank INTEGER REFERENCES Aajna(a_aajnakramank),
    sav_vastukramank INTEGER REFERENCES Vastuvivara(v_vastukramank),
    sav_sarabharajudarakramank INTEGER REFERENCES Sarabharajudara(s_sarabharajudarakramank),
    sav_vastusankhya INTEGER,
    sav_vastubela DOUBLE PRECISION,
    sav_discount DOUBLE PRECISION,
    sav_kar DOUBLE PRECISION,
    sav_vastusanket CHAR(1),
    sav_vastusamahita DATE,
    sav_vastupoorakathe DATE,
    sav_vastusamapti DATE,
    sav_vastutharate CHAR(1),
    sav_vastumahiti VARCHAR(44),
    PRIMARY KEY (sav_aajnakramank, sav_vastukramank, sav_sarabharajudarakramank)
);

Mandatory instructions on query formulation:
1. Do not use redundant join conditions.
2. Do not use any predicate with place holder parameter.
3. No attribute in the database has NULL value.
4. Do not use predicate on any other attribute than that are used in the following query:
 SELECT 
    s_sarabharajudaranama,
    s_sarabharajudarathikana
FROM web_Aajnavastu av
JOIN Vastuvivara v ON av.wav_vastukramank = v.v_vastukramank
JOIN Sarabharajudaravastu sv ON v.v_vastukramank = sv.sv_vastukramank
JOIN Sarabharajudara s ON av.wav_sarabharajudarakramank = sv.sv_sarabharajudarakramank
JOIN Rashtra r ON s.s_rashtrakramank = r.r_rashtrakramank
WHERE 
    r.r_rashtranama = 'FRANCE'
    AND av.wav_vastusankhya <= 9687.99
    AND av.wav_vastusamahita BETWEEN '1995-01-01' AND '1995-12-31'
    AND v.v_vastunama LIKE '%ivory%'
    AND sv.sv_vastukanishta >= 12
ORDER BY s_sarabharajudaranama ASC;
5. The attributes present in projections are accurate, use them in projection. 
6. Also use the projection aliases used in the query.
7. Use the tables present in the FROM clause of the query in your query. No table appears more than once in the query.
8. Order by of the above query is accurate, reuse it.
9. Text-based filter predicates of the above query are accurate, however they may be 
scattered around subqueries in the expected query.
Strictly follow the above instructions while formulating the query.
"""

Next_shot = """
The output produced by the above query is:
"Sarabharajudara#000000198"	"ncWe9nTBqJETno"
"Sarabharajudara#000000198"	"ncWe9nTBqJETno"
"Sarabharajudara#000000322"	"lB2qcFCrwazl7Qa"
"Sarabharajudara#000000322"	"lB2qcFCrwazl7Qa"
"Sarabharajudara#000000509"	"SF7dR8V5pK"
"Sarabharajudara#000000509"	"SF7dR8V5pK"
"Sarabharajudara#000000553"	"a,liVofXbCJ"
"Sarabharajudara#000000556"	"g3QRUaiDAI1nQQPJLJfAa9W"
"Sarabharajudara#000000593"	"qvlFqgoEMzzksE2uQlchYQ8V"
"Sarabharajudara#000000616"	"Ktao GA3 5k7oF,wkDyhc0uatR72dD65pD"
"Sarabharajudara#000000616"	"Ktao GA3 5k7oF,wkDyhc0uatR72dD65pD"
"Sarabharajudara#000000769"	"ak2320fUkG"
"Sarabharajudara#000000769"	"ak2320fUkG"
"Sarabharajudara#000000769"	"ak2320fUkG"
"Sarabharajudara#000000812"	"8qh4tezyScl5bidLAysvutB,,ZI2dn6xP"
"Sarabharajudara#000000839"	"1fSx9Sv6LraqnVP3u"
"Sarabharajudara#000000839"	"1fSx9Sv6LraqnVP3u"
"Sarabharajudara#000000839"	"1fSx9Sv6LraqnVP3u"
"Sarabharajudara#000000839"	"1fSx9Sv6LraqnVP3u"
"Sarabharajudara#000000954"	"P3O5p UFz1QsLmZX"
"Sarabharajudara#000000954"	"P3O5p UFz1QsLmZX"
"Sarabharajudara#000000954"	"P3O5p UFz1QsLmZX"
"Sarabharajudara#000000954"	"P3O5p UFz1QsLmZX"
"Sarabharajudara#000000954"	"P3O5p UFz1QsLmZX"
"Sarabharajudara#000001154"	"lPDPT5D5b7u4uNLN, Rl"
"Sarabharajudara#000001154"	"lPDPT5D5b7u4uNLN, Rl"
"Sarabharajudara#000001198"	"vRfsLGzF6aE2XhsqgmJFUHGmMHepJW3X"
"Sarabharajudara#000001198"	"vRfsLGzF6aE2XhsqgmJFUHGmMHepJW3X"
"Sarabharajudara#000001285"	"6GzzLGh7I9P3LhBWnTz,L2gECjp1P1I9mq4TaaK"
"Sarabharajudara#000001285"	"6GzzLGh7I9P3LhBWnTz,L2gECjp1P1I9mq4TaaK"
"Sarabharajudara#000001331"	"6 n,NZ875vge3mSHRgD,"
"Sarabharajudara#000001383"	"HpxV1sNupK1Qe cNH0"
"Sarabharajudara#000001384"	"fjgJwG4DViJrxMxJbO2kS2"
"Sarabharajudara#000001384"	"fjgJwG4DViJrxMxJbO2kS2"
"Sarabharajudara#000001398"	"H1l294pHv2YCA2hQztBZsLGsBmhVBRRh"
"Sarabharajudara#000001398"	"H1l294pHv2YCA2hQztBZsLGsBmhVBRRh"
"Sarabharajudara#000001462"	"HgxOeUIzzWk7BTRw2ax8oHi"
"Sarabharajudara#000001462"	"HgxOeUIzzWk7BTRw2ax8oHi"
"Sarabharajudara#000001541"	"rPUV63BMAmT8Y2qhs 5Z9IT D8zjCJeBHZjW"
"Sarabharajudara#000001541"	"rPUV63BMAmT8Y2qhs 5Z9IT D8zjCJeBHZjW"
"Sarabharajudara#000001576"	"3dj4fsF5fNQ2boo1riXOA7N9t"
"Sarabharajudara#000001576"	"3dj4fsF5fNQ2boo1riXOA7N9t"
"Sarabharajudara#000001576"	"3dj4fsF5fNQ2boo1riXOA7N9t"
"Sarabharajudara#000001776"	"T3DN kKgRFwZQAfUuH1rAWw8qS"
"Sarabharajudara#000001776"	"T3DN kKgRFwZQAfUuH1rAWw8qS"
"Sarabharajudara#000001776"	"T3DN kKgRFwZQAfUuH1rAWw8qS"
"Sarabharajudara#000001784"	"WwxpO7ccLORAYgPyH"
"Sarabharajudara#000001784"	"WwxpO7ccLORAYgPyH"
"Sarabharajudara#000001816"	"e7vab91vLJPWxxZnewmnDBpDmxYHrb"
"Sarabharajudara#000001845"	"Qxx8BfLUs8c1D2umIcr"
"Sarabharajudara#000001866"	"gJ9bAJPfBjX0s5x9dU,qA"
"Sarabharajudara#000001866"	"gJ9bAJPfBjX0s5x9dU,qA"
"Sarabharajudara#000001866"	"gJ9bAJPfBjX0s5x9dU,qA"
"Sarabharajudara#000001938"	"aFMa1UzMRPAO5hsX"
"Sarabharajudara#000001938"	"aFMa1UzMRPAO5hsX"
"Sarabharajudara#000001938"	"aFMa1UzMRPAO5hsX"
"Sarabharajudara#000001938"	"aFMa1UzMRPAO5hsX"
"Sarabharajudara#000002070"	"gZ8nCVAgQIMUfoYvIaTF X"
"Sarabharajudara#000002070"	"gZ8nCVAgQIMUfoYvIaTF X"
"Sarabharajudara#000002162"	"6ya g3MW991n9JfhxSrvgM"
"Sarabharajudara#000002179"	"1bSbNinI5914UbVpjbR8"
"Sarabharajudara#000002179"	"1bSbNinI5914UbVpjbR8"
"Sarabharajudara#000002202"	"l3CTXqUqnR67po0RNhF5"
"Sarabharajudara#000002202"	"l3CTXqUqnR67po0RNhF5"
"Sarabharajudara#000002202"	"l3CTXqUqnR67po0RNhF5"
"Sarabharajudara#000002202"	"l3CTXqUqnR67po0RNhF5"
"Sarabharajudara#000002202"	"l3CTXqUqnR67po0RNhF5"
"Sarabharajudara#000002202"	"l3CTXqUqnR67po0RNhF5"
"Sarabharajudara#000002268"	"1So0dHWj0xfwuNopKvDKFHlCOcL1OvgtkhhUPb"
"Sarabharajudara#000002268"	"1So0dHWj0xfwuNopKvDKFHlCOcL1OvgtkhhUPb"
"Sarabharajudara#000002319"	"3z3bTulBgv8Re30oDzKgGlZQT"
"Sarabharajudara#000002319"	"3z3bTulBgv8Re30oDzKgGlZQT"
"Sarabharajudara#000002319"	"3z3bTulBgv8Re30oDzKgGlZQT"
"Sarabharajudara#000002319"	"3z3bTulBgv8Re30oDzKgGlZQT"
"Sarabharajudara#000002397"	"E0b,zxlk yKgtoKg1jH,"
"Sarabharajudara#000002397"	"E0b,zxlk yKgtoKg1jH,"
"Sarabharajudara#000002397"	"E0b,zxlk yKgtoKg1jH,"
"Sarabharajudara#000002397"	"E0b,zxlk yKgtoKg1jH,"
"Sarabharajudara#000002548"	"UABiGgMCkyTzQnloHsNBCr6da6ITjR"
"Sarabharajudara#000002548"	"UABiGgMCkyTzQnloHsNBCr6da6ITjR"
"Sarabharajudara#000002548"	"UABiGgMCkyTzQnloHsNBCr6da6ITjR"
"Sarabharajudara#000002560"	"gC4t9RFtBMoItUG5dPD"
"Sarabharajudara#000002560"	"gC4t9RFtBMoItUG5dPD"
"Sarabharajudara#000002676"	"Xl4TnYEpX4JlkQh11gL8hXTYRQ1"
"Sarabharajudara#000002692"	"1B3q56lLAYJlOR5LGa V"
"Sarabharajudara#000002692"	"1B3q56lLAYJlOR5LGa V"
"Sarabharajudara#000002766"	"CPJjKybUHBxm0snUwnwWxfZZLk4sbE4JISVWhr"
"Sarabharajudara#000002766"	"CPJjKybUHBxm0snUwnwWxfZZLk4sbE4JISVWhr"
"Sarabharajudara#000002818"	"kzzNb5Jcm9WNmB LGlHk7JgN7"
"Sarabharajudara#000002818"	"kzzNb5Jcm9WNmB LGlHk7JgN7"
"Sarabharajudara#000002818"	"kzzNb5Jcm9WNmB LGlHk7JgN7"
"Sarabharajudara#000002818"	"kzzNb5Jcm9WNmB LGlHk7JgN7"
"Sarabharajudara#000002831"	"8DGtt26QGtxI,3xEQ8gwSwY0JkzYpZWl4OjiunU"
"Sarabharajudara#000002906"	"498dqBD0lISHzpDOGmJf3W57mBSh woorgn"
"Sarabharajudara#000002906"	"498dqBD0lISHzpDOGmJf3W57mBSh woorgn"
"Sarabharajudara#000002924"	"6 nxmhb4Okr1CdJZPA2TaNRrLSXFfzy"
"Sarabharajudara#000002924"	"6 nxmhb4Okr1CdJZPA2TaNRrLSXFfzy"
"Sarabharajudara#000003029"	"aWkIsIRUh3zz8LiwvImuv"
"Sarabharajudara#000003067"	"9EPagnou6ashdkFA"
"Sarabharajudara#000003067"	"9EPagnou6ashdkFA"
"Sarabharajudara#000003086"	"EdiLbOuVZPvcIKQ 8C53GAQCRGDQEn"
"Sarabharajudara#000003133"	"ctd9ax8DHT93kvfF91"
"Sarabharajudara#000003153"	"zZjHS,4cNlNAK1KFaFTNpYh9Y5Ceb"
"Sarabharajudara#000003280"	"TtNwejP, 4GKXNfky9Jc,8gaGEI"
"Sarabharajudara#000003280"	"TtNwejP, 4GKXNfky9Jc,8gaGEI"
"Sarabharajudara#000003280"	"TtNwejP, 4GKXNfky9Jc,8gaGEI"
"Sarabharajudara#000003419"	"yt KX357gL"
"Sarabharajudara#000003419"	"yt KX357gL"
"Sarabharajudara#000003429"	"EAn2WPCt0Glq,y6"
"Sarabharajudara#000003635"	"iZVQF YThR0AJ5kW8QaHZh"
"Sarabharajudara#000003689"	"KuH5dUsSzixv"
"Sarabharajudara#000003689"	"KuH5dUsSzixv"
"Sarabharajudara#000003689"	"KuH5dUsSzixv"
"Sarabharajudara#000003689"	"KuH5dUsSzixv"
"Sarabharajudara#000003746"	"O43Nikgv5lasOik8Ez2mOt3uU"
"Sarabharajudara#000003746"	"O43Nikgv5lasOik8Ez2mOt3uU"
"Sarabharajudara#000003746"	"O43Nikgv5lasOik8Ez2mOt3uU"
"Sarabharajudara#000003746"	"O43Nikgv5lasOik8Ez2mOt3uU"
"Sarabharajudara#000003796"	"gC,28F ofakz0ZdgKQ2nrW7JFO35 RJN"
"Sarabharajudara#000003796"	"gC,28F ofakz0ZdgKQ2nrW7JFO35 RJN"
"Sarabharajudara#000003825"	"hK1aUlbzeTz MSPwcPVyRGY"
"Sarabharajudara#000003825"	"hK1aUlbzeTz MSPwcPVyRGY"
"Sarabharajudara#000003836"	"tdBz4J0l7wDJJu Dej1"
"Sarabharajudara#000003836"	"tdBz4J0l7wDJJu Dej1"
"Sarabharajudara#000003850"	",27mYEAukUi JHLAjUTMCX3hkL8uzcq88"
"Sarabharajudara#000003850"	",27mYEAukUi JHLAjUTMCX3hkL8uzcq88"
"Sarabharajudara#000003892"	"7upn3 0JxQtolUElV7uffY"
"Sarabharajudara#000004072"	"lAYDI98l4wGJ98"
"Sarabharajudara#000004090"	"vRKDWYYcJ9xGtf4xHcWTjXW22"
"Sarabharajudara#000004164"	"f60HY65zdJb6eSCUYOmm"
"Sarabharajudara#000004164"	"f60HY65zdJb6eSCUYOmm"
"Sarabharajudara#000004164"	"f60HY65zdJb6eSCUYOmm"
"Sarabharajudara#000004164"	"f60HY65zdJb6eSCUYOmm"
"Sarabharajudara#000004552"	"eRwxvVjYTpamQHXlldIxF,q8C"
"Sarabharajudara#000004566"	"mAKi0qJOdVHuta0zJx3WUr4er,6QJbSrUXRFN0fN"
"Sarabharajudara#000004579"	"K5nhdAhx6aGpbcRNj0"
"Sarabharajudara#000004579"	"K5nhdAhx6aGpbcRNj0"
"Sarabharajudara#000004592"	"6eoAjyJrWXrsoJr2HelM8zc4ZV5sW,d2je"
"Sarabharajudara#000004592"	"6eoAjyJrWXrsoJr2HelM8zc4ZV5sW,d2je"
"Sarabharajudara#000004597"	"gKuHIUE7XWqK9ZDCA,Kp0jFza4PvTq,RtFF"
"Sarabharajudara#000004597"	"gKuHIUE7XWqK9ZDCA,Kp0jFza4PvTq,RtFF"
"Sarabharajudara#000004597"	"gKuHIUE7XWqK9ZDCA,Kp0jFza4PvTq,RtFF"
"Sarabharajudara#000004597"	"gKuHIUE7XWqK9ZDCA,Kp0jFza4PvTq,RtFF"
"Sarabharajudara#000004746"	"HrNlq N3KfDAfcfX3uho4LqI"

Whereas the output should be:
"Sarabharajudara#000000198"	"ncWe9nTBqJETno"	"FRANCE                   "
"Sarabharajudara#000000322"	"lB2qcFCrwazl7Qa"	"FRANCE                   "
"Sarabharajudara#000000509"	"SF7dR8V5pK"	"FRANCE                   "
"Sarabharajudara#000000553"	"a,liVofXbCJ"	"FRANCE                   "
"Sarabharajudara#000000556"	"g3QRUaiDAI1nQQPJLJfAa9W"	"FRANCE                   "
"Sarabharajudara#000000593"	"qvlFqgoEMzzksE2uQlchYQ8V"	"FRANCE                   "
"Sarabharajudara#000000616"	"Ktao GA3 5k7oF,wkDyhc0uatR72dD65pD"	"FRANCE                   "
"Sarabharajudara#000000769"	"ak2320fUkG"	"FRANCE                   "
"Sarabharajudara#000000812"	"8qh4tezyScl5bidLAysvutB,,ZI2dn6xP"	"FRANCE                   "
"Sarabharajudara#000000839"	"1fSx9Sv6LraqnVP3u"	"FRANCE                   "
"Sarabharajudara#000000954"	"P3O5p UFz1QsLmZX"	"FRANCE                   "
"Sarabharajudara#000001154"	"lPDPT5D5b7u4uNLN, Rl"	"FRANCE                   "
"Sarabharajudara#000001198"	"vRfsLGzF6aE2XhsqgmJFUHGmMHepJW3X"	"FRANCE                   "
"Sarabharajudara#000001285"	"6GzzLGh7I9P3LhBWnTz,L2gECjp1P1I9mq4TaaK"	"FRANCE                   "
"Sarabharajudara#000001331"	"6 n,NZ875vge3mSHRgD,"	"FRANCE                   "
"Sarabharajudara#000001383"	"HpxV1sNupK1Qe cNH0"	"FRANCE                   "
"Sarabharajudara#000001384"	"fjgJwG4DViJrxMxJbO2kS2"	"FRANCE                   "
"Sarabharajudara#000001398"	"H1l294pHv2YCA2hQztBZsLGsBmhVBRRh"	"FRANCE                   "
"Sarabharajudara#000001462"	"HgxOeUIzzWk7BTRw2ax8oHi"	"FRANCE                   "
"Sarabharajudara#000001541"	"rPUV63BMAmT8Y2qhs 5Z9IT D8zjCJeBHZjW"	"FRANCE                   "
"Sarabharajudara#000001576"	"3dj4fsF5fNQ2boo1riXOA7N9t"	"FRANCE                   "
"Sarabharajudara#000001776"	"T3DN kKgRFwZQAfUuH1rAWw8qS"	"FRANCE                   "
"Sarabharajudara#000001784"	"WwxpO7ccLORAYgPyH"	"FRANCE                   "
"Sarabharajudara#000001816"	"e7vab91vLJPWxxZnewmnDBpDmxYHrb"	"FRANCE                   "
"Sarabharajudara#000001845"	"Qxx8BfLUs8c1D2umIcr"	"FRANCE                   "
"Sarabharajudara#000001866"	"gJ9bAJPfBjX0s5x9dU,qA"	"FRANCE                   "
"Sarabharajudara#000001938"	"aFMa1UzMRPAO5hsX"	"FRANCE                   "
"Sarabharajudara#000002070"	"gZ8nCVAgQIMUfoYvIaTF X"	"FRANCE                   "
"Sarabharajudara#000002162"	"6ya g3MW991n9JfhxSrvgM"	"FRANCE                   "
"Sarabharajudara#000002179"	"1bSbNinI5914UbVpjbR8"	"FRANCE                   "
"Sarabharajudara#000002202"	"l3CTXqUqnR67po0RNhF5"	"FRANCE                   "
"Sarabharajudara#000002268"	"1So0dHWj0xfwuNopKvDKFHlCOcL1OvgtkhhUPb"	"FRANCE                   "
"Sarabharajudara#000002319"	"3z3bTulBgv8Re30oDzKgGlZQT"	"FRANCE                   "
"Sarabharajudara#000002397"	"E0b,zxlk yKgtoKg1jH,"	"FRANCE                   "
"Sarabharajudara#000002548"	"UABiGgMCkyTzQnloHsNBCr6da6ITjR"	"FRANCE                   "
"Sarabharajudara#000002560"	"gC4t9RFtBMoItUG5dPD"	"FRANCE                   "
"Sarabharajudara#000002676"	"Xl4TnYEpX4JlkQh11gL8hXTYRQ1"	"FRANCE                   "
"Sarabharajudara#000002692"	"1B3q56lLAYJlOR5LGa V"	"FRANCE                   "
"Sarabharajudara#000002766"	"CPJjKybUHBxm0snUwnwWxfZZLk4sbE4JISVWhr"	"FRANCE                   "
"Sarabharajudara#000002818"	"kzzNb5Jcm9WNmB LGlHk7JgN7"	"FRANCE                   "
"Sarabharajudara#000002831"	"8DGtt26QGtxI,3xEQ8gwSwY0JkzYpZWl4OjiunU"	"FRANCE                   "
"Sarabharajudara#000002906"	"498dqBD0lISHzpDOGmJf3W57mBSh woorgn"	"FRANCE                   "
"Sarabharajudara#000002924"	"6 nxmhb4Okr1CdJZPA2TaNRrLSXFfzy"	"FRANCE                   "
"Sarabharajudara#000003029"	"aWkIsIRUh3zz8LiwvImuv"	"FRANCE                   "
"Sarabharajudara#000003067"	"9EPagnou6ashdkFA"	"FRANCE                   "
"Sarabharajudara#000003086"	"EdiLbOuVZPvcIKQ 8C53GAQCRGDQEn"	"FRANCE                   "
"Sarabharajudara#000003133"	"ctd9ax8DHT93kvfF91"	"FRANCE                   "
"Sarabharajudara#000003153"	"zZjHS,4cNlNAK1KFaFTNpYh9Y5Ceb"	"FRANCE                   "
"Sarabharajudara#000003280"	"TtNwejP, 4GKXNfky9Jc,8gaGEI"	"FRANCE                   "
"Sarabharajudara#000003419"	"yt KX357gL"	"FRANCE                   "
"Sarabharajudara#000003429"	"EAn2WPCt0Glq,y6"	"FRANCE                   "
"Sarabharajudara#000003635"	"iZVQF YThR0AJ5kW8QaHZh"	"FRANCE                   "
"Sarabharajudara#000003689"	"KuH5dUsSzixv"	"FRANCE                   "
"Sarabharajudara#000003746"	"O43Nikgv5lasOik8Ez2mOt3uU"	"FRANCE                   "
"Sarabharajudara#000003796"	"gC,28F ofakz0ZdgKQ2nrW7JFO35 RJN"	"FRANCE                   "
"Sarabharajudara#000003825"	"hK1aUlbzeTz MSPwcPVyRGY"	"FRANCE                   "
"Sarabharajudara#000003836"	"tdBz4J0l7wDJJu Dej1"	"FRANCE                   "
"Sarabharajudara#000003850"	",27mYEAukUi JHLAjUTMCX3hkL8uzcq88"	"FRANCE                   "
"Sarabharajudara#000003892"	"7upn3 0JxQtolUElV7uffY"	"FRANCE                   "
"Sarabharajudara#000004072"	"lAYDI98l4wGJ98"	"FRANCE                   "
"Sarabharajudara#000004090"	"vRKDWYYcJ9xGtf4xHcWTjXW22"	"FRANCE                   "
"Sarabharajudara#000004164"	"f60HY65zdJb6eSCUYOmm"	"FRANCE                   "
"Sarabharajudara#000004552"	"eRwxvVjYTpamQHXlldIxF,q8C"	"FRANCE                   "
"Sarabharajudara#000004566"	"mAKi0qJOdVHuta0zJx3WUr4er,6QJbSrUXRFN0fN"	"FRANCE                   "
"Sarabharajudara#000004579"	"K5nhdAhx6aGpbcRNj0"	"FRANCE                   "
"Sarabharajudara#000004592"	"6eoAjyJrWXrsoJr2HelM8zc4ZV5sW,d2je"	"FRANCE                   "
"Sarabharajudara#000004597"	"gKuHIUE7XWqK9ZDCA,Kp0jFza4PvTq,RtFF"	"FRANCE                   "
"Sarabharajudara#000004746"	"HrNlq N3KfDAfcfX3uho4LqI"	"FRANCE                   "

The latest query formulated by you is:
SELECT 
    s.s_sarabharajudaranama,
    s.s_sarabharajudarathikana,
    r.r_rashtranama
FROM (
    SELECT 
        av.av_sarabharajudarakramank,
        av.av_vastukramank,
        SUM(av.av_vastusankhya) AS total_shipped
    FROM Aajnavastu av
    JOIN Vastuvivara v ON av.av_vastukramank = v.v_vastukramank
    WHERE 
        av.av_vastusamahita BETWEEN '1995-01-01' AND '1995-12-31'
        AND v.v_vastunama LIKE '%ivory%'
    GROUP BY av.av_sarabharajudarakramank, av.av_vastukramank
) shipped
JOIN (
    SELECT 
        sv.sv_sarabharajudarakramank,
        sv.sv_vastukramank,
        sv.sv_vastukanishta
    FROM Sarabharajudaravastu sv
    JOIN Vastuvivara v ON sv.sv_vastukramank = v.v_vastukramank
    WHERE 
        v.v_vastunama LIKE '%ivory%'
        AND sv.sv_vastukanishta >= 12
) stock ON shipped.av_sarabharajudarakramank = stock.sv_sarabharajudarakramank
    AND shipped.av_vastukramank = stock.sv_vastukramank
JOIN Sarabharajudara s ON stock.sv_sarabharajudarakramank = s.s_sarabharajudarakramank
JOIN Rashtra r ON s.s_rashtrakramank = r.r_rashtrakramank
WHERE 
    r.r_rashtranama = 'FRANCE'
    AND stock.sv_vastukanishta > 0.5 * shipped.total_shipped
ORDER BY s.s_sarabharajudaranama ASC;

Which gives incorrect output:
"Sarabharajudara#000000198"	"ncWe9nTBqJETno"	"FRANCE                   "
"Sarabharajudara#000000322"	"lB2qcFCrwazl7Qa"	"FRANCE                   "
"Sarabharajudara#000000322"	"lB2qcFCrwazl7Qa"	"FRANCE                   "
"Sarabharajudara#000000509"	"SF7dR8V5pK"	"FRANCE                   "
"Sarabharajudara#000000553"	"a,liVofXbCJ"	"FRANCE                   "
"Sarabharajudara#000000556"	"g3QRUaiDAI1nQQPJLJfAa9W"	"FRANCE                   "
"Sarabharajudara#000000593"	"qvlFqgoEMzzksE2uQlchYQ8V"	"FRANCE                   "
"Sarabharajudara#000000616"	"Ktao GA3 5k7oF,wkDyhc0uatR72dD65pD"	"FRANCE                   "
"Sarabharajudara#000000769"	"ak2320fUkG"	"FRANCE                   "
"Sarabharajudara#000000812"	"8qh4tezyScl5bidLAysvutB,,ZI2dn6xP"	"FRANCE                   "
"Sarabharajudara#000000839"	"1fSx9Sv6LraqnVP3u"	"FRANCE                   "
"Sarabharajudara#000000839"	"1fSx9Sv6LraqnVP3u"	"FRANCE                   "
"Sarabharajudara#000000839"	"1fSx9Sv6LraqnVP3u"	"FRANCE                   "
"Sarabharajudara#000000954"	"P3O5p UFz1QsLmZX"	"FRANCE                   "
"Sarabharajudara#000001154"	"lPDPT5D5b7u4uNLN, Rl"	"FRANCE                   "
"Sarabharajudara#000001154"	"lPDPT5D5b7u4uNLN, Rl"	"FRANCE                   "
"Sarabharajudara#000001198"	"vRfsLGzF6aE2XhsqgmJFUHGmMHepJW3X"	"FRANCE                   "
"Sarabharajudara#000001198"	"vRfsLGzF6aE2XhsqgmJFUHGmMHepJW3X"	"FRANCE                   "
"Sarabharajudara#000001285"	"6GzzLGh7I9P3LhBWnTz,L2gECjp1P1I9mq4TaaK"	"FRANCE                   "
"Sarabharajudara#000001331"	"6 n,NZ875vge3mSHRgD,"	"FRANCE                   "
"Sarabharajudara#000001383"	"HpxV1sNupK1Qe cNH0"	"FRANCE                   "
"Sarabharajudara#000001384"	"fjgJwG4DViJrxMxJbO2kS2"	"FRANCE                   "
"Sarabharajudara#000001398"	"H1l294pHv2YCA2hQztBZsLGsBmhVBRRh"	"FRANCE                   "
"Sarabharajudara#000001462"	"HgxOeUIzzWk7BTRw2ax8oHi"	"FRANCE                   "
"Sarabharajudara#000001462"	"HgxOeUIzzWk7BTRw2ax8oHi"	"FRANCE                   "
"Sarabharajudara#000001541"	"rPUV63BMAmT8Y2qhs 5Z9IT D8zjCJeBHZjW"	"FRANCE                   "
"Sarabharajudara#000001576"	"3dj4fsF5fNQ2boo1riXOA7N9t"	"FRANCE                   "
"Sarabharajudara#000001776"	"T3DN kKgRFwZQAfUuH1rAWw8qS"	"FRANCE                   "
"Sarabharajudara#000001784"	"WwxpO7ccLORAYgPyH"	"FRANCE                   "
"Sarabharajudara#000001784"	"WwxpO7ccLORAYgPyH"	"FRANCE                   "
"Sarabharajudara#000001816"	"e7vab91vLJPWxxZnewmnDBpDmxYHrb"	"FRANCE                   "
"Sarabharajudara#000001845"	"Qxx8BfLUs8c1D2umIcr"	"FRANCE                   "
"Sarabharajudara#000001866"	"gJ9bAJPfBjX0s5x9dU,qA"	"FRANCE                   "
"Sarabharajudara#000001938"	"aFMa1UzMRPAO5hsX"	"FRANCE                   "
"Sarabharajudara#000002070"	"gZ8nCVAgQIMUfoYvIaTF X"	"FRANCE                   "
"Sarabharajudara#000002162"	"6ya g3MW991n9JfhxSrvgM"	"FRANCE                   "
"Sarabharajudara#000002179"	"1bSbNinI5914UbVpjbR8"	"FRANCE                   "
"Sarabharajudara#000002202"	"l3CTXqUqnR67po0RNhF5"	"FRANCE                   "
"Sarabharajudara#000002202"	"l3CTXqUqnR67po0RNhF5"	"FRANCE                   "
"Sarabharajudara#000002268"	"1So0dHWj0xfwuNopKvDKFHlCOcL1OvgtkhhUPb"	"FRANCE                   "
"Sarabharajudara#000002319"	"3z3bTulBgv8Re30oDzKgGlZQT"	"FRANCE                   "
"Sarabharajudara#000002319"	"3z3bTulBgv8Re30oDzKgGlZQT"	"FRANCE                   "
"Sarabharajudara#000002397"	"E0b,zxlk yKgtoKg1jH,"	"FRANCE                   "
"Sarabharajudara#000002397"	"E0b,zxlk yKgtoKg1jH,"	"FRANCE                   "
"Sarabharajudara#000002548"	"UABiGgMCkyTzQnloHsNBCr6da6ITjR"	"FRANCE                   "
"Sarabharajudara#000002560"	"gC4t9RFtBMoItUG5dPD"	"FRANCE                   "
"Sarabharajudara#000002676"	"Xl4TnYEpX4JlkQh11gL8hXTYRQ1"	"FRANCE                   "
"Sarabharajudara#000002692"	"1B3q56lLAYJlOR5LGa V"	"FRANCE                   "
"Sarabharajudara#000002766"	"CPJjKybUHBxm0snUwnwWxfZZLk4sbE4JISVWhr"	"FRANCE                   "
"Sarabharajudara#000002818"	"kzzNb5Jcm9WNmB LGlHk7JgN7"	"FRANCE                   "
"Sarabharajudara#000002818"	"kzzNb5Jcm9WNmB LGlHk7JgN7"	"FRANCE                   "
"Sarabharajudara#000002831"	"8DGtt26QGtxI,3xEQ8gwSwY0JkzYpZWl4OjiunU"	"FRANCE                   "
"Sarabharajudara#000002906"	"498dqBD0lISHzpDOGmJf3W57mBSh woorgn"	"FRANCE                   "
"Sarabharajudara#000002924"	"6 nxmhb4Okr1CdJZPA2TaNRrLSXFfzy"	"FRANCE                   "
"Sarabharajudara#000002924"	"6 nxmhb4Okr1CdJZPA2TaNRrLSXFfzy"	"FRANCE                   "
"Sarabharajudara#000003029"	"aWkIsIRUh3zz8LiwvImuv"	"FRANCE                   "
"Sarabharajudara#000003067"	"9EPagnou6ashdkFA"	"FRANCE                   "
"Sarabharajudara#000003067"	"9EPagnou6ashdkFA"	"FRANCE                   "
"Sarabharajudara#000003086"	"EdiLbOuVZPvcIKQ 8C53GAQCRGDQEn"	"FRANCE                   "
"Sarabharajudara#000003133"	"ctd9ax8DHT93kvfF91"	"FRANCE                   "
"Sarabharajudara#000003153"	"zZjHS,4cNlNAK1KFaFTNpYh9Y5Ceb"	"FRANCE                   "
"Sarabharajudara#000003280"	"TtNwejP, 4GKXNfky9Jc,8gaGEI"	"FRANCE                   "
"Sarabharajudara#000003419"	"yt KX357gL"	"FRANCE                   "
"Sarabharajudara#000003429"	"EAn2WPCt0Glq,y6"	"FRANCE                   "
"Sarabharajudara#000003635"	"iZVQF YThR0AJ5kW8QaHZh"	"FRANCE                   "
"Sarabharajudara#000003689"	"KuH5dUsSzixv"	"FRANCE                   "
"Sarabharajudara#000003689"	"KuH5dUsSzixv"	"FRANCE                   "
"Sarabharajudara#000003746"	"O43Nikgv5lasOik8Ez2mOt3uU"	"FRANCE                   "
"Sarabharajudara#000003796"	"gC,28F ofakz0ZdgKQ2nrW7JFO35 RJN"	"FRANCE                   "
"Sarabharajudara#000003825"	"hK1aUlbzeTz MSPwcPVyRGY"	"FRANCE                   "
"Sarabharajudara#000003836"	"tdBz4J0l7wDJJu Dej1"	"FRANCE                   "
"Sarabharajudara#000003850"	",27mYEAukUi JHLAjUTMCX3hkL8uzcq88"	"FRANCE                   "
"Sarabharajudara#000003892"	"7upn3 0JxQtolUElV7uffY"	"FRANCE                   "
"Sarabharajudara#000004072"	"lAYDI98l4wGJ98"	"FRANCE                   "
"Sarabharajudara#000004090"	"vRKDWYYcJ9xGtf4xHcWTjXW22"	"FRANCE                   "
"Sarabharajudara#000004164"	"f60HY65zdJb6eSCUYOmm"	"FRANCE                   "
"Sarabharajudara#000004552"	"eRwxvVjYTpamQHXlldIxF,q8C"	"FRANCE                   "
"Sarabharajudara#000004566"	"mAKi0qJOdVHuta0zJx3WUr4er,6QJbSrUXRFN0fN"	"FRANCE                   "
"Sarabharajudara#000004579"	"K5nhdAhx6aGpbcRNj0"	"FRANCE                   "
"Sarabharajudara#000004592"	"6eoAjyJrWXrsoJr2HelM8zc4ZV5sW,d2je"	"FRANCE                   "
"Sarabharajudara#000004597"	"gKuHIUE7XWqK9ZDCA,Kp0jFza4PvTq,RtFF"	"FRANCE                   "
"Sarabharajudara#000004597"	"gKuHIUE7XWqK9ZDCA,Kp0jFza4PvTq,RtFF"	"FRANCE                   "
"Sarabharajudara#000004746"	"HrNlq N3KfDAfcfX3uho4LqI"	"FRANCE                   "

Fix the query.
"""

def count_tokens(text):
    encoding = tiktoken.encoding_for_model("gpt-4o")
    tokens = encoding.encode(text)
    return len(tokens)


def one_round():
    k = 2
    text = f"{text_2_sql_prompt}"
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "user",
                "content": f"{text}",
            },
        ], temperature=0, logprobs=True, top_logprobs=k, stream=False
    )

    for i in range(len(response.choices)):
        tokens = response.choices[i].logprobs.content
        for token_info in tokens:
            print(token_info.token, end="")

    c_token = count_tokens(text)
    print(f"\nToken count = {c_token}\n")


orig_out = sys.stdout
f = open('chatgpt_tpch_sql.py', 'w')
sys.stdout = f
one_round()
sys.stdout = orig_out
f.close()
