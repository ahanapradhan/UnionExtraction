import sys

from openai import OpenAI

# gets API Key from environment variable OPENAI_API_KEY
client = OpenAI()

text_2_sql_prompt = """Give me SQL for the following text:

The query considers Lingjian of a given Pinpai 'Brand#53' and with a given Rongqi 'MED BAG' and determines the
average Shuliang of such Lingjians ordered for all Dingdan in the 7-year database.
What would be the average yearly gross undiscounted loss in revenue if orders
for these Lingjians with a Shuliang of less than 70% of this average were no longer taken?


CREATE TABLE Diqu (
    DiquJian INTEGER PRIMARY KEY,
    Mingcheng VARCHAR(25) NOT NULL,
    Beizhu VARCHAR(152)
);

CREATE TABLE Guojia (
    GuojiaJian INTEGER PRIMARY KEY,
    Mingcheng VARCHAR(25) NOT NULL,
    DiquJian INTEGER REFERENCES Diqu(DiquJian),
    Beizhu VARCHAR(152)
);

CREATE TABLE Gongyingshang (
    GongyingshangJian INTEGER PRIMARY KEY,
    Mingcheng VARCHAR(25) NOT NULL,
    Dizhi VARCHAR(40) NOT NULL,
    GuojiaJian INTEGER REFERENCES Guojia(GuojiaJian),
    Dianhua VARCHAR(15) NOT NULL,
    ZhanghuYue DECIMAL(15, 2) NOT NULL,
    Beizhu VARCHAR(101)
);

CREATE TABLE Kehu (
    KehuJian INTEGER PRIMARY KEY,
    Mingcheng VARCHAR(25) NOT NULL,
    Dizhi VARCHAR(40) NOT NULL,
    GuojiaJian INTEGER REFERENCES Guojia(GuojiaJian),
    Dianhua VARCHAR(15) NOT NULL,
    ZhanghuYue DECIMAL(15, 2) NOT NULL,
    ShichangFenqu VARCHAR(10) NOT NULL,
    Beizhu VARCHAR(117)
);

CREATE TABLE Lingjian (
    LingjianJian INTEGER PRIMARY KEY,
    Mingcheng VARCHAR(55) NOT NULL,
    Zhizaoshang VARCHAR(25) NOT NULL,
    Pinpai VARCHAR(10) NOT NULL,
    Leixing VARCHAR(25) NOT NULL,
    Chicun INTEGER NOT NULL,
    Rongqi VARCHAR(10) NOT NULL,
    LingshouJiage DECIMAL(15, 2) NOT NULL,
    Beizhu VARCHAR(23)
);

CREATE TABLE LingjianGongying (
    LingjianJian INTEGER REFERENCES Lingjian(LingjianJian),
    GongyingshangJian INTEGER REFERENCES Gongyingshang(GongyingshangJian),
    KeyongShuliang INTEGER NOT NULL,
    GongyingChengben DECIMAL(15, 2) NOT NULL,
    Beizhu VARCHAR(199),
    PRIMARY KEY (LingjianJian, GongyingshangJian)
);

CREATE TABLE Dingdan (
    DingdanJian INTEGER PRIMARY KEY,
    KehuJian INTEGER REFERENCES Kehu(KehuJian),
    DingdanZhuangtai CHAR(1) NOT NULL,
    ZongJiage DECIMAL(15, 2) NOT NULL,
    DingdanRiqi DATE NOT NULL,
    DingdanYouxianCixu VARCHAR(15) NOT NULL,
    Wenyuan VARCHAR(15) NOT NULL,
    FahuoYouxianCixu INTEGER NOT NULL,
    Beizhu VARCHAR(79)
);

CREATE TABLE DingdanXiangmu (
    DingdanJian INTEGER REFERENCES Dingdan(DingdanJian),
    LingjianJian INTEGER REFERENCES Lingjian(LingjianJian),
    GongyingshangJian INTEGER REFERENCES Gongyingshang(GongyingshangJian),
    HangLieHao INTEGER NOT NULL,
    Shuliang INTEGER NOT NULL,
    KuozhanJiage DECIMAL(15, 2) NOT NULL,
    Youhui DECIMAL(15, 2) NOT NULL,
    Shuifei DECIMAL(15, 2) NOT NULL,
    TuihuoBiaozhi CHAR(1) NOT NULL,
    HangLieZhuangtai CHAR(1) NOT NULL,
    FahuoRiqi DATE NOT NULL,
    ChengnuoRiqi DATE NOT NULL,
    ShouhuoRiqi DATE NOT NULL,
    FahuoZhiling VARCHAR(25) NOT NULL,
    FahuoFangshi VARCHAR(10) NOT NULL,
    Beizhu VARCHAR(44),
    PRIMARY KEY (DingdanJian, HangLieHao)
);


Further instructions on query formulation:
Do not use redundant join conditions.
Table DingdanXiangmu is used in the query twice. Table Lingjian is used in the query once.
No other tables are used in the query.
The output of the query:
avg_yearly: 2069328.994285714286

You formulated the following query in your first trial:
WITH AverageShuliang AS (
    SELECT 
        AVG(dx.Shuliang) AS avg_shuliang
    FROM Lingjian l
    JOIN DingdanXiangmu dx ON l.LingjianJian = dx.LingjianJian
    WHERE l.Pinpai = 'Brand#53'
      AND l.Rongqi = 'MED BAG'
),

Threshold AS (
    SELECT 
        avg_shuliang * 0.7 AS threshold
    FROM AverageShuliang
),

PotentialLoss AS (
    SELECT 
        SUM(dx.KuozhanJiage) AS total_loss
    FROM DingdanXiangmu dx
    JOIN Lingjian l ON l.LingjianJian = dx.LingjianJian
    WHERE l.Pinpai = 'Brand#53'
      AND l.Rongqi = 'MED BAG'
      AND dx.Shuliang < (SELECT threshold FROM Threshold)
)

SELECT 
    total_loss / 7 AS avg_yearly
FROM PotentialLoss;

It gives the following result:
2077424.101428571429

Results do not match. Please try fixing the query again.

You formulated the following query in your second trial:
WITH AverageShuliang AS (
    SELECT 
        AVG(dx.Shuliang) AS avg_shuliang
    FROM Lingjian l
    JOIN DingdanXiangmu dx ON l.LingjianJian = dx.LingjianJian
    WHERE l.Pinpai = 'Brand#53'
      AND l.Rongqi = 'MED BAG'
),

Threshold AS (
    SELECT 
        avg_shuliang * 0.7 AS threshold
    FROM AverageShuliang
),

PotentialLoss AS (
    SELECT 
        SUM((dx.KuozhanJiage + dx.Shuifei) * dx.Shuliang) AS total_loss
    FROM DingdanXiangmu dx
    JOIN Lingjian l ON l.LingjianJian = dx.LingjianJian
    WHERE l.Pinpai = 'Brand#53'
      AND l.Rongqi = 'MED BAG'
      AND dx.Shuliang < (SELECT threshold FROM Threshold)
)

SELECT 
    total_loss / 7 AS avg_yearly
FROM PotentialLoss;

It gives the following result:
25381003.654614285714

Results do not match. Please try fixing the query again.

You formulated the following query in your third trial:

WITH AverageShuliang AS (
    SELECT 
        AVG(dx.Shuliang) AS avg_shuliang
    FROM Lingjian l
    JOIN DingdanXiangmu dx ON l.LingjianJian = dx.LingjianJian
    WHERE l.Pinpai = 'Brand#53'
      AND l.Rongqi = 'MED BAG'
),

Threshold AS (
    SELECT 
        avg_shuliang * 0.7 AS threshold
    FROM AverageShuliang
),

PotentialLoss AS (
    SELECT 
        SUM(dx.KuozhanJiage * dx.Shuliang) AS total_loss
    FROM DingdanXiangmu dx
    JOIN Lingjian l ON l.LingjianJian = dx.LingjianJian
    WHERE l.Pinpai = 'Brand#53'
      AND l.Rongqi = 'MED BAG'
      AND dx.Shuliang < (SELECT threshold FROM Threshold)
)

SELECT 
    total_loss / 7 AS avg_yearly
FROM PotentialLoss;

It gives the following result:
24426637.395714285714
Results do not match. Please try fixing the query again.

You formulated the following query in your fourth trial:

WITH AverageShuliang AS (
    SELECT 
        AVG(dx.Shuliang) AS avg_shuliang
    FROM Lingjian l
    JOIN DingdanXiangmu dx ON l.LingjianJian = dx.LingjianJian
    WHERE l.Pinpai = 'Brand#53'
      AND l.Rongqi = 'MED BAG'
),

Threshold AS (
    SELECT 
        avg_shuliang * 0.7 AS threshold
    FROM AverageShuliang
),

PotentialLoss AS (
    SELECT 
        SUM(dx.KuozhanJiage * dx.Shuliang) AS total_loss
    FROM DingdanXiangmu dx
    JOIN Lingjian l ON l.LingjianJian = dx.LingjianJian
    WHERE l.Pinpai = 'Brand#53'
      AND l.Rongqi = 'MED BAG'
      AND dx.Shuliang < (SELECT threshold FROM Threshold)
)

SELECT 
    total_loss / 7 AS avg_yearly
FROM PotentialLoss;

It gives the following result:
24426637.395714285714
Results do not match. Please try fixing the query again.

Toy formulated the following query in your fifth trial:
WITH AverageShuliang AS (
    SELECT 
        AVG(dx.Shuliang) AS avg_shuliang
    FROM Lingjian l
    JOIN DingdanXiangmu dx ON l.LingjianJian = dx.LingjianJian
    WHERE l.Pinpai = 'Brand#53'
      AND l.Rongqi = 'MED BAG'
),

Threshold AS (
    SELECT 
        avg_shuliang * 0.7 AS threshold
    FROM AverageShuliang
),

PotentialLoss AS (
    SELECT 
        SUM((dx.KuozhanJiage - dx.Youhui) * dx.Shuliang) AS total_loss
    FROM DingdanXiangmu dx
    JOIN Lingjian l ON l.LingjianJian = dx.LingjianJian
    WHERE l.Pinpai = 'Brand#53'
      AND l.Rongqi = 'MED BAG'
      AND dx.Shuliang < (SELECT threshold FROM Threshold)
)

SELECT 
    total_loss / 7 AS avg_yearly
FROM PotentialLoss;
It gives the following result:
23216384.122700000000
Results do not match. Please try fixing the query again.

Considering the following data:
DingdanXiangmu table 286 rows:
8293	7340	3592	1	50.00	62367.00	0.07	0.06	"A"	"F"	"1992-10-01"	"1992-09-16"	"1992-10-25"	"NONE                     "	"TRUCK     "	"ke even ideas. carefully ironic p"
14598	9471	4472	3	40.00	55218.80	0.08	0.06	"N"	"O"	"1998-03-05"	"1998-03-17"	"1998-03-17"	"TAKE BACK RETURN         "	"MAIL      "	" slyly bra"
18882	4769	1020	2	39.00	65276.64	0.10	0.00	"N"	"O"	"1998-02-15"	"1998-03-18"	"1998-03-15"	"NONE                     "	"AIR       "	" blithely regular theodol"
30597	4906	4907	5	49.00	88734.10	0.05	0.07	"A"	"F"	"1992-11-29"	"1992-10-04"	"1992-12-27"	"DELIVER IN PERSON        "	"FOB       "	"ions engage afte"
36678	9471	4472	6	44.00	60740.68	0.09	0.06	"R"	"F"	"1994-03-10"	"1994-03-25"	"1994-03-15"	"COLLECT COD              "	"AIR       "	"ts are carefully agai"
37572	8393	4645	4	14.00	18219.46	0.09	0.06	"A"	"F"	"1993-08-25"	"1993-10-18"	"1993-09-21"	"DELIVER IN PERSON        "	"FOB       "	"sual sentiments. carefully bold ideas "
39809	81	1332	3	41.00	40224.28	0.06	0.04	"N"	"O"	"1998-04-20"	"1998-05-22"	"1998-05-01"	"DELIVER IN PERSON        "	"TRUCK     "	" the slowly regular "
75302	4906	4907	2	10.00	18109.00	0.03	0.06	"A"	"F"	"1994-12-13"	"1994-11-06"	"1994-12-19"	"TAKE BACK RETURN         "	"TRUCK     "	"accounts nag fluffily bold instructions."
62982	81	3832	4	23.00	22564.84	0.05	0.04	"A"	"F"	"1993-03-07"	"1992-12-31"	"1993-03-27"	"NONE                     "	"TRUCK     "	"ng the bli"
67718	9471	723	4	4.00	5521.88	0.07	0.04	"R"	"F"	"1993-06-25"	"1993-05-23"	"1993-07-02"	"TAKE BACK RETURN         "	"REG AIR   "	"pearls: furiousl"
75941	81	2582	7	18.00	17659.44	0.01	0.03	"N"	"O"	"1995-06-28"	"1995-06-25"	"1995-07-13"	"TAKE BACK RETURN         "	"SHIP      "	"ong the special, pending instructions! furi"
165600	9471	3225	4	19.00	26228.93	0.10	0.00	"A"	"F"	"1992-03-30"	"1992-04-18"	"1992-04-11"	"DELIVER IN PERSON        "	"RAIL      "	" thin accounts print."
84868	5140	1392	4	23.00	24038.22	0.09	0.04	"R"	"F"	"1992-02-14"	"1992-03-14"	"1992-03-13"	"COLLECT COD              "	"SHIP      "	"eful instructions doze above the quickly"
118946	4906	4907	6	48.00	86923.20	0.08	0.07	"N"	"O"	"1997-07-24"	"1997-08-15"	"1997-08-06"	"TAKE BACK RETURN         "	"FOB       "	"fully abov"
188998	7340	1094	2	4.00	4989.36	0.00	0.08	"N"	"O"	"1998-07-27"	"1998-07-19"	"1998-08-24"	"NONE                     "	"FOB       "	"refully. accounts affix fluffily a"
173636	9471	3225	2	32.00	44175.04	0.04	0.04	"A"	"F"	"1994-05-22"	"1994-04-26"	"1994-06-05"	"DELIVER IN PERSON        "	"TRUCK     "	"furiously ironic packages. c"
191335	81	2582	1	41.00	40224.28	0.09	0.05	"A"	"F"	"1993-02-14"	"1993-04-23"	"1993-02-28"	"COLLECT COD              "	"REG AIR   "	"lly ironic deposits must nag furiously."
258596	5140	1392	7	29.00	30309.06	0.00	0.08	"R"	"F"	"1994-11-03"	"1994-11-05"	"1994-12-03"	"DELIVER IN PERSON        "	"MAIL      "	"inal accounts. furiou"
231075	11134	2387	2	17.00	17767.21	0.04	0.03	"N"	"O"	"1996-05-27"	"1996-03-23"	"1996-05-29"	"COLLECT COD              "	"AIR       "	"s wake evenly around th"
247719	7833	1587	5	47.00	81819.01	0.01	0.03	"R"	"F"	"1992-08-06"	"1992-09-10"	"1992-08-09"	"DELIVER IN PERSON        "	"AIR       "	"ar requests"
270661	8393	2147	1	28.00	36438.92	0.00	0.00	"N"	"O"	"1997-07-19"	"1997-06-19"	"1997-07-20"	"NONE                     "	"SHIP      "	"ly ironic "
265765	11134	4891	2	5.00	5225.65	0.08	0.06	"R"	"F"	"1994-09-21"	"1994-08-07"	"1994-10-07"	"DELIVER IN PERSON        "	"TRUCK     "	"along the slyly regular packages. evenly "
266979	4906	2407	1	29.00	52516.10	0.10	0.02	"N"	"O"	"1998-03-29"	"1998-03-20"	"1998-03-30"	"TAKE BACK RETURN         "	"TRUCK     "	"l accounts boost blithel"
609505	6392	2644	2	36.00	46742.04	0.01	0.04	"A"	"F"	"1993-06-23"	"1993-06-24"	"1993-07-03"	"COLLECT COD              "	"RAIL      "	"equests haggle quickly special fo"
321059	7833	1587	5	43.00	74855.69	0.01	0.07	"N"	"O"	"1998-06-11"	"1998-07-23"	"1998-06-17"	"COLLECT COD              "	"REG AIR   "	"ns cajole slyly ev"
363174	9471	1974	5	35.00	48316.45	0.03	0.08	"R"	"F"	"1992-10-05"	"1992-10-29"	"1992-10-16"	"COLLECT COD              "	"REG AIR   "	"mptotes sleep carefully alo"
369154	4769	1020	3	50.00	83688.00	0.02	0.08	"N"	"O"	"1997-02-23"	"1997-03-18"	"1997-03-10"	"DELIVER IN PERSON        "	"TRUCK     "	"sts cajole slyly"
369346	8393	3394	2	12.00	15616.68	0.08	0.04	"N"	"O"	"1997-01-02"	"1997-02-27"	"1997-01-27"	"COLLECT COD              "	"REG AIR   "	"ggle against "
463298	5140	3894	1	20.00	20902.80	0.02	0.01	"A"	"F"	"1992-07-13"	"1992-08-21"	"1992-07-26"	"DELIVER IN PERSON        "	"AIR       "	"sits sleep slyly acr"
470145	5140	2643	3	32.00	33444.48	0.00	0.01	"N"	"O"	"1996-08-30"	"1996-09-16"	"1996-09-04"	"DELIVER IN PERSON        "	"AIR       "	"lyly regular ideas. pending, unu"
483813	7833	4085	1	50.00	87041.50	0.05	0.05	"N"	"O"	"1997-08-11"	"1997-08-08"	"1997-09-10"	"DELIVER IN PERSON        "	"AIR       "	"on the even, quick de"
503975	7340	4843	4	8.00	9978.72	0.09	0.03	"R"	"F"	"1995-03-03"	"1995-02-01"	"1995-03-04"	"COLLECT COD              "	"AIR       "	"s engage. carefully pending theodolit"
504803	11134	1135	2	34.00	35534.42	0.04	0.03	"N"	"O"	"1997-07-20"	"1997-05-29"	"1997-08-09"	"DELIVER IN PERSON        "	"RAIL      "	"ithely silent deposits wake fluffily "
511776	81	1332	5	27.00	26489.16	0.01	0.06	"N"	"F"	"1995-06-16"	"1995-07-22"	"1995-06-22"	"COLLECT COD              "	"MAIL      "	"ly even packages. blithely pending pinto "
276032	8393	896	2	14.00	18219.46	0.05	0.01	"R"	"F"	"1995-04-02"	"1995-04-24"	"1995-04-29"	"NONE                     "	"MAIL      "	"ounts after the blithely unusual ideas aff"
294021	9471	4472	3	25.00	34511.75	0.07	0.04	"N"	"O"	"1996-02-20"	"1996-01-20"	"1996-03-14"	"COLLECT COD              "	"FOB       "	"c packages against the quickly"
345313	5140	2643	5	49.00	51211.86	0.02	0.05	"A"	"F"	"1993-02-17"	"1993-02-02"	"1993-03-11"	"TAKE BACK RETURN         "	"AIR       "	"its. quickly regular platelets"
381030	5140	141	5	12.00	12541.68	0.09	0.01	"R"	"F"	"1993-11-06"	"1993-10-25"	"1993-11-07"	"NONE                     "	"RAIL      "	"ly final Tiresias. quic"
382662	9471	723	4	43.00	59360.21	0.05	0.08	"R"	"F"	"1993-12-12"	"1994-01-31"	"1994-01-05"	"DELIVER IN PERSON        "	"FOB       "	"the requests. slyly ironic theodolit"
405217	81	82	3	32.00	31394.56	0.02	0.05	"R"	"F"	"1993-11-06"	"1993-09-14"	"1993-12-03"	"COLLECT COD              "	"FOB       "	"ackages eat across the regular foxes-- furi"
405798	4769	4770	2	16.00	26780.16	0.00	0.06	"N"	"O"	"1996-02-27"	"1996-02-25"	"1996-03-22"	"NONE                     "	"FOB       "	"ainst the packages. quickly fi"
406182	4906	3657	2	2.00	3621.80	0.09	0.07	"R"	"F"	"1992-12-21"	"1992-12-24"	"1992-12-23"	"COLLECT COD              "	"TRUCK     "	"nst the bravely ironic pains integrate abov"
460035	9471	4472	2	18.00	24848.46	0.06	0.06	"A"	"F"	"1992-08-31"	"1992-06-23"	"1992-09-22"	"COLLECT COD              "	"RAIL      "	"fully-- even instructions haggle f"
621986	4769	3520	1	20.00	33475.20	0.08	0.07	"R"	"F"	"1994-09-06"	"1994-08-16"	"1994-09-08"	"TAKE BACK RETURN         "	"SHIP      "	"riously. instructio"
635205	6392	3895	2	16.00	20774.24	0.07	0.03	"N"	"O"	"1997-07-15"	"1997-06-10"	"1997-07-21"	"DELIVER IN PERSON        "	"REG AIR   "	" foxes grow. bold gifts use carefully. iron"
654784	6392	1393	4	6.00	7790.34	0.08	0.07	"R"	"F"	"1994-08-19"	"1994-09-17"	"1994-09-14"	"TAKE BACK RETURN         "	"REG AIR   "	"according to the regula"
662343	81	2582	5	24.00	23545.92	0.06	0.05	"N"	"O"	"1996-10-11"	"1996-10-12"	"1996-11-05"	"DELIVER IN PERSON        "	"TRUCK     "	"kly unusual forge"
676519	4769	4770	3	10.00	16737.60	0.05	0.02	"N"	"O"	"1996-02-28"	"1996-01-07"	"1996-03-07"	"TAKE BACK RETURN         "	"AIR       "	"the special dolphins. even asymptotes u"
674820	81	82	5	34.00	33356.72	0.02	0.03	"N"	"O"	"1997-07-18"	"1997-09-11"	"1997-08-15"	"DELIVER IN PERSON        "	"AIR       "	"e the slyly enticing excuses wake furiou"
710337	8393	2147	1	12.00	15616.68	0.06	0.00	"A"	"F"	"1992-02-23"	"1992-02-23"	"1992-02-27"	"DELIVER IN PERSON        "	"FOB       "	"the special requests. "
679620	6392	1393	2	21.00	27266.19	0.07	0.03	"A"	"F"	"1993-12-24"	"1993-11-07"	"1994-01-03"	"DELIVER IN PERSON        "	"SHIP      "	"arefully regular d"
710630	6392	3895	7	9.00	11685.51	0.08	0.04	"N"	"O"	"1998-08-29"	"1998-06-14"	"1998-09-14"	"DELIVER IN PERSON        "	"FOB       "	"leep furiously above the furiously spec"
765511	5140	2643	6	21.00	21947.94	0.00	0.07	"A"	"F"	"1994-01-17"	"1994-02-01"	"1994-02-15"	"TAKE BACK RETURN         "	"TRUCK     "	" furiously even accoun"
734150	11134	4891	5	6.00	6270.78	0.01	0.00	"N"	"O"	"1997-10-31"	"1997-11-05"	"1997-11-04"	"TAKE BACK RETURN         "	"TRUCK     "	" ironic accounts. "
735811	8393	3394	2	7.00	9109.73	0.05	0.01	"N"	"F"	"1995-06-16"	"1995-05-15"	"1995-06-29"	"DELIVER IN PERSON        "	"MAIL      "	"urious ideas hagg"
756964	9471	4472	3	22.00	30370.34	0.07	0.02	"N"	"O"	"1998-03-26"	"1998-03-24"	"1998-04-14"	"COLLECT COD              "	"MAIL      "	" cajole quickly. slyly final requests us"
757667	4769	4770	1	3.00	5021.28	0.02	0.01	"N"	"O"	"1997-03-24"	"1997-01-17"	"1997-04-17"	"TAKE BACK RETURN         "	"AIR       "	"carefully special package"
760769	11134	1135	6	23.00	24037.99	0.02	0.02	"N"	"O"	"1996-06-22"	"1996-07-02"	"1996-06-30"	"NONE                     "	"RAIL      "	"ress packages."
725094	6392	146	4	29.00	37653.31	0.00	0.00	"N"	"O"	"1997-07-29"	"1997-06-17"	"1997-08-08"	"TAKE BACK RETURN         "	"MAIL      "	"ular, final instructions wake furiously "
727972	9471	4472	1	45.00	62121.15	0.09	0.05	"R"	"F"	"1992-07-19"	"1992-06-16"	"1992-07-23"	"NONE                     "	"REG AIR   "	"ts ought to wake furiously"
730496	81	3832	2	22.00	21583.76	0.07	0.00	"R"	"F"	"1992-08-31"	"1992-06-17"	"1992-09-11"	"COLLECT COD              "	"RAIL      "	"ly special deposits. regular deposits woul"
757735	5140	3894	3	47.00	49121.58	0.03	0.06	"N"	"O"	"1998-02-13"	"1998-01-23"	"1998-02-25"	"TAKE BACK RETURN         "	"TRUCK     "	"ickly even requests are slyly. ne"
774789	6392	146	1	12.00	15580.68	0.10	0.08	"N"	"O"	"1996-10-03"	"1996-08-16"	"1996-10-15"	"COLLECT COD              "	"SHIP      "	"ve the eve"
773024	4906	1157	2	28.00	50705.20	0.02	0.04	"R"	"F"	"1992-11-30"	"1993-01-23"	"1992-12-27"	"DELIVER IN PERSON        "	"REG AIR   "	"slyly even accounts. furiously i"
778371	7340	3592	3	17.00	21204.78	0.05	0.00	"N"	"O"	"1996-10-20"	"1996-09-07"	"1996-11-03"	"NONE                     "	"SHIP      "	"c waters. furio"
786305	6392	3895	3	43.00	55830.77	0.00	0.05	"N"	"O"	"1996-08-03"	"1996-09-21"	"1996-08-18"	"TAKE BACK RETURN         "	"TRUCK     "	" slyly. slyly ironic depos"
794598	81	3832	4	35.00	34337.80	0.01	0.03	"N"	"O"	"1995-08-07"	"1995-10-08"	"1995-08-20"	"COLLECT COD              "	"RAIL      "	"the carefully spec"
814980	5140	2643	1	34.00	35534.76	0.06	0.04	"A"	"F"	"1992-06-30"	"1992-09-07"	"1992-07-24"	"NONE                     "	"MAIL      "	"ts are daringly among the furiously fina"
805575	7340	4843	2	41.00	51140.94	0.02	0.00	"N"	"O"	"1996-09-12"	"1996-08-07"	"1996-10-11"	"DELIVER IN PERSON        "	"AIR       "	"ly pending deposits ha"
811264	6392	3895	4	21.00	27266.19	0.03	0.04	"N"	"O"	"1996-03-25"	"1996-05-22"	"1996-04-23"	"DELIVER IN PERSON        "	"FOB       "	"posits integrate. "
817091	7340	1094	3	16.00	19957.44	0.04	0.02	"N"	"O"	"1996-10-20"	"1996-11-07"	"1996-11-12"	"TAKE BACK RETURN         "	"MAIL      "	"ronic packages hinder f"
849536	7340	2341	4	6.00	7484.04	0.02	0.03	"N"	"O"	"1995-11-05"	"1995-09-13"	"1995-12-03"	"COLLECT COD              "	"TRUCK     "	"jole slyly slow packa"
855843	7340	4843	1	27.00	33678.18	0.01	0.05	"R"	"F"	"1994-11-18"	"1995-01-02"	"1994-11-28"	"TAKE BACK RETURN         "	"TRUCK     "	"y pending deposits "
992227	8393	3394	1	3.00	3904.17	0.08	0.08	"N"	"O"	"1997-08-29"	"1997-08-26"	"1997-09-23"	"TAKE BACK RETURN         "	"TRUCK     "	"quests! blithely unusual accou"
858786	7340	2341	1	8.00	9978.72	0.10	0.07	"N"	"O"	"1996-02-20"	"1996-01-29"	"1996-03-16"	"COLLECT COD              "	"REG AIR   "	"ronic deposits wa"
879777	7833	2834	3	30.00	52224.90	0.00	0.06	"N"	"O"	"1997-07-11"	"1997-07-01"	"1997-07-14"	"TAKE BACK RETURN         "	"TRUCK     "	" haggle. carefully final depos"
893959	7833	2834	5	36.00	62669.88	0.04	0.00	"N"	"O"	"1998-05-13"	"1998-06-06"	"1998-06-02"	"COLLECT COD              "	"REG AIR   "	"y bold accounts are. slyly regular "
925730	7340	4843	6	34.00	42409.56	0.07	0.02	"A"	"F"	"1994-03-14"	"1994-03-23"	"1994-03-28"	"TAKE BACK RETURN         "	"TRUCK     "	"gular excuses a"
963174	81	3832	2	48.00	47091.84	0.00	0.00	"N"	"O"	"1997-12-05"	"1997-11-01"	"1997-12-27"	"COLLECT COD              "	"FOB       "	"sits sleep always. slyly express deposits "
966851	4906	4907	1	36.00	65192.40	0.10	0.06	"R"	"F"	"1993-04-30"	"1993-06-09"	"1993-05-16"	"TAKE BACK RETURN         "	"MAIL      "	"s sleep blithely express acco"
992228	4769	1020	4	2.00	3347.52	0.01	0.04	"N"	"O"	"1995-11-03"	"1996-01-07"	"1995-11-05"	"DELIVER IN PERSON        "	"TRUCK     "	"s along the regular deposits serve"
886919	81	3832	5	39.00	38262.12	0.06	0.02	"N"	"O"	"1995-06-26"	"1995-08-19"	"1995-07-14"	"COLLECT COD              "	"REG AIR   "	"aggle above the car"
937314	8393	4645	1	39.00	50754.21	0.04	0.07	"A"	"F"	"1994-03-03"	"1994-01-18"	"1994-03-28"	"COLLECT COD              "	"FOB       "	"uriously final theodolites use unusual d"
958976	4769	2270	2	4.00	6695.04	0.05	0.07	"N"	"O"	"1996-02-25"	"1996-01-05"	"1996-03-21"	"TAKE BACK RETURN         "	"RAIL      "	"nic hockey players r"
1047685	4769	3520	2	5.00	8368.80	0.02	0.07	"A"	"F"	"1994-07-06"	"1994-08-04"	"1994-07-24"	"COLLECT COD              "	"AIR       "	"ounts. requests engage eve"
1000423	81	1332	1	2.00	1962.16	0.02	0.01	"N"	"O"	"1998-07-05"	"1998-06-02"	"1998-07-20"	"NONE                     "	"AIR       "	"lithely re"
1020549	6392	1393	6	30.00	38951.70	0.10	0.01	"R"	"F"	"1994-04-13"	"1994-02-17"	"1994-05-04"	"NONE                     "	"MAIL      "	"y final ac"
1071717	81	82	3	7.00	6867.56	0.10	0.02	"R"	"F"	"1993-01-22"	"1993-02-20"	"1993-01-28"	"DELIVER IN PERSON        "	"FOB       "	"iously regular ex"
1054694	11134	4891	2	7.00	7315.91	0.07	0.01	"N"	"O"	"1997-09-11"	"1997-10-11"	"1997-10-11"	"NONE                     "	"SHIP      "	"nic deposits sleep "
1079426	11134	1135	4	20.00	20902.60	0.08	0.07	"A"	"F"	"1994-03-23"	"1994-02-16"	"1994-03-25"	"COLLECT COD              "	"FOB       "	" final, ironic packages wake blit"
1072835	11134	3639	5	13.00	13586.69	0.05	0.02	"A"	"F"	"1994-06-06"	"1994-07-26"	"1994-06-27"	"TAKE BACK RETURN         "	"FOB       "	"fter the regular, even deposits. fluffil"
1080224	5140	141	3	3.00	3135.42	0.03	0.04	"R"	"F"	"1993-02-24"	"1993-03-10"	"1993-03-08"	"DELIVER IN PERSON        "	"MAIL      "	" excuses wake. silent requests about the "
1081604	11134	1135	2	44.00	45985.72	0.01	0.02	"N"	"O"	"1996-09-22"	"1996-09-30"	"1996-10-20"	"TAKE BACK RETURN         "	"FOB       "	"xes. even, final requests cajole at th"
1091716	8393	3394	6	28.00	36438.92	0.05	0.08	"A"	"F"	"1995-05-26"	"1995-07-01"	"1995-06-08"	"DELIVER IN PERSON        "	"AIR       "	" could have to sleep: sometimes final depos"
1148423	8393	2147	3	4.00	5205.56	0.08	0.00	"N"	"O"	"1996-09-10"	"1996-10-13"	"1996-09-18"	"DELIVER IN PERSON        "	"TRUCK     "	"thely regular theodolites. f"
1144867	7340	2341	2	38.00	47398.92	0.10	0.05	"A"	"F"	"1992-08-01"	"1992-07-22"	"1992-08-25"	"DELIVER IN PERSON        "	"SHIP      "	"ole slyly regul"
1099138	9471	3225	1	12.00	16565.64	0.02	0.04	"R"	"F"	"1993-01-24"	"1993-02-14"	"1993-02-08"	"NONE                     "	"AIR       "	"g gifts integrate "
1125511	4906	3657	3	22.00	39839.80	0.09	0.01	"N"	"O"	"1997-11-16"	"1997-09-23"	"1997-11-22"	"NONE                     "	"TRUCK     "	"onic ideas. reg"
1168674	5140	2643	4	30.00	31354.20	0.09	0.01	"R"	"F"	"1993-06-04"	"1993-06-15"	"1993-06-08"	"COLLECT COD              "	"AIR       "	" furiously ironic requests nag quickly."
1169312	11134	3639	3	14.00	14631.82	0.09	0.02	"A"	"F"	"1995-01-12"	"1995-02-15"	"1995-02-06"	"NONE                     "	"AIR       "	"uickly iro"
1171808	81	82	3	16.00	15697.28	0.07	0.06	"A"	"F"	"1992-07-27"	"1992-07-04"	"1992-08-09"	"TAKE BACK RETURN         "	"FOB       "	" packages caj"
1177921	4769	4770	1	45.00	75319.20	0.09	0.04	"N"	"O"	"1995-06-24"	"1995-08-03"	"1995-06-29"	"DELIVER IN PERSON        "	"FOB       "	"ehind the express foxes are carefu"
1263395	8393	2147	1	34.00	44247.26	0.06	0.04	"R"	"F"	"1993-06-08"	"1993-07-18"	"1993-07-04"	"TAKE BACK RETURN         "	"SHIP      "	"d the pending ideas are slowly special "
1223492	4906	1157	2	46.00	83301.40	0.07	0.01	"R"	"F"	"1994-10-12"	"1994-08-13"	"1994-11-04"	"TAKE BACK RETURN         "	"SHIP      "	"s. blithely even foxes wake amo"
1227845	4906	2407	1	30.00	54327.00	0.10	0.01	"N"	"O"	"1997-08-01"	"1997-07-22"	"1997-08-28"	"COLLECT COD              "	"REG AIR   "	"ake carefully s"
1252423	8393	3394	6	48.00	62466.72	0.02	0.03	"N"	"O"	"1997-07-31"	"1997-10-24"	"1997-08-08"	"COLLECT COD              "	"MAIL      "	" across the carefully sp"
1279235	9471	3225	2	6.00	8282.82	0.10	0.08	"N"	"O"	"1996-04-14"	"1996-03-17"	"1996-04-19"	"TAKE BACK RETURN         "	"AIR       "	"ously according to the slyly express inst"
1272322	5140	3894	3	34.00	35534.76	0.10	0.02	"N"	"O"	"1995-12-01"	"1996-01-09"	"1995-12-30"	"DELIVER IN PERSON        "	"RAIL      "	"ly quickly bo"
1282403	6392	1393	2	18.00	23371.02	0.04	0.04	"A"	"F"	"1993-08-15"	"1993-08-30"	"1993-08-21"	"NONE                     "	"REG AIR   "	"ly above the furiousl"
1285956	9471	4472	7	40.00	55218.80	0.00	0.05	"A"	"F"	"1992-04-08"	"1992-02-07"	"1992-05-08"	"NONE                     "	"AIR       "	"le blithel"
1323298	5140	141	4	21.00	21947.94	0.07	0.04	"A"	"F"	"1993-07-25"	"1993-07-02"	"1993-08-15"	"COLLECT COD              "	"SHIP      "	" deposits. slyly ironic packages nag sl"
1315396	4906	1157	2	14.00	25352.60	0.02	0.01	"N"	"O"	"1997-07-29"	"1997-09-20"	"1997-08-28"	"DELIVER IN PERSON        "	"REG AIR   "	"ts. even packages cajole sly"
1315654	8393	4645	2	3.00	3904.17	0.05	0.08	"N"	"O"	"1997-06-07"	"1997-05-19"	"1997-06-08"	"COLLECT COD              "	"REG AIR   "	"final packages haggle slyly ironic package"
1319873	4769	3520	2	23.00	38496.48	0.06	0.02	"N"	"O"	"1997-01-08"	"1997-01-22"	"1997-02-07"	"NONE                     "	"FOB       "	"heodolites sl"
1294052	6392	146	1	2.00	2596.78	0.00	0.02	"N"	"O"	"1997-07-30"	"1997-09-08"	"1997-08-14"	"DELIVER IN PERSON        "	"REG AIR   "	"uriously. blithely even instructions "
1299905	5140	3894	3	20.00	20902.80	0.04	0.04	"R"	"F"	"1994-06-25"	"1994-07-09"	"1994-07-21"	"DELIVER IN PERSON        "	"MAIL      "	"posits. sl"
1311715	6392	3895	3	17.00	22072.63	0.06	0.06	"N"	"O"	"1995-10-27"	"1995-09-05"	"1995-11-09"	"COLLECT COD              "	"SHIP      "	"pecial instructions; final pack"
1340518	8393	896	6	16.00	20822.24	0.04	0.06	"A"	"F"	"1993-12-12"	"1993-12-13"	"1993-12-26"	"NONE                     "	"MAIL      "	"st carefully exp"
1327876	9471	4472	3	50.00	69023.50	0.07	0.03	"A"	"F"	"1995-04-17"	"1995-04-04"	"1995-04-26"	"NONE                     "	"RAIL      "	"ackages thrash wit"
1332387	4906	1157	2	5.00	9054.50	0.00	0.08	"N"	"O"	"1996-09-25"	"1996-10-30"	"1996-10-21"	"TAKE BACK RETURN         "	"FOB       "	"ding foxes. slyly unusual foxes are c"
1439107	4769	1020	1	27.00	45191.52	0.10	0.03	"N"	"O"	"1998-07-22"	"1998-08-26"	"1998-08-13"	"TAKE BACK RETURN         "	"TRUCK     "	". blithely regula"
1352711	9471	3225	2	23.00	31750.81	0.10	0.03	"A"	"F"	"1993-06-19"	"1993-04-26"	"1993-07-09"	"COLLECT COD              "	"RAIL      "	"about the final accounts. quickl"
1400002	9471	4472	1	43.00	59360.21	0.06	0.08	"N"	"O"	"1997-05-26"	"1997-05-06"	"1997-06-20"	"DELIVER IN PERSON        "	"SHIP      "	"old deposits. even accoun"
1401924	7833	4085	4	17.00	29594.11	0.08	0.07	"N"	"O"	"1997-08-22"	"1997-10-06"	"1997-09-11"	"COLLECT COD              "	"SHIP      "	"s cajole blithel"
1404935	4906	2407	2	3.00	5432.70	0.02	0.06	"N"	"O"	"1996-08-07"	"1996-07-17"	"1996-08-28"	"TAKE BACK RETURN         "	"REG AIR   "	"ever brave dependencies are daringly. b"
1425093	6392	146	2	40.00	51935.60	0.00	0.04	"R"	"F"	"1993-06-18"	"1993-05-08"	"1993-07-04"	"COLLECT COD              "	"RAIL      "	", special asymptotes. blithe"
1460130	5140	141	1	33.00	34489.62	0.02	0.01	"A"	"F"	"1993-12-07"	"1994-01-17"	"1993-12-16"	"COLLECT COD              "	"FOB       "	" express accounts. pending asy"
1457889	8393	2147	3	42.00	54658.38	0.00	0.07	"N"	"O"	"1997-08-05"	"1997-07-23"	"1997-08-27"	"NONE                     "	"MAIL      "	" furiously special deposits about the"
1546951	5140	2643	6	15.00	15677.10	0.08	0.00	"A"	"F"	"1992-08-16"	"1992-09-16"	"1992-09-09"	"DELIVER IN PERSON        "	"REG AIR   "	"ets. carefully pending instructions "
1468357	81	1332	1	12.00	11772.96	0.05	0.02	"N"	"O"	"1997-07-14"	"1997-08-23"	"1997-08-03"	"NONE                     "	"AIR       "	"even depths promise carefully b"
1485767	11134	4891	2	19.00	19857.47	0.08	0.00	"N"	"O"	"1995-12-07"	"1996-02-05"	"1995-12-16"	"NONE                     "	"MAIL      "	"blithely regular packag"
1492389	6392	3895	1	40.00	51935.60	0.00	0.05	"A"	"F"	"1994-02-26"	"1994-03-01"	"1994-03-15"	"DELIVER IN PERSON        "	"TRUCK     "	"aggle; quickly regular accounts a"
1494854	11134	2387	1	40.00	41805.20	0.06	0.04	"R"	"F"	"1994-07-18"	"1994-06-07"	"1994-07-20"	"NONE                     "	"SHIP      "	"gle quickly "
1508933	6392	1393	3	28.00	36354.92	0.10	0.01	"R"	"F"	"1992-08-14"	"1992-06-10"	"1992-08-20"	"DELIVER IN PERSON        "	"SHIP      "	"bout the fluffily bold e"
1514852	4769	4770	3	47.00	78666.72	0.04	0.07	"N"	"O"	"1995-11-23"	"1995-12-23"	"1995-12-07"	"TAKE BACK RETURN         "	"MAIL      "	"ys above the slyly special instru"
1524164	6392	146	1	41.00	53233.99	0.01	0.04	"N"	"O"	"1997-01-03"	"1996-11-26"	"1997-01-19"	"TAKE BACK RETURN         "	"REG AIR   "	"unts. pending,"
1501766	7833	336	3	49.00	85300.67	0.07	0.03	"A"	"F"	"1992-07-12"	"1992-07-19"	"1992-07-17"	"NONE                     "	"FOB       "	" carefully final deposits. blithely regular"
1520358	7833	2834	3	21.00	36557.43	0.10	0.02	"N"	"O"	"1996-09-25"	"1996-10-06"	"1996-10-07"	"DELIVER IN PERSON        "	"RAIL      "	"sts against the special, "
1555652	11134	3639	3	41.00	42850.33	0.01	0.03	"N"	"O"	"1996-01-29"	"1996-03-15"	"1996-02-12"	"COLLECT COD              "	"TRUCK     "	"gular, regular fox"
1550369	7340	3592	2	40.00	49893.60	0.03	0.05	"R"	"F"	"1993-10-19"	"1993-10-09"	"1993-10-21"	"NONE                     "	"AIR       "	" pending accounts cajol"
1631015	7833	4085	5	2.00	3481.66	0.03	0.08	"A"	"F"	"1994-02-09"	"1993-12-14"	"1994-02-14"	"DELIVER IN PERSON        "	"REG AIR   "	"wake. furiously special pinto beans along t"
1566310	7833	1587	3	26.00	45261.58	0.05	0.05	"A"	"F"	"1994-07-03"	"1994-09-04"	"1994-07-26"	"DELIVER IN PERSON        "	"FOB       "	"careful foxes. excuses wake always"
1573574	8393	2147	7	10.00	13013.90	0.03	0.04	"R"	"F"	"1992-03-07"	"1992-04-15"	"1992-04-04"	"COLLECT COD              "	"REG AIR   "	"tructions haggle "
1604065	7340	4843	2	45.00	56130.30	0.03	0.08	"N"	"O"	"1996-12-04"	"1996-12-27"	"1996-12-21"	"DELIVER IN PERSON        "	"FOB       "	"late blithely. ironic instructions sle"
1580320	8393	2147	5	16.00	20822.24	0.09	0.05	"A"	"F"	"1994-05-24"	"1994-03-21"	"1994-06-20"	"TAKE BACK RETURN         "	"MAIL      "	"wake never quickly even packages. quick"
1615591	7833	1587	2	35.00	60929.05	0.09	0.01	"R"	"F"	"1993-02-08"	"1993-04-22"	"1993-02-21"	"DELIVER IN PERSON        "	"RAIL      "	" excuses wake r"
1649056	5140	141	3	40.00	41805.60	0.01	0.08	"N"	"O"	"1996-02-19"	"1996-03-01"	"1996-02-29"	"TAKE BACK RETURN         "	"SHIP      "	"mes close accounts. furious"
1646146	7833	4085	3	5.00	8704.15	0.03	0.08	"N"	"O"	"1997-11-16"	"1997-11-02"	"1997-12-14"	"COLLECT COD              "	"TRUCK     "	"g carefully express deposits. ir"
1656865	4906	2407	4	25.00	45272.50	0.09	0.02	"N"	"O"	"1996-07-31"	"1996-06-22"	"1996-08-11"	"DELIVER IN PERSON        "	"RAIL      "	"e carefully at the special pinto beans."
1652070	8393	2147	4	24.00	31233.36	0.06	0.04	"R"	"F"	"1994-11-08"	"1994-10-25"	"1994-11-28"	"DELIVER IN PERSON        "	"RAIL      "	"onic instructions. fluff"
1670692	7833	336	7	2.00	3481.66	0.08	0.01	"N"	"O"	"1996-09-14"	"1996-11-10"	"1996-09-16"	"NONE                     "	"FOB       "	" nag blithely a"
1663683	81	82	1	41.00	40224.28	0.06	0.08	"A"	"F"	"1992-05-30"	"1992-07-22"	"1992-06-13"	"COLLECT COD              "	"SHIP      "	"breach fluffi"
1712993	5140	1392	1	33.00	34489.62	0.04	0.08	"N"	"O"	"1996-02-15"	"1995-12-27"	"1996-03-09"	"TAKE BACK RETURN         "	"FOB       "	"deposits must sleep"
1680676	4906	4907	1	1.00	1810.90	0.02	0.06	"A"	"F"	"1994-11-25"	"1994-10-29"	"1994-12-05"	"NONE                     "	"MAIL      "	"arefully re"
1761255	4906	2407	6	31.00	56137.90	0.01	0.08	"N"	"O"	"1997-09-22"	"1997-09-02"	"1997-10-11"	"TAKE BACK RETURN         "	"AIR       "	"ly final pi"
1758115	7833	1587	6	26.00	45261.58	0.04	0.06	"N"	"O"	"1998-02-07"	"1998-04-07"	"1998-03-06"	"DELIVER IN PERSON        "	"RAIL      "	"en waters nag bravely bold deposits? s"
1716070	11134	2387	1	14.00	14631.82	0.02	0.06	"R"	"F"	"1992-05-31"	"1992-06-29"	"1992-06-02"	"NONE                     "	"REG AIR   "	"ly express foxe"
1733859	7340	1094	1	37.00	46151.58	0.03	0.01	"R"	"F"	"1994-06-20"	"1994-06-06"	"1994-07-01"	"TAKE BACK RETURN         "	"FOB       "	"ke quickly regular escapades. bold, ev"
1748512	5140	2643	1	6.00	6270.84	0.00	0.08	"N"	"O"	"1996-06-17"	"1996-06-14"	"1996-06-26"	"TAKE BACK RETURN         "	"MAIL      "	"the careful"
1760356	7340	3592	3	49.00	61119.66	0.03	0.08	"N"	"O"	"1998-01-27"	"1998-03-10"	"1998-02-13"	"TAKE BACK RETURN         "	"AIR       "	"s wake furiously. silent, ironic deposi"
1827557	4906	2407	4	2.00	3621.80	0.10	0.03	"R"	"F"	"1993-03-05"	"1992-12-22"	"1993-03-18"	"COLLECT COD              "	"REG AIR   "	"uthlessly express r"
1771847	7833	336	3	43.00	74855.69	0.00	0.02	"A"	"F"	"1992-05-20"	"1992-05-07"	"1992-06-19"	"DELIVER IN PERSON        "	"MAIL      "	" regular, regular instructions."
1775045	4906	1157	5	48.00	86923.20	0.08	0.01	"R"	"F"	"1992-05-05"	"1992-05-29"	"1992-05-07"	"COLLECT COD              "	"SHIP      "	"e final, special notornis"
1813186	81	1332	2	24.00	23545.92	0.05	0.04	"N"	"O"	"1996-10-30"	"1996-09-10"	"1996-11-22"	"DELIVER IN PERSON        "	"MAIL      "	"lly bold epitaphs! requests haggle "
1804582	11134	2387	6	18.00	18812.34	0.04	0.07	"R"	"F"	"1994-05-14"	"1994-04-17"	"1994-05-18"	"TAKE BACK RETURN         "	"RAIL      "	"n requests. stealt"
1817089	9471	1974	5	43.00	59360.21	0.01	0.06	"R"	"F"	"1994-08-15"	"1994-10-12"	"1994-09-06"	"NONE                     "	"SHIP      "	"regular packages are qu"
1825729	6392	3895	3	4.00	5193.56	0.08	0.08	"N"	"O"	"1995-08-11"	"1995-09-20"	"1995-08-30"	"DELIVER IN PERSON        "	"TRUCK     "	"e regular warthogs wake "
1827427	4769	2270	1	7.00	11716.32	0.07	0.03	"A"	"F"	"1992-12-22"	"1993-01-24"	"1993-01-06"	"DELIVER IN PERSON        "	"FOB       "	"its among the sometimes ironic packages h"
1859780	5140	1392	1	12.00	12541.68	0.00	0.04	"R"	"F"	"1993-04-04"	"1993-05-15"	"1993-04-17"	"TAKE BACK RETURN         "	"REG AIR   "	"osits alongside of the theodolites mu"
1831239	7340	1094	2	32.00	39914.88	0.08	0.02	"N"	"O"	"1995-10-23"	"1995-12-12"	"1995-10-24"	"TAKE BACK RETURN         "	"TRUCK     "	"ts; ironic ideas are according to the blith"
1853701	4906	2407	3	49.00	88734.10	0.08	0.06	"R"	"F"	"1993-01-20"	"1993-02-27"	"1993-02-05"	"NONE                     "	"FOB       "	"the furiously final reque"
1855364	4906	3657	4	26.00	47083.40	0.08	0.02	"N"	"O"	"1996-01-19"	"1996-04-07"	"1996-01-26"	"COLLECT COD              "	"REG AIR   "	"o the sometimes final dep"
1911879	9471	723	1	18.00	24848.46	0.00	0.03	"R"	"F"	"1994-03-08"	"1993-12-29"	"1994-03-27"	"COLLECT COD              "	"REG AIR   "	"nstead of t"
1889088	5140	1392	5	43.00	44941.02	0.10	0.06	"A"	"F"	"1993-01-16"	"1992-11-16"	"1993-02-06"	"NONE                     "	"AIR       "	"osits. furiously pending foxes are fluffi"
1895111	11134	4891	3	40.00	41805.20	0.02	0.02	"A"	"F"	"1993-10-14"	"1993-11-23"	"1993-11-12"	"DELIVER IN PERSON        "	"RAIL      "	"o are quickl"
1899910	81	82	1	8.00	7848.64	0.04	0.05	"N"	"O"	"1998-07-02"	"1998-09-04"	"1998-07-23"	"COLLECT COD              "	"RAIL      "	"furiously theodolites. f"
1886113	7340	1094	3	41.00	51140.94	0.09	0.00	"A"	"F"	"1993-10-03"	"1993-11-24"	"1993-10-18"	"NONE                     "	"REG AIR   "	"unusual requests ha"
2019270	7340	1094	6	9.00	11226.06	0.08	0.07	"A"	"F"	"1992-06-27"	"1992-05-02"	"1992-07-25"	"TAKE BACK RETURN         "	"REG AIR   "	". enticing"
1938691	7833	4085	1	17.00	29594.11	0.02	0.06	"N"	"O"	"1995-11-22"	"1995-10-31"	"1995-11-30"	"COLLECT COD              "	"SHIP      "	"mptotes. slyl"
1945095	8393	2147	2	49.00	63768.11	0.04	0.00	"N"	"O"	"1996-06-29"	"1996-08-10"	"1996-07-17"	"NONE                     "	"FOB       "	"sly express requests around the caref"
1940611	7833	4085	4	39.00	67892.37	0.06	0.05	"R"	"F"	"1995-02-05"	"1995-01-05"	"1995-02-17"	"TAKE BACK RETURN         "	"SHIP      "	"ly even deposits. "
1946722	81	3832	1	41.00	40224.28	0.09	0.04	"A"	"F"	"1993-11-28"	"1993-12-02"	"1993-12-03"	"DELIVER IN PERSON        "	"TRUCK     "	"n ideas. stealthily dogg"
1984800	9471	1974	7	37.00	51077.39	0.04	0.02	"N"	"O"	"1998-07-17"	"1998-07-09"	"1998-07-28"	"COLLECT COD              "	"MAIL      "	". idly even asymptotes wake sly"
1986279	5140	3894	7	17.00	17767.38	0.01	0.02	"A"	"F"	"1995-03-29"	"1995-05-10"	"1995-03-30"	"DELIVER IN PERSON        "	"MAIL      "	"y special pa"
2040644	4769	4770	2	35.00	58581.60	0.10	0.06	"N"	"O"	"1997-09-28"	"1997-08-22"	"1997-10-05"	"NONE                     "	"MAIL      "	"ly ironic instructions: "
2028006	7833	336	6	37.00	64410.71	0.10	0.04	"A"	"F"	"1995-05-27"	"1995-06-25"	"1995-06-09"	"TAKE BACK RETURN         "	"TRUCK     "	" foxes haggle fluffily braids. special, "
2030372	9471	4472	6	9.00	12424.23	0.02	0.03	"A"	"F"	"1995-03-22"	"1995-01-13"	"1995-04-06"	"TAKE BACK RETURN         "	"REG AIR   "	"es wake above t"
2031207	5140	2643	1	28.00	29263.92	0.07	0.05	"A"	"F"	"1995-05-05"	"1995-07-25"	"1995-05-26"	"TAKE BACK RETURN         "	"TRUCK     "	"ithely according "
2050913	4906	1157	1	49.00	88734.10	0.03	0.01	"A"	"F"	"1992-07-02"	"1992-05-05"	"1992-07-24"	"NONE                     "	"MAIL      "	"ously unusual packages sleep blith"
2047141	9471	723	3	15.00	20707.05	0.04	0.03	"N"	"O"	"1996-03-03"	"1996-03-21"	"1996-03-20"	"DELIVER IN PERSON        "	"FOB       "	" pinto beans. blithely pending pack"
2065187	9471	1974	2	27.00	37272.69	0.09	0.00	"R"	"F"	"1993-10-06"	"1993-09-08"	"1993-10-11"	"COLLECT COD              "	"REG AIR   "	"ronic dugouts unwind furiously after "
2125217	7833	4085	1	18.00	31334.94	0.09	0.06	"R"	"F"	"1994-08-29"	"1994-09-19"	"1994-09-22"	"NONE                     "	"REG AIR   "	"en, ironic accounts doubt against the "
2097765	4906	1157	7	3.00	5432.70	0.09	0.06	"N"	"O"	"1995-07-09"	"1995-07-12"	"1995-07-10"	"TAKE BACK RETURN         "	"FOB       "	"ideas wake furiously quickly"
2120707	4769	1020	2	15.00	25106.40	0.05	0.02	"N"	"O"	"1996-04-13"	"1996-04-26"	"1996-04-23"	"TAKE BACK RETURN         "	"MAIL      "	"ously regular instructions. f"
2152865	9471	3225	1	7.00	9663.29	0.10	0.04	"N"	"O"	"1996-04-26"	"1996-04-10"	"1996-05-15"	"TAKE BACK RETURN         "	"TRUCK     "	"se blithely according to"
2136007	5140	3894	2	47.00	49121.58	0.07	0.02	"N"	"O"	"1995-10-28"	"1995-09-17"	"1995-11-27"	"TAKE BACK RETURN         "	"RAIL      "	"he special, fu"
2146945	4906	3657	5	38.00	68814.20	0.06	0.04	"N"	"O"	"1998-06-03"	"1998-07-09"	"1998-06-28"	"COLLECT COD              "	"RAIL      "	"quests? re"
2260801	11134	4891	4	6.00	6270.78	0.03	0.03	"A"	"F"	"1993-07-08"	"1993-07-31"	"1993-07-26"	"TAKE BACK RETURN         "	"REG AIR   "	"uthlessly regular i"
2153152	7340	3592	1	44.00	54882.96	0.06	0.02	"N"	"O"	"1998-04-11"	"1998-04-26"	"1998-05-05"	"COLLECT COD              "	"REG AIR   "	" special foxes boost furiously regular acco"
2162119	6392	2644	3	26.00	33758.14	0.04	0.01	"R"	"F"	"1993-10-11"	"1993-09-24"	"1993-10-19"	"DELIVER IN PERSON        "	"AIR       "	"ic dependencies haggle slyly "
2162752	4769	4770	4	23.00	38496.48	0.00	0.08	"N"	"O"	"1998-08-12"	"1998-08-10"	"1998-09-04"	"NONE                     "	"FOB       "	"hely ironic pa"
2165281	4769	3520	5	19.00	31801.44	0.07	0.04	"N"	"O"	"1998-06-29"	"1998-08-29"	"1998-07-08"	"DELIVER IN PERSON        "	"MAIL      "	" special foxes wake care"
2181442	6392	3895	2	5.00	6491.95	0.04	0.02	"N"	"O"	"1997-08-25"	"1997-08-02"	"1997-09-12"	"COLLECT COD              "	"FOB       "	"mong the fu"
2184705	4769	2270	5	24.00	40170.24	0.03	0.08	"N"	"O"	"1998-05-09"	"1998-06-02"	"1998-06-06"	"COLLECT COD              "	"SHIP      "	"thely regular packages doubt af"
2189668	7833	4085	5	46.00	80078.18	0.02	0.05	"R"	"F"	"1993-04-28"	"1993-05-06"	"1993-05-08"	"COLLECT COD              "	"FOB       "	"e quickly ironic accoun"
2326342	4769	1020	5	8.00	13390.08	0.09	0.01	"R"	"F"	"1992-08-07"	"1992-07-14"	"1992-08-09"	"COLLECT COD              "	"MAIL      "	"nto beans along the"
2269316	5140	2643	2	36.00	37625.04	0.01	0.07	"N"	"O"	"1995-12-21"	"1996-01-20"	"1996-01-01"	"NONE                     "	"AIR       "	"ly regular requests above th"
2270691	5140	141	2	14.00	14631.96	0.00	0.01	"N"	"O"	"1997-12-06"	"1997-12-19"	"1997-12-17"	"NONE                     "	"MAIL      "	"riously furiously regular pinto beans. bl"
2276896	6392	146	3	3.00	3895.17	0.07	0.01	"N"	"O"	"1997-05-29"	"1997-04-04"	"1997-06-24"	"COLLECT COD              "	"RAIL      "	"s breach blithely along the si"
2303750	7833	2834	2	6.00	10444.98	0.09	0.00	"N"	"O"	"1996-03-19"	"1996-05-18"	"1996-04-12"	"TAKE BACK RETURN         "	"MAIL      "	"blithely unusual theodolites sleep slyl"
2307778	8393	896	3	16.00	20822.24	0.01	0.04	"N"	"O"	"1995-08-20"	"1995-07-07"	"1995-09-06"	"DELIVER IN PERSON        "	"MAIL      "	"s are dolphins"
2308064	8393	2147	4	29.00	37740.31	0.01	0.01	"N"	"O"	"1998-10-09"	"1998-09-05"	"1998-10-27"	"DELIVER IN PERSON        "	"FOB       "	"lyly final requests cajole c"
2323973	81	82	3	19.00	18640.52	0.02	0.04	"N"	"O"	"1998-01-07"	"1998-01-13"	"1998-02-06"	"DELIVER IN PERSON        "	"RAIL      "	" blithely unusual"
2335843	81	2582	1	2.00	1962.16	0.05	0.02	"N"	"O"	"1996-09-23"	"1996-08-14"	"1996-10-10"	"DELIVER IN PERSON        "	"RAIL      "	"carefully carefully even packages. bold "
2343013	4906	3657	1	11.00	19919.90	0.07	0.04	"N"	"O"	"1997-06-10"	"1997-05-10"	"1997-07-03"	"COLLECT COD              "	"RAIL      "	" final deposits kin"
2359430	6392	146	2	12.00	15580.68	0.09	0.00	"N"	"O"	"1998-05-15"	"1998-04-27"	"1998-06-03"	"COLLECT COD              "	"SHIP      "	"special requests? carefully r"
2374279	5140	1392	2	48.00	50166.72	0.08	0.08	"R"	"F"	"1993-08-28"	"1993-06-19"	"1993-09-11"	"NONE                     "	"TRUCK     "	"sts need to ha"
2362657	8393	3394	3	7.00	9109.73	0.03	0.06	"A"	"F"	"1995-04-20"	"1995-05-19"	"1995-05-07"	"DELIVER IN PERSON        "	"FOB       "	" against the exp"
2365443	8393	2147	1	32.00	41644.48	0.05	0.00	"N"	"O"	"1997-03-11"	"1997-04-05"	"1997-04-01"	"TAKE BACK RETURN         "	"MAIL      "	"y quickly final deposi"
2410944	7340	3592	6	48.00	59872.32	0.02	0.02	"N"	"O"	"1997-09-06"	"1997-08-14"	"1997-09-07"	"NONE                     "	"AIR       "	" slyly special pinto beans cajole blithe"
2398437	9471	1974	2	21.00	28989.87	0.04	0.01	"N"	"O"	"1996-12-10"	"1996-10-15"	"1996-12-27"	"NONE                     "	"AIR       "	"carefully."
2387174	8393	896	2	16.00	20822.24	0.02	0.02	"N"	"O"	"1996-04-23"	"1996-06-16"	"1996-05-12"	"NONE                     "	"REG AIR   "	"ly fluffily regular theodolites. furious"
2426561	7340	1094	1	24.00	29936.16	0.09	0.05	"A"	"F"	"1993-11-20"	"1993-10-03"	"1993-12-15"	"DELIVER IN PERSON        "	"AIR       "	"about the deposits are furiously exp"
2413378	7833	336	3	34.00	59188.22	0.08	0.01	"N"	"O"	"1996-02-25"	"1996-04-20"	"1996-03-04"	"COLLECT COD              "	"SHIP      "	"s nag blithely across the unusual depo"
2415559	4906	3657	1	1.00	1810.90	0.09	0.03	"R"	"F"	"1993-09-29"	"1993-11-10"	"1993-10-07"	"NONE                     "	"REG AIR   "	"ffix furiously regular excuses. pin"
2431140	8393	4645	2	18.00	23425.02	0.08	0.08	"N"	"O"	"1995-07-07"	"1995-06-15"	"1995-08-06"	"TAKE BACK RETURN         "	"FOB       "	"hely final theodolites. slyl"
2432357	8393	896	1	30.00	39041.70	0.00	0.06	"N"	"O"	"1996-10-16"	"1996-08-24"	"1996-11-14"	"TAKE BACK RETURN         "	"TRUCK     "	"sly unusual ideas. fluffil"
2432647	8393	2147	2	9.00	11712.51	0.01	0.08	"N"	"O"	"1995-10-20"	"1995-10-01"	"1995-11-16"	"DELIVER IN PERSON        "	"AIR       "	"iously final ideas. e"
2490816	5140	3894	2	44.00	45986.16	0.00	0.01	"A"	"F"	"1994-01-23"	"1994-03-22"	"1994-02-15"	"COLLECT COD              "	"RAIL      "	"gainst the warhorses. even, final"
2451299	4906	4907	5	11.00	19919.90	0.07	0.06	"R"	"F"	"1994-04-01"	"1994-06-07"	"1994-04-11"	"DELIVER IN PERSON        "	"SHIP      "	"ng foxes wake among the "
2461120	11134	4891	3	28.00	29263.64	0.08	0.07	"N"	"O"	"1997-11-15"	"1997-10-12"	"1997-12-07"	"NONE                     "	"RAIL      "	"he theodolites"
2479943	7340	1094	3	7.00	8731.38	0.01	0.05	"N"	"O"	"1997-11-22"	"1997-10-22"	"1997-12-14"	"DELIVER IN PERSON        "	"TRUCK     "	"gside of the furi"
2584263	81	3832	5	23.00	22564.84	0.07	0.00	"N"	"O"	"1995-08-03"	"1995-08-23"	"1995-08-16"	"TAKE BACK RETURN         "	"FOB       "	". deposits hag"
2514851	6392	146	5	6.00	7790.34	0.09	0.04	"N"	"O"	"1998-01-14"	"1998-01-28"	"1998-01-17"	"NONE                     "	"MAIL      "	" blithely across the regular, bold de"
2576802	11134	1135	1	17.00	17767.21	0.06	0.04	"N"	"O"	"1995-08-06"	"1995-08-28"	"1995-08-27"	"DELIVER IN PERSON        "	"AIR       "	"nal foxes. carefully i"
2578023	9471	723	2	48.00	66262.56	0.08	0.07	"A"	"F"	"1992-03-25"	"1992-05-27"	"1992-04-11"	"DELIVER IN PERSON        "	"REG AIR   "	" somas. blithely ironic excus"
2578882	8393	896	1	4.00	5205.56	0.08	0.00	"N"	"O"	"1996-08-28"	"1996-08-04"	"1996-09-10"	"DELIVER IN PERSON        "	"REG AIR   "	" the special accounts: carefully pend"
2497445	9471	1974	5	4.00	5521.88	0.04	0.03	"N"	"O"	"1998-05-30"	"1998-06-03"	"1998-06-02"	"TAKE BACK RETURN         "	"REG AIR   "	"nal deposits sleep slyly acros"
2524896	4769	1020	4	30.00	50212.80	0.03	0.04	"N"	"O"	"1997-11-01"	"1997-09-09"	"1997-11-03"	"TAKE BACK RETURN         "	"AIR       "	"s. carefully ironic packages af"
2573409	7833	336	1	20.00	34816.60	0.01	0.07	"N"	"O"	"1996-10-25"	"1996-10-12"	"1996-11-23"	"COLLECT COD              "	"SHIP      "	"ic packages wake. furiously ironic a"
2583042	5140	141	3	5.00	5225.70	0.01	0.05	"N"	"O"	"1996-04-27"	"1996-05-17"	"1996-04-29"	"TAKE BACK RETURN         "	"REG AIR   "	"ial deposits. regular, regular"
2584192	81	82	2	49.00	48072.92	0.01	0.07	"R"	"F"	"1992-06-08"	"1992-07-22"	"1992-06-20"	"TAKE BACK RETURN         "	"FOB       "	"he deposits slee"
2623493	9471	723	2	20.00	27609.40	0.09	0.07	"N"	"O"	"1997-01-09"	"1997-01-30"	"1997-01-16"	"DELIVER IN PERSON        "	"FOB       "	"egular packages boost. quic"
2613120	6392	3895	6	25.00	32459.75	0.06	0.07	"N"	"O"	"1996-09-24"	"1996-10-11"	"1996-09-30"	"DELIVER IN PERSON        "	"AIR       "	"dencies are quickly against the car"
2629058	7833	4085	3	6.00	10444.98	0.10	0.08	"R"	"F"	"1994-06-13"	"1994-06-12"	"1994-06-28"	"NONE                     "	"FOB       "	"ters maintain quickly regular packages. de"
2658402	11134	4891	7	18.00	18812.34	0.07	0.04	"A"	"F"	"1994-09-15"	"1994-10-30"	"1994-10-15"	"TAKE BACK RETURN         "	"SHIP      "	"ly final asympto"
2643234	7833	4085	6	36.00	62669.88	0.06	0.02	"R"	"F"	"1993-03-23"	"1993-06-04"	"1993-04-19"	"COLLECT COD              "	"FOB       "	"furiously. special packages haggle. careful"
2701027	6392	146	2	28.00	36354.92	0.09	0.04	"N"	"O"	"1996-12-01"	"1996-12-02"	"1996-12-31"	"NONE                     "	"RAIL      "	"quests within the"
2674823	5140	141	1	42.00	43895.88	0.10	0.01	"N"	"F"	"1995-06-14"	"1995-07-20"	"1995-06-27"	"TAKE BACK RETURN         "	"MAIL      "	"atelets lose sly"
2677859	7833	4085	3	2.00	3481.66	0.02	0.02	"N"	"O"	"1997-10-08"	"1997-07-15"	"1997-10-09"	"DELIVER IN PERSON        "	"RAIL      "	"y even pack"
2688198	7340	4843	1	7.00	8731.38	0.10	0.07	"N"	"O"	"1998-07-08"	"1998-07-06"	"1998-07-16"	"NONE                     "	"TRUCK     "	"ans. quickly even do"
2692674	4769	1020	1	14.00	23432.64	0.03	0.01	"N"	"O"	"1997-07-22"	"1997-06-22"	"1997-08-13"	"DELIVER IN PERSON        "	"SHIP      "	"e against the regular"
2660226	8393	3394	1	1.00	1301.39	0.00	0.03	"A"	"F"	"1993-03-12"	"1993-02-09"	"1993-04-04"	"NONE                     "	"MAIL      "	" boost quickly whithout the special, ev"
2698759	8393	896	4	43.00	55959.77	0.05	0.04	"R"	"F"	"1993-12-13"	"1993-09-29"	"1993-12-18"	"COLLECT COD              "	"TRUCK     "	"ar requests about the ironic "
2708484	8393	2147	3	29.00	37740.31	0.09	0.07	"R"	"F"	"1992-12-29"	"1993-01-11"	"1992-12-31"	"COLLECT COD              "	"FOB       "	"nag closely blithely sp"
2715171	5140	141	2	35.00	36579.90	0.04	0.07	"A"	"F"	"1993-10-01"	"1993-10-31"	"1993-10-28"	"COLLECT COD              "	"TRUCK     "	" haggle furiously packages. "
2714371	8393	2147	1	4.00	5205.56	0.05	0.01	"N"	"O"	"1996-02-27"	"1996-01-26"	"1996-03-20"	"NONE                     "	"RAIL      "	"ular asymptotes among the stealthy packa"
2728961	4906	2407	1	16.00	28974.40	0.04	0.02	"N"	"O"	"1998-07-30"	"1998-07-26"	"1998-08-16"	"COLLECT COD              "	"AIR       "	"nts-- express deposits inte"
2716485	8393	4645	2	13.00	16918.07	0.06	0.07	"N"	"O"	"1998-05-10"	"1998-06-27"	"1998-06-09"	"TAKE BACK RETURN         "	"FOB       "	"y. pending, bold foxes throughout th"
2745378	8393	896	3	23.00	29931.97	0.08	0.00	"R"	"F"	"1993-07-10"	"1993-08-08"	"1993-07-23"	"NONE                     "	"AIR       "	"lar, final req"
2783558	4769	2270	2	1.00	1673.76	0.03	0.07	"N"	"O"	"1997-03-30"	"1997-05-13"	"1997-04-14"	"TAKE BACK RETURN         "	"SHIP      "	" ideas cajole. furiously ironic foxes a"
2775363	4769	4770	4	28.00	46865.28	0.09	0.05	"N"	"O"	"1997-03-30"	"1997-05-22"	"1997-04-14"	"DELIVER IN PERSON        "	"FOB       "	" special ideas. furiously bold pinto"
2778852	7833	336	7	41.00	71374.03	0.09	0.04	"N"	"O"	"1997-05-22"	"1997-04-15"	"1997-05-27"	"COLLECT COD              "	"MAIL      "	"the special packages. carefull"
2784803	8393	4645	1	46.00	59863.94	0.02	0.04	"N"	"O"	"1997-09-09"	"1997-08-17"	"1997-09-15"	"TAKE BACK RETURN         "	"RAIL      "	"ts. unusual instructions might nag ag"
2803745	6392	3895	4	16.00	20774.24	0.07	0.07	"N"	"O"	"1998-08-05"	"1998-07-24"	"1998-08-09"	"DELIVER IN PERSON        "	"TRUCK     "	"y bold theodolites. silently even acc"
2870660	4906	3657	1	18.00	32596.20	0.09	0.06	"A"	"F"	"1993-07-13"	"1993-08-26"	"1993-07-24"	"NONE                     "	"AIR       "	"old multipliers. blithely unu"
2842816	7340	4843	1	48.00	59872.32	0.02	0.02	"N"	"O"	"1997-12-27"	"1998-02-17"	"1998-01-21"	"NONE                     "	"TRUCK     "	" ideas integrate along the slyly "
2849571	7340	4843	1	50.00	62367.00	0.06	0.00	"N"	"O"	"1997-11-20"	"1997-09-10"	"1997-12-14"	"DELIVER IN PERSON        "	"AIR       "	"uriously according to the theodolite"
2852643	6392	1393	6	40.00	51935.60	0.10	0.01	"R"	"F"	"1992-03-08"	"1992-03-05"	"1992-04-04"	"DELIVER IN PERSON        "	"AIR       "	" unusual accounts. slyly exp"
2861122	11134	2387	4	9.00	9406.17	0.01	0.04	"N"	"O"	"1996-07-01"	"1996-08-05"	"1996-07-20"	"DELIVER IN PERSON        "	"SHIP      "	"cording to th"
2875616	4769	1020	5	46.00	76992.96	0.06	0.08	"A"	"F"	"1992-06-25"	"1992-06-24"	"1992-07-02"	"COLLECT COD              "	"SHIP      "	"arefully furiousl"
2908514	8393	896	5	26.00	33836.14	0.06	0.06	"R"	"F"	"1992-04-04"	"1992-03-03"	"1992-04-18"	"COLLECT COD              "	"TRUCK     "	"haggle about the pending dependencies"
2889539	6392	146	4	41.00	53233.99	0.03	0.08	"A"	"F"	"1994-10-20"	"1994-12-08"	"1994-10-30"	"TAKE BACK RETURN         "	"AIR       "	" deposits poach carefu"
2897509	81	82	1	9.00	8829.72	0.03	0.03	"N"	"O"	"1997-11-06"	"1997-08-22"	"1997-11-18"	"TAKE BACK RETURN         "	"FOB       "	"fix finally close pinto "
2897537	4906	3657	1	17.00	30785.30	0.01	0.07	"A"	"F"	"1994-05-30"	"1994-05-29"	"1994-05-31"	"TAKE BACK RETURN         "	"REG AIR   "	"p slyly carefully final deposits! "
2900676	6392	1393	2	29.00	37653.31	0.10	0.07	"N"	"O"	"1996-09-18"	"1996-11-14"	"1996-10-14"	"TAKE BACK RETURN         "	"TRUCK     "	"ending foxes cajole slyly across th"
2895232	6392	146	1	2.00	2596.78	0.04	0.00	"N"	"O"	"1997-04-13"	"1997-02-24"	"1997-05-07"	"DELIVER IN PERSON        "	"AIR       "	"ual deposits alongside of the furious"
2897861	6392	3895	3	49.00	63621.11	0.03	0.00	"R"	"F"	"1994-12-09"	"1994-10-10"	"1994-12-17"	"NONE                     "	"SHIP      "	". ironic packages"
2906244	4769	4770	1	24.00	40170.24	0.07	0.07	"N"	"O"	"1997-11-03"	"1997-12-14"	"1997-11-06"	"DELIVER IN PERSON        "	"FOB       "	"uiet theodol"
2923845	11134	2387	3	30.00	31353.90	0.10	0.02	"N"	"O"	"1998-07-12"	"1998-07-09"	"1998-08-07"	"TAKE BACK RETURN         "	"SHIP      "	"unts. furiously pending reques"
2971495	8393	3394	3	10.00	13013.90	0.05	0.03	"N"	"O"	"1998-04-21"	"1998-04-30"	"1998-05-03"	"TAKE BACK RETURN         "	"REG AIR   "	"furiously abo"
2936197	9471	1974	1	49.00	67643.03	0.01	0.08	"R"	"F"	"1994-04-18"	"1994-03-30"	"1994-05-04"	"TAKE BACK RETURN         "	"SHIP      "	" regular packages wake slyly. re"
2943303	4906	1157	6	36.00	65192.40	0.00	0.03	"R"	"F"	"1993-05-13"	"1993-06-29"	"1993-05-29"	"NONE                     "	"AIR       "	"ag daringly furiously unusual packages. fu"
2995168	5140	1392	2	36.00	37625.04	0.01	0.00	"R"	"F"	"1992-08-23"	"1992-07-08"	"1992-09-15"	"DELIVER IN PERSON        "	"FOB       "	"press foxes print ca"
2976449	81	2582	1	35.00	34337.80	0.00	0.07	"N"	"O"	"1996-11-06"	"1997-01-29"	"1996-11-17"	"TAKE BACK RETURN         "	"FOB       "	" carefully "
2995072	81	3832	2	38.00	37281.04	0.07	0.01	"R"	"F"	"1994-10-08"	"1994-11-07"	"1994-11-07"	"NONE                     "	"REG AIR   "	"after the regular pinto beans. s"

Lingjian table 10 rows:
81	"misty sandy cornsilk dodger blush"	"Manufacturer#5           "	"Brand#53  "	"ECONOMY BRUSHED TIN"	21	"MED BAG   "	981.08	"ove the furiou"
4769	"slate cyan lavender peru green"	"Manufacturer#5           "	"Brand#53  "	"MEDIUM PLATED COPPER"	31	"MED BAG   "	1673.76	"lets. even ideas"
4906	"mint sienna white midnight magenta"	"Manufacturer#5           "	"Brand#53  "	"LARGE PLATED COPPER"	23	"MED BAG   "	1810.90	"ckly final packag"
5140	"dodger pale deep black azure"	"Manufacturer#5           "	"Brand#53  "	"LARGE ANODIZED NICKEL"	31	"MED BAG   "	1045.14	"ts. silent, fi"
6392	"linen orchid navajo white ivory"	"Manufacturer#5           "	"Brand#53  "	"ECONOMY POLISHED COPPER"	28	"MED BAG   "	1298.39	"furiousl"
7340	"midnight orange green ivory frosted"	"Manufacturer#5           "	"Brand#53  "	"SMALL BRUSHED TIN"	40	"MED BAG   "	1247.34	"xpress packages. sile"
7833	"grey black tan lavender deep"	"Manufacturer#5           "	"Brand#53  "	"LARGE BRUSHED COPPER"	33	"MED BAG   "	1740.83	"egular deposi"
8393	"blush saddle linen slate dim"	"Manufacturer#5           "	"Brand#53  "	"PROMO PLATED NICKEL"	35	"MED BAG   "	1301.39	"about the furiously "
9471	"aquamarine white ivory red orange"	"Manufacturer#5           "	"Brand#53  "	"SMALL BRUSHED COPPER"	3	"MED BAG   "	1380.47	"ironic pinto beans"
11134	"antique pink burnished light magenta"	"Manufacturer#5           "	"Brand#53  "	"PROMO ANODIZED NICKEL"	30	"MED BAG   "	1045.13	"somas. theodoli"

The last query you produced was as follows, which produced result 2083641.598571428571:
WITH AverageShuliang AS (
    SELECT 
        AVG(dx.Shuliang) AS avg_shuliang
    FROM Lingjian l
    JOIN DingdanXiangmu dx ON l.LingjianJian = dx.LingjianJian
    WHERE l.Pinpai = 'Brand#53'
      AND l.Rongqi = 'MED BAG'
),

Threshold AS (
    SELECT 
        avg_shuliang * 0.7 AS threshold
    FROM AverageShuliang
),

PotentialLoss AS (
    SELECT 
        SUM((dx.KuozhanJiage + dx.Shuifei) * dx.Shuliang) AS total_loss
    FROM DingdanXiangmu dx
    JOIN Lingjian l ON l.LingjianJian = dx.LingjianJian
    WHERE l.Pinpai = 'Brand#53'
      AND l.Rongqi = 'MED BAG'
      AND dx.Shuliang < (SELECT threshold FROM Threshold)
)

SELECT 
    total_loss / 7 AS avg_yearly
FROM PotentialLoss;

The expected result is: 161173.847142857143

Fix the query.

The query formulated by you in your latest trial is as follows:
WITH AverageShuliang AS (
    SELECT 
        AVG(dx.Shuliang) AS avg_shuliang
    FROM Lingjian l
    JOIN DingdanXiangmu dx ON l.LingjianJian = dx.LingjianJian
    WHERE l.Pinpai = 'Brand#53'
      AND l.Rongqi = 'MED BAG'
),

Threshold AS (
    SELECT 
        avg_shuliang * 0.7 AS threshold
    FROM AverageShuliang
),

PotentialLoss AS (
    SELECT 
        SUM(dx.KuozhanJiage * dx.Shuliang) AS total_loss
    FROM DingdanXiangmu dx
    JOIN Lingjian l ON l.LingjianJian = dx.LingjianJian
    WHERE l.Pinpai = 'Brand#53'
      AND l.Rongqi = 'MED BAG'
      AND dx.Shuliang < (SELECT threshold FROM Threshold)
)

SELECT 
    total_loss / 7 AS avg_yearly
FROM PotentialLoss;

Its output on the above sample data is: 2083636.475714285714, whereas the expected output is: 161173.847142857143
Fix the query.

The query formulated by you in your latest trial is as follows:
WITH AverageShuliang AS (
    SELECT 
        AVG(dx.Shuliang) AS avg_shuliang
    FROM Lingjian l
    JOIN DingdanXiangmu dx ON l.LingjianJian = dx.LingjianJian
    WHERE l.Pinpai = 'Brand#53'
      AND l.Rongqi = 'MED BAG'
),

Threshold AS (
    SELECT 
        avg_shuliang * 0.7 AS threshold
    FROM AverageShuliang
),

PotentialLoss AS (
    SELECT 
        SUM((dx.KuozhanJiage * dx.Shuliang) - dx.Youhui) AS total_loss
    FROM DingdanXiangmu dx
    JOIN Lingjian l ON l.LingjianJian = dx.LingjianJian
    WHERE l.Pinpai = 'Brand#53'
      AND l.Rongqi = 'MED BAG'
      AND dx.Shuliang < (SELECT threshold FROM Threshold)
)

SELECT 
    total_loss / 7 AS avg_yearly
FROM PotentialLoss;

Its output on the above sample data is: 2074816.278242857143, whereas the expected output is: 161173.847142857143
Fix the query. Do not use table Lingjian twice.

"""


def one_round():
    print("----- streaming request -----")
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "user",
                "content": f"{text_2_sql_prompt}",
            },
        ], temperature=0, stream=True
    )
    for chunk in response:
        if not chunk.choices:
            continue

        print(chunk.choices[0].delta.content, end="")
        print("")


orig_out = sys.stdout
f = open('chatgpt_tpch_sql.py', 'w')
sys.stdout = f
one_round()
sys.stdout = orig_out
f.close()
