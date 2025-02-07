etpch_schema_Q13 = """CREATE TABLE VAG ( V_VAGKEY SERIAL, V_NAME VARCHAR(55), V_MFGR CHAR(25), V_BRAND CHAR(10), V_TYPE VARCHAR(25), V_SIZE INTEGER, V_CONTAINER CHAR(10), V_RETAILPRICE DECIMAL, V_COMMENT VARCHAR(23) ); 


CREATE TABLE BIKRETA ( B_BIKKEY SERIAL, B_NAME CHAR(25), B_ADDRESS VARCHAR(40), B_DESHKEY INTEGER NOT NULL, -- references D_DESHKEY B_PHONE CHAR(15), B_ACCTBAL DECIMAL, B_COMMENT VARCHAR(101) ); 


CREATE TABLE VAGBIK ( PB_VAGKEY INTEGER NOT NULL, -- references V_VAGKEY PB_BIKKEY INTEGER NOT NULL, -- references B_BIKKEY PB_AVAILQTY INTEGER, PB_BIKLYCOST DECIMAL, PB_COMMENT VARCHAR(199) ); 


CREATE TABLE KRETA ( K_CUSTKEY SERIAL, K_NAME VARCHAR(25), K_ADDRESS VARCHAR(40), K_DESHKEY INTEGER NOT NULL, -- references D_DESHKEY K_PHONE CHAR(15), K_ACCTBAL DECIMAL, K_PROKAR CHAR(10), K_COMMENT VARCHAR(117) ); 


CREATE TABLE HUKUM ( H_HUKEY SERIAL, H_CUSTKEY INTEGER NOT NULL, -- references K_CUSTKEY H_HUKUMTATUS CHAR(1), H_TOTALPRICE DECIMAL, H_HUDATE DATE, H_HUPRIORITY CHAR(15), H_CLERK CHAR(15), H_SHIPPRIORITY INTEGER, H_COMMENT VARCHAR(79) ); 


CREATE TABLE JINIS ( J_HUKEY INTEGER NOT NULL, -- references H_HUKEY J_VAGKEY INTEGER NOT NULL, -- references V_VAGKEY (compound fk to VAGBIK) J_BIKKEY INTEGER NOT NULL, -- references B_BIKKEY (compound fk to VAGBIK) J_LINENUMBER INTEGER, J_QUANTITY DECIMAL, J_EXTENDEDPRICE DECIMAL, J_DISCOUNT DECIMAL, J_TAX DECIMAL, J_RETURNFLAG CHAR(1), J_LINESTATUS CHAR(1), J_SHIPDATE DATE, J_COMMITDATE DATE, J_RECEIPTDATE DATE, J_SHIPINSTRUCT CHAR(25), J_SHIPMODE CHAR(10), J_COMMENT VARCHAR(44) ); 


CREATE TABLE DESH ( D_DESHKEY SERIAL, D_NAME CHAR(25), D_MOHADESHKEY INTEGER NOT NULL, -- references MH_MOHADESHKEY D_COMMENT VARCHAR(152) ); 


CREATE TABLE MOHADESH ( MH_MOHADESHKEY SERIAL, MH_NAME CHAR(25), MH_COMMENT VARCHAR(152) ); 
"""

Q13_text = """This query determines the distribution of kretas by the number of hukums they have made, 
including kretas who have no record of hukums, in past or present. 
It counts and reports how many kretas have no hukums, how many have 1, 2, 3, etc. 
A check is made to ensure that the hukums counted do not fall into one of several special categories of hukums. Special categories are identified in the hukums comment column by looking for a '%special%requests%' pattern. Consider the following schema while formulating the SQL: 
"""

Q13_seed = """
Select H_HUDATE as c_count, Count(*) as c_orderdate, 1 as custdist 
 From KRETA, HUKUM 
 Where KRETA.K_CUSTKEY = HUKUM.H_CUSTKEY 
 Group By H_HUDATE 
 Order By c_count desc, custdist asc; 
"""

Q13_seed_output = """	0	50005
"1995-01-13"	1	686
"1996-05-31"	1	685
"1993-03-19"	1	685
"1998-01-21"	1	683
"1995-04-13"	1	682
"1998-02-16"	1	681
"1996-12-31"	1	681
"1994-03-15"	1	681
"1996-09-23"	1	680
"1997-07-28"	1	678
"1994-09-12"	1	678
"1993-08-01"	1	678
"1992-02-24"	1	678
"1998-03-21"	1	676
"1997-08-23"	1	676
"1993-08-12"	1	676
"1993-01-22"	1	676
"1997-03-22"	1	675
"1998-01-15"	1	674
"1997-12-02"	1	674
"1997-01-05"	1	674
"1994-08-01"	1	674
"1992-10-24"	1	673
"1996-11-29"	1	672
"1996-07-26"	1	672
"1995-03-26"	1	672
"1993-01-02"	1	672
"1995-10-11"	1	671
"1994-01-02"	1	671
"1997-08-17"	1	670
"1996-07-01"	1	670
"1996-02-21"	1	670
"1995-04-21"	1	670
"1995-01-26"	1	670
"1998-06-11"	1	669
"1997-08-05"	1	669
"1996-03-23"	1	669
"1996-09-18"	1	668
"1996-06-15"	1	668
"1994-11-13"	1	668
"1997-01-22"	1	667
"1995-07-01"	1	667
"1993-09-24"	1	667
"1997-12-05"	1	666
"1997-04-29"	1	666
"1996-01-09"	1	666
"1995-11-15"	1	666
"1994-03-05"	1	666
"1992-12-03"	1	666
"1997-06-25"	1	665
"1994-12-19"	1	665
"1994-03-28"	1	665
"1994-02-08"	1	665
"1993-09-09"	1	665
"1993-04-24"	1	665
"1993-04-04"	1	665
"1992-12-05"	1	665
"1992-07-13"	1	665
"1997-12-08"	1	664
"1996-11-25"	1	664
"1995-04-23"	1	664
"1998-05-04"	1	663
"1996-05-08"	1	663
"1996-02-29"	1	663
"1995-07-16"	1	663
"1995-01-28"	1	663
"1992-06-18"	1	663
"1992-01-04"	1	663
"1998-03-29"	1	662
"1997-06-05"	1	662
"1996-06-20"	1	662
"1995-04-06"	1	662
"1995-01-30"	1	662
"1998-01-02"	1	661
"1997-03-05"	1	661
"1995-06-03"	1	661
"1994-11-14"	1	661
"1994-06-20"	1	661
"1992-05-29"	1	661
"1997-08-04"	1	660
"1995-05-04"	1	660
"1994-12-22"	1	660
"1993-09-01"	1	660
"1992-01-31"	1	660
"1998-05-26"	1	659
"1997-06-12"	1	659
"1996-09-27"	1	659
"1995-12-19"	1	659
"1995-07-26"	1	659
"1992-08-20"	1	659
"1998-05-19"	1	658
"1997-02-17"	1	658
"1996-06-30"	1	658
"1996-03-07"	1	658
"1996-01-03"	1	658
"1995-07-27"	1	658
"1993-10-18"	1	658
"1997-12-06"	1	657
"1997-05-28"	1	657"""

Q13_actual_output = """"1998-08-02"	581	1
"1998-08-01"	618	1
"1998-07-31"	621	1
"1998-07-30"	619	1
"1998-07-29"	608	1
"1998-07-28"	612	1
"1998-07-27"	602	1
"1998-07-26"	630	1
"1998-07-25"	627	1
"1998-07-24"	606	1
"1998-07-23"	640	1
"1998-07-22"	648	1
"1998-07-21"	629	1
"1998-07-20"	606	1
"1998-07-19"	638	1
"1998-07-18"	640	1
"1998-07-17"	641	1
"1998-07-16"	620	1
"1998-07-15"	639	1
"1998-07-14"	636	1
"1998-07-13"	572	1
"1998-07-12"	630	1
"1998-07-11"	668	1
"1998-07-10"	658	1
"1998-07-09"	606	1
"1998-07-08"	620	1
"1998-07-07"	627	1
"1998-07-06"	620	1
"1998-07-05"	592	1
"1998-07-04"	606	1
"1998-07-03"	645	1
"1998-07-02"	616	1
"1998-07-01"	651	1
"1998-06-30"	588	1
"1998-06-29"	629	1
"1998-06-28"	634	1
"1998-06-27"	567	1
"1998-06-26"	632	1
"1998-06-25"	620	1
"1998-06-24"	659	1
"1998-06-23"	588	1
"1998-06-22"	616	1
"1998-06-21"	639	1
"1998-06-20"	603	1
"1998-06-19"	567	1
"1998-06-18"	625	1
"1998-06-17"	626	1
"1998-06-16"	597	1
"1998-06-15"	606	1
"1998-06-14"	638	1
"1998-06-13"	634	1
"1998-06-12"	636	1
"1998-06-11"	685	1
"1998-06-10"	616	1
"1998-06-09"	597	1
"1998-06-08"	660	1
"1998-06-07"	585	1
"1998-06-06"	640	1
"1998-06-05"	620	1
"1998-06-04"	612	1
"1998-06-03"	623	1
"1998-06-02"	648	1
"1998-06-01"	600	1
"1998-05-31"	593	1
"1998-05-30"	604	1
"1998-05-29"	649	1
"1998-05-28"	656	1
"1998-05-27"	630	1
"1998-05-26"	671	1
"1998-05-25"	590	1
"1998-05-24"	636	1
"1998-05-23"	640	1
"1998-05-22"	578	1
"1998-05-21"	622	1
"1998-05-20"	643	1
"1998-05-19"	667	1
"1998-05-18"	623	1
"1998-05-17"	663	1
"1998-05-16"	605	1
"1998-05-15"	607	1
"1998-05-14"	652	1
"1998-05-13"	609	1
"1998-05-12"	631	1
"1998-05-11"	621	1
"1998-05-10"	624	1
"1998-05-09"	631	1
"1998-05-08"	590	1
"1998-05-07"	599	1
"1998-05-06"	609	1
"1998-05-05"	613	1
"1998-05-04"	670	1
"1998-05-03"	615	1
"1998-05-02"	635	1
"1998-05-01"	656	1
"1998-04-30"	654	1
"1998-04-29"	639	1
"1998-04-28"	625	1
"1998-04-27"	633	1
"1998-04-26"	611	1
"1998-04-25"	566	1"""


Q13_feedback1 = """ 
Consider the possibility of nested query.
Do not use COALESCE.
Strictly use H_HUDATE in projection and group by clause.
Strictly have 3 projections in the final query.
First projection is of date datatype.
Fix the SQL."""

Q13_feedback2 = """
Do not use COALESCE.
The query has 3 projections, with their aliases as c_count, c_orderdate and custdist. 
hukum_date (H_HUDATE) corresponds to c_count. 
kreta_count corresponds to count(*). 
Find out custdist to match the text.
Fix the SQL.
"""

Q13_feedback3 = """
Do not use COALESCE.
The query has 3 projections, with their aliases as c_count, c_orderdate and custdist. 
c_count column has values 1, 2, 3 etc in the result. 
Find out custdist to match the text description.
Fix the SQL.
"""

Q13_feedback4_sample_data = """
Do not use COALESCE.

Here is sample data from the tables. Fix the SQL to match input-output.
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

do not remove nesting of the SQL.
Fix the SQL.
"""

