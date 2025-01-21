----- streaming request -----

```
sql


SELECT
 

   
 c
.c
_c
ust
key
,
 

   
 c
.c
_name
,
 

   
 o
.o
_order
key
,
 

   
 o
.o
_total
price
,
 

   
 s
.s
_s
upp
key
,
 

   
 s
.s
_name
,
 

   
 AVG
(l
.l
_extended
price
 *
 (
1
 -
 l
.l
_discount
))
 AS
 avg
_discount
ed
_price


FROM
 

   
 customer
 c


JOIN
 

   
 orders
 o
 ON
 c
.c
_c
ust
key
 =
 o
.o
_c
ust
key


JOIN
 

   
 line
item
 l
 ON
 o
.o
_order
key
 =
 l
.l
_order
key


JOIN
 

   
 supplier
 s
 ON
 l
.l
_s
upp
key
 =
 s
.s
_s
upp
key


JOIN
 

   
 nation
 n
 ON
 s
.s
_n
ation
key
 =
 n
.n
_n
ation
key


JOIN
 

   
 region
 r
 ON
 n
.n
_region
key
 =
 r
.r
_region
key


WHERE
 

   
 c
.c
_ac
ct
bal
 >=
 o
.o
_total
price


   
 AND
 s
.s
_ac
ct
bal
 >=
 o
.o
_total
price


   
 AND
 r
.r
_name
 NOT
 IN
 ('
EU
RO
PE
')

GROUP
 BY
 

   
 c
.c
_c
ust
key
,
 c
.c
_name
,
 o
.o
_order
key
,
 o
.o
_total
price
,
 s
.s
_s
upp
key
,
 s
.s
_name


ORDER
 BY
 

   
 avg
_discount
ed
_price
 ASC
;

```
None
