select l_orderkey, l_extendedprice
from lineitem where
l_extendedprice >= (select max(l_discount) from lineitem);