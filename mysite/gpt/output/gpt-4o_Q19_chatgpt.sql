```sql
select
    sum(wl_extendedprice * (1 - wl_discount)) as revenue
from
    web_lineitem,
    part
where
    p_partkey = wl_partkey
    and wl_shipinstruct = 'DELIVER IN PERSON'
    and wl_shipmode = 'AIR'
    and (
        (p_brand = 'Brand#12' and p_size between 1 and 5 and wl_quantity between 1 and 11 and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'))
        or (p_brand = 'Brand#23' and p_size between 1 and 10 and wl_quantity between 10 and 20 and p_container in ('MED CASE', 'MED BOX', 'MED PACK', 'MED PKG'))
        or (p_brand = 'Brand#34' and p_size between 1 and 15 and wl_quantity between 20 and 30 and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'))
    );
```

-- Prompt Token count = 2664

