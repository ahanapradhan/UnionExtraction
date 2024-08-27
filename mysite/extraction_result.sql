
 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT cc_name, avg(cc_tax_percentage) from call_center group by cc_name order by cc_name desc limit 10;
 --- extracted query:
 Some problem while extracting from clause. Aborting!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT cc_name, avg(cc_tax_percentage) from call_center group by cc_name order by cc_name desc limit 10;
 --- extracted query:
 Some problem while extracting from clause. Aborting!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT cc_name, avg(cc_tax_percentage) from call_center group by cc_name order by cc_name desc limit 10;
 --- extracted query:
 Some problem while extracting from clause. Aborting!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT cc_name, avg(cc_tax_percentage) from call_center group by cc_name order by cc_name desc limit 10;
 --- extracted query:
 Some problem while extracting from clause. Aborting!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT cc_name, avg(cc_tax_percentage) from call_center group by cc_name order by cc_name desc limit 10;
 --- extracted query:
 Some problem in filter extraction!Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT cc_name, avg(cc_tax_percentage) from call_center group by cc_name order by cc_name desc limit 10;
 --- extracted query:
 Some problem in filter extraction!Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
    with ssr as (select s_store_id,sum(sales_price) as sales,
sum(profit) as profit,sum(return_amt) as returns,
sum(net_loss) as profit_loss 
from ( select ss_store_sk as store_sk,ss_sold_date_sk as date_sk,ss_ext_sales_price as sales_price,
ss_net_profit as profit,cast(0 as decimal(7,2)) as return_amt,cast(0 as decimal(7,2)) as net_loss 
from store_sales 

union all 
select sr_store_sk as store_sk,sr_returned_date_sk as date_sk,cast(0 as decimal(7,2)) as sales_price,
cast(0 as decimal(7,2)) as profit,sr_return_amt as return_amt,sr_net_loss as net_loss 
from store_returns ) 
salesreturns,
date_dim,
store 
where date_sk = d_date_sk and 
d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + interval '14' day) 
and store_sk = s_store_sk group by s_store_id),

csr as (select cp_catalog_page_id,sum(sales_price) as sales,sum(profit) as profit,sum(return_amt) as returns,
sum(net_loss) as profit_loss 
from ( 
select cs_catalog_page_sk as page_sk,cs_sold_date_sk as date_sk,cs_ext_sales_price as sales_price,
cs_net_profit as profit,cast(0 as decimal(7,2)) as return_amt,cast(0 as decimal(7,2)) as net_loss 
from catalog_sales 
union all 
select cr_catalog_page_sk as page_sk,cr_returned_date_sk as date_sk,cast(0 as decimal(7,2)) as sales_price,
cast(0 as decimal(7,2)) as profit,cr_return_amount as return_amt,cr_net_loss as net_loss 
from catalog_returns 
) salesreturns,
date_dim,
catalog_page 
where date_sk = d_date_sk 
and d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + interval '14' day) 
and page_sk = cp_catalog_page_sk group by cp_catalog_page_id),

wsr as (select web_site_id,sum(sales_price) as sales,sum(profit) as profit,sum(return_amt) as returns,
sum(net_loss) as profit_loss 
from ( 
select ws_web_site_sk as wsr_web_site_sk,ws_sold_date_sk as date_sk,
ws_ext_sales_price as sales_price,ws_net_profit as profit,cast(0 as decimal(7,2)) as return_amt,
cast(0 as decimal(7,2)) as net_loss 
from web_sales 
union all 
select ws_web_site_sk as wsr_web_site_sk,wr_returned_date_sk as date_sk,cast(0 as decimal(7,2)) as sales_price,
cast(0 as decimal(7,2)) as profit,wr_return_amt as return_amt,wr_net_loss as net_loss 
from web_returns 
left outer join 
web_sales on ( wr_item_sk = ws_item_sk and wr_order_number = ws_order_number) ) salesreturns,
date_dim,
web_site 
where date_sk = d_date_sk 
and d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + interval '14' day) 
and wsr_web_site_sk = web_site_sk group by web_site_id) 
select channel,id,sum(sales) as sales,sum(returns) as returns,sum(profit) as profit 
from (select 'store channel' as channel,'store' || s_store_id as id,sales,returns,
(profit - profit_loss) as profit from ssr 
union all 
select 'catalog channel' as channel,'catalog_page' || cp_catalog_page_id as id,sales,
returns,(profit - profit_loss) as profit from csr 
union all 
select 'web channel' as channel,'web_site' || web_site_id as id,sales,returns,
(profit - profit_loss) as profit from wsr ) x group by rollup (channel,id) 
order by channel,id limit 100; 
    
 --- extracted query:
 Some problem in filter extraction!Some problem in Regular mutation pipeline. Aborting extraction!current transaction is aborted, commands ignored until end of transaction block

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
    with ssr as (select s_store_id,sum(sales_price) as sales,
sum(profit) as profit,sum(return_amt) as returns,
sum(net_loss) as profit_loss 
from ( select ss_store_sk as store_sk,ss_sold_date_sk as date_sk,ss_ext_sales_price as sales_price,
ss_net_profit as profit,cast(0 as decimal(7,2)) as return_amt,cast(0 as decimal(7,2)) as net_loss 
from store_sales 

union all 
select sr_store_sk as store_sk,sr_returned_date_sk as date_sk,cast(0 as decimal(7,2)) as sales_price,
cast(0 as decimal(7,2)) as profit,sr_return_amt as return_amt,sr_net_loss as net_loss 
from store_returns ) 
salesreturns,
date_dim,
store 
where date_sk = d_date_sk and 
d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + interval '14' day) 
and store_sk = s_store_sk group by s_store_id),

csr as (select cp_catalog_page_id,sum(sales_price) as sales,sum(profit) as profit,sum(return_amt) as returns,
sum(net_loss) as profit_loss 
from ( 
select cs_catalog_page_sk as page_sk,cs_sold_date_sk as date_sk,cs_ext_sales_price as sales_price,
cs_net_profit as profit,cast(0 as decimal(7,2)) as return_amt,cast(0 as decimal(7,2)) as net_loss 
from catalog_sales 
union all 
select cr_catalog_page_sk as page_sk,cr_returned_date_sk as date_sk,cast(0 as decimal(7,2)) as sales_price,
cast(0 as decimal(7,2)) as profit,cr_return_amount as return_amt,cr_net_loss as net_loss 
from catalog_returns 
) salesreturns,
date_dim,
catalog_page 
where date_sk = d_date_sk 
and d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + interval '14' day) 
and page_sk = cp_catalog_page_sk group by cp_catalog_page_id),

wsr as (select web_site_id,sum(sales_price) as sales,sum(profit) as profit,sum(return_amt) as returns,
sum(net_loss) as profit_loss 
from ( 
select ws_web_site_sk as wsr_web_site_sk,ws_sold_date_sk as date_sk,
ws_ext_sales_price as sales_price,ws_net_profit as profit,cast(0 as decimal(7,2)) as return_amt,
cast(0 as decimal(7,2)) as net_loss 
from web_sales 
union all 
select ws_web_site_sk as wsr_web_site_sk,wr_returned_date_sk as date_sk,cast(0 as decimal(7,2)) as sales_price,
cast(0 as decimal(7,2)) as profit,wr_return_amt as return_amt,wr_net_loss as net_loss 
from web_returns 
left outer join 
web_sales on ( wr_item_sk = ws_item_sk and wr_order_number = ws_order_number) ) salesreturns,
date_dim,
web_site 
where date_sk = d_date_sk 
and d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + interval '14' day) 
and wsr_web_site_sk = web_site_sk group by web_site_id) 
select channel,id,sum(sales) as sales,sum(returns) as returns,sum(profit) as profit 
from (select 'store channel' as channel,'store' || s_store_id as id,sales,returns,
(profit - profit_loss) as profit from ssr 
union all 
select 'catalog channel' as channel,'catalog_page' || cp_catalog_page_id as id,sales,
returns,(profit - profit_loss) as profit from csr 
union all 
select 'web channel' as channel,'web_site' || web_site_id as id,sales,returns,
(profit - profit_loss) as profit from wsr ) x group by rollup (channel,id) 
order by channel,id limit 100; 
    
 --- extracted query:
 Some problem in filter extraction!Some problem in Regular mutation pipeline. Aborting extraction!current transaction is aborted, commands ignored until end of transaction block

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
    with ssr as (select s_store_id,sum(sales_price) as sales,
sum(profit) as profit,sum(return_amt) as returns,
sum(net_loss) as profit_loss 
from ( select ss_store_sk as store_sk,ss_sold_date_sk as date_sk,ss_ext_sales_price as sales_price,
ss_net_profit as profit,cast(0 as decimal(7,2)) as return_amt,cast(0 as decimal(7,2)) as net_loss 
from store_sales 

union all 
select sr_store_sk as store_sk,sr_returned_date_sk as date_sk,cast(0 as decimal(7,2)) as sales_price,
cast(0 as decimal(7,2)) as profit,sr_return_amt as return_amt,sr_net_loss as net_loss 
from store_returns ) 
salesreturns,
date_dim,
store 
where date_sk = d_date_sk and 
d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + interval '14' day) 
and store_sk = s_store_sk group by s_store_id),

csr as (select cp_catalog_page_id,sum(sales_price) as sales,sum(profit) as profit,sum(return_amt) as returns,
sum(net_loss) as profit_loss 
from ( 
select cs_catalog_page_sk as page_sk,cs_sold_date_sk as date_sk,cs_ext_sales_price as sales_price,
cs_net_profit as profit,cast(0 as decimal(7,2)) as return_amt,cast(0 as decimal(7,2)) as net_loss 
from catalog_sales 
union all 
select cr_catalog_page_sk as page_sk,cr_returned_date_sk as date_sk,cast(0 as decimal(7,2)) as sales_price,
cast(0 as decimal(7,2)) as profit,cr_return_amount as return_amt,cr_net_loss as net_loss 
from catalog_returns 
) salesreturns,
date_dim,
catalog_page 
where date_sk = d_date_sk 
and d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + interval '14' day) 
and page_sk = cp_catalog_page_sk group by cp_catalog_page_id),

wsr as (select web_site_id,sum(sales_price) as sales,sum(profit) as profit,sum(return_amt) as returns,
sum(net_loss) as profit_loss 
from ( 
select ws_web_site_sk as wsr_web_site_sk,ws_sold_date_sk as date_sk,
ws_ext_sales_price as sales_price,ws_net_profit as profit,cast(0 as decimal(7,2)) as return_amt,
cast(0 as decimal(7,2)) as net_loss 
from web_sales 
union all 
select ws_web_site_sk as wsr_web_site_sk,wr_returned_date_sk as date_sk,cast(0 as decimal(7,2)) as sales_price,
cast(0 as decimal(7,2)) as profit,wr_return_amt as return_amt,wr_net_loss as net_loss 
from web_returns 
left outer join 
web_sales on ( wr_item_sk = ws_item_sk and wr_order_number = ws_order_number) ) salesreturns,
date_dim,
web_site 
where date_sk = d_date_sk 
and d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + interval '14' day) 
and wsr_web_site_sk = web_site_sk group by web_site_id) 
select channel,id,sum(sales) as sales,sum(returns) as returns,sum(profit) as profit 
from (select 'store channel' as channel,'store' || s_store_id as id,sales,returns,
(profit - profit_loss) as profit from ssr 
union all 
select 'catalog channel' as channel,'catalog_page' || cp_catalog_page_id as id,sales,
returns,(profit - profit_loss) as profit from csr 
union all 
select 'web channel' as channel,'web_site' || web_site_id as id,sales,returns,
(profit - profit_loss) as profit from wsr ) x group by rollup (channel,id) 
order by channel,id limit 100; 
    
 --- extracted query:
 Some problem in filter extraction!Some problem in Regular mutation pipeline. Aborting extraction!current transaction is aborted, commands ignored until end of transaction block

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
    with ssr as (select s_store_id,sum(sales_price) as sales,
sum(profit) as profit,sum(return_amt) as returns,
sum(net_loss) as profit_loss 
from ( select ss_store_sk as store_sk,ss_sold_date_sk as date_sk,ss_ext_sales_price as sales_price,
ss_net_profit as profit,cast(0 as decimal(7,2)) as return_amt,cast(0 as decimal(7,2)) as net_loss 
from store_sales 

union all 
select sr_store_sk as store_sk,sr_returned_date_sk as date_sk,cast(0 as decimal(7,2)) as sales_price,
cast(0 as decimal(7,2)) as profit,sr_return_amt as return_amt,sr_net_loss as net_loss 
from store_returns ) 
salesreturns,
date_dim,
store 
where date_sk = d_date_sk and 
d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + interval '14' day) 
and store_sk = s_store_sk group by s_store_id),

csr as (select cp_catalog_page_id,sum(sales_price) as sales,sum(profit) as profit,sum(return_amt) as returns,
sum(net_loss) as profit_loss 
from ( 
select cs_catalog_page_sk as page_sk,cs_sold_date_sk as date_sk,cs_ext_sales_price as sales_price,
cs_net_profit as profit,cast(0 as decimal(7,2)) as return_amt,cast(0 as decimal(7,2)) as net_loss 
from catalog_sales 
union all 
select cr_catalog_page_sk as page_sk,cr_returned_date_sk as date_sk,cast(0 as decimal(7,2)) as sales_price,
cast(0 as decimal(7,2)) as profit,cr_return_amount as return_amt,cr_net_loss as net_loss 
from catalog_returns 
) salesreturns,
date_dim,
catalog_page 
where date_sk = d_date_sk 
and d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + interval '14' day) 
and page_sk = cp_catalog_page_sk group by cp_catalog_page_id),

wsr as (select web_site_id,sum(sales_price) as sales,sum(profit) as profit,sum(return_amt) as returns,
sum(net_loss) as profit_loss 
from ( 
select ws_web_site_sk as wsr_web_site_sk,ws_sold_date_sk as date_sk,
ws_ext_sales_price as sales_price,ws_net_profit as profit,cast(0 as decimal(7,2)) as return_amt,
cast(0 as decimal(7,2)) as net_loss 
from web_sales 
union all 
select ws_web_site_sk as wsr_web_site_sk,wr_returned_date_sk as date_sk,cast(0 as decimal(7,2)) as sales_price,
cast(0 as decimal(7,2)) as profit,wr_return_amt as return_amt,wr_net_loss as net_loss 
from web_returns 
left outer join 
web_sales on ( wr_item_sk = ws_item_sk and wr_order_number = ws_order_number) ) salesreturns,
date_dim,
web_site 
where date_sk = d_date_sk 
and d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + interval '14' day) 
and wsr_web_site_sk = web_site_sk group by web_site_id) 
select channel,id,sum(sales) as sales,sum(returns) as returns,sum(profit) as profit 
from (select 'store channel' as channel,'store' || s_store_id as id,sales,returns,
(profit - profit_loss) as profit from ssr 
union all 
select 'catalog channel' as channel,'catalog_page' || cp_catalog_page_id as id,sales,
returns,(profit - profit_loss) as profit from csr 
union all 
select 'web channel' as channel,'web_site' || web_site_id as id,sales,returns,
(profit - profit_loss) as profit from wsr ) x group by rollup (channel,id) 
order by channel,id limit 100; 
    
 --- extracted query:
 Some problem in filter extraction!Some problem in Regular mutation pipeline. Aborting extraction!current transaction is aborted, commands ignored until end of transaction block

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
    with ssr as (select s_store_id,sum(sales_price) as sales,
sum(profit) as profit,sum(return_amt) as returns,
sum(net_loss) as profit_loss 
from ( select ss_store_sk as store_sk,ss_sold_date_sk as date_sk,ss_ext_sales_price as sales_price,
ss_net_profit as profit,cast(0 as decimal(7,2)) as return_amt,cast(0 as decimal(7,2)) as net_loss 
from store_sales 

union all 
select sr_store_sk as store_sk,sr_returned_date_sk as date_sk,cast(0 as decimal(7,2)) as sales_price,
cast(0 as decimal(7,2)) as profit,sr_return_amt as return_amt,sr_net_loss as net_loss 
from store_returns ) 
salesreturns,
date_dim,
store 
where date_sk = d_date_sk and 
d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + interval '14' day) 
and store_sk = s_store_sk group by s_store_id),

csr as (select cp_catalog_page_id,sum(sales_price) as sales,sum(profit) as profit,sum(return_amt) as returns,
sum(net_loss) as profit_loss 
from ( 
select cs_catalog_page_sk as page_sk,cs_sold_date_sk as date_sk,cs_ext_sales_price as sales_price,
cs_net_profit as profit,cast(0 as decimal(7,2)) as return_amt,cast(0 as decimal(7,2)) as net_loss 
from catalog_sales 
union all 
select cr_catalog_page_sk as page_sk,cr_returned_date_sk as date_sk,cast(0 as decimal(7,2)) as sales_price,
cast(0 as decimal(7,2)) as profit,cr_return_amount as return_amt,cr_net_loss as net_loss 
from catalog_returns 
) salesreturns,
date_dim,
catalog_page 
where date_sk = d_date_sk 
and d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + interval '14' day) 
and page_sk = cp_catalog_page_sk group by cp_catalog_page_id),

wsr as (select web_site_id,sum(sales_price) as sales,sum(profit) as profit,sum(return_amt) as returns,
sum(net_loss) as profit_loss 
from ( 
select ws_web_site_sk as wsr_web_site_sk,ws_sold_date_sk as date_sk,
ws_ext_sales_price as sales_price,ws_net_profit as profit,cast(0 as decimal(7,2)) as return_amt,
cast(0 as decimal(7,2)) as net_loss 
from web_sales 
union all 
select ws_web_site_sk as wsr_web_site_sk,wr_returned_date_sk as date_sk,cast(0 as decimal(7,2)) as sales_price,
cast(0 as decimal(7,2)) as profit,wr_return_amt as return_amt,wr_net_loss as net_loss 
from web_returns 
left outer join 
web_sales on ( wr_item_sk = ws_item_sk and wr_order_number = ws_order_number) ) salesreturns,
date_dim,
web_site 
where date_sk = d_date_sk 
and d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + interval '14' day) 
and wsr_web_site_sk = web_site_sk group by web_site_id) 
select channel,id,sum(sales) as sales,sum(returns) as returns,sum(profit) as profit 
from (select 'store channel' as channel,'store' || s_store_id as id,sales,returns,
(profit - profit_loss) as profit from ssr 
union all 
select 'catalog channel' as channel,'catalog_page' || cp_catalog_page_id as id,sales,
returns,(profit - profit_loss) as profit from csr 
union all 
select 'web channel' as channel,'web_site' || web_site_id as id,sales,returns,
(profit - profit_loss) as profit from wsr ) x group by rollup (channel,id) 
order by channel,id limit 100; 
    
 --- extracted query:
 Some problem in filter extraction!Some problem in Regular mutation pipeline. Aborting extraction!Could not extract the query due to errors.
Here's what I have as a half-baked answer:
FROM(q1) = { date_dim, catalog_page, catalog_sales }, FROM(q2) = { web_sales, date_dim, web_site }, FROM(q3) = { catalog_returns, date_dim, catalog_page }, FROM(q4) = { date_dim, store, store_sales }, FROM(q5) = { date_dim, store, store_returns }

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
    select sum(cs_ext_discount_amt) as "excess_discount_amount"
	from catalog_sales, item, date_dim 
	where i_manufact_id = 722 
	and i_item_sk = cs_item_sk 
	and d_date between DATE '2002-03-09' and DATE '2002-03-09' + interval '90 days'
    and d_date_sk = cs_sold_date_sk 
	and cs_ext_discount_amt > 10 
	and d_date_sk = cs_sold_date_sk
    limit 100;
    
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
    select sum(cs_ext_discount_amt) as "excess_discount_amount"
	from catalog_sales, item, date_dim 
	where i_manufact_id = 722 
	and i_item_sk = cs_item_sk 
	and d_date between DATE '2002-03-09' and DATE '2002-03-09' + interval '90 days'
    and d_date_sk = cs_sold_date_sk 
	and cs_ext_discount_amt > 10 
	and d_date_sk = cs_sold_date_sk
    limit 100;
    
 --- extracted query:
 connectUsingParams() takes 1 positional argument but 2 were given
 --- END OF ONE EXTRACTION EXPERIMENT
