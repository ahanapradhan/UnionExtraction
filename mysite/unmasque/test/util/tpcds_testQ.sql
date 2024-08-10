(select sum(ss_net_profit + ss_coupon_amt) as total_profit, sum(ss_quantity) as total_quantity,
	ss_item_sk
from store_sales, item, date_dim
where ss_item_sk = i_item_sk
	and ss_sold_date_sk = d_date_sk
	and ss_ext_sales_price > 3999
	and ss_ext_sales_price > i_current_price
	and i_wholesale_cost > 3.00
	and d_date between date '2000-01-01' and date '2001-12-31'
	and i_size IN ('large', 'extra large')
	and i_brand LIKE 'export%'
	and i_category = 'Children'
	and d_day_name <> 'Sunday'
group by ss_item_sk
order by ss_item_sk, total_profit, total_quantity
limit 10)

UNION ALL
(select sum(ws_net_profit + ws_coupon_amt) as total_profit, sum(ws_quantity) as total_quantity,
	ws_item_sk
from web_sales, item, date_dim
where ws_item_sk = i_item_sk
	and ws_sold_date_sk = d_date_sk
	and ws_ext_sales_price > 4999
	and ws_ext_sales_price > i_current_price
	and i_wholesale_cost > 4.00
	and d_date between date '2000-01-01' and date '2001-12-31'
	and i_size IN ('large', 'extra large')
	and i_brand LIKE 'export%'
	and i_category = 'Children'
	and d_day_name <> 'Sunday'
group by ws_item_sk
order by ws_item_sk, total_profit, total_quantity
limit 10)

UNION ALL

(select sum(cs_net_profit + cs_coupon_amt) as total_profit, sum(cs_quantity) as total_quantity,
	cs_item_sk
from catalog_sales, item, date_dim
where cs_item_sk = i_item_sk
	and cs_sold_date_sk = d_date_sk
	and cs_ext_sales_price > 3990
	and cs_ext_sales_price >= i_current_price
	and i_wholesale_cost > 2.00
	and d_date between date '2000-01-01' and date '2001-12-31'
	and i_size IN ('large', 'extra large')
	and i_brand LIKE 'export%'
	and i_category = 'Children'
	and d_day_name <> 'Sunday'
group by cs_item_sk
order by cs_item_sk, total_profit, total_quantity
limit 10)
	;

