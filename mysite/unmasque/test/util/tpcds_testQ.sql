-- test SQL
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

-- Himanshu Q1 Subquery
select sr_customer_sk as ctr_customer_sk,sr_store_sk as ctr_store_sk,sum(SR_FEE) as ctr_total_return 
from store_returns,date_dim 
where sr_returned_date_sk = d_date_sk and d_year =2000 group by sr_customer_sk,sr_store_sk;

-- Himanshu Q3
select dt.d_year,item.i_brand_id brand_id,item.i_brand brand,sum(ss_sales_price) sum_agg 
from date_dim dt,store_sales,item 
where dt.d_date_sk = store_sales.ss_sold_date_sk and store_sales.ss_item_sk = item.i_item_sk 
and item.i_manufact_id = 816 and dt.d_moy=11 group by dt.d_year,item.i_brand,item.i_brand_id 
order by dt.d_year,sum_agg desc,brand_id limit 100; 


-- Himanshu Q4 Subquery
select c_customer_id customer_id,c_first_name customer_first_name,c_last_name customer_last_name,
c_preferred_cust_flag customer_preferred_cust_flag,
c_birth_country customer_birth_country,
c_login customer_login,c_email_address customer_email_address,d_year dyear,
sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total,'s' sale_type 
from customer,store_sales,date_dim 
where c_customer_sk = ss_customer_sk and ss_sold_date_sk = d_date_sk 
group by c_customer_id,c_first_name,c_last_name,c_preferred_cust_flag,
c_birth_country,c_login,c_email_address,d_year 
UNION ALL 
select c_customer_id customer_id,c_first_name customer_first_name,
c_last_name customer_last_name,c_preferred_cust_flag customer_preferred_cust_flag,
c_birth_country customer_birth_country,c_login customer_login,c_email_address customer_email_address,d_year dyear,
sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2) ) year_total,
'c' sale_type 
from customer,catalog_sales,date_dim 
where c_customer_sk = cs_bill_customer_sk and cs_sold_date_sk = d_date_sk 
group by c_customer_id,c_first_name,c_last_name,c_preferred_cust_flag,c_birth_country,c_login,c_email_address,d_year 
UNION ALL 
select c_customer_id customer_id,c_first_name customer_first_name,
c_last_name customer_last_name,c_preferred_cust_flag customer_preferred_cust_flag,
c_birth_country customer_birth_country,c_login customer_login,c_email_address customer_email_address,d_year dyear,
sum((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2) ) year_total,
'w' sale_type 
from customer,web_sales,date_dim 
where c_customer_sk = ws_bill_customer_sk and ws_sold_date_sk = d_date_sk 
group by c_customer_id,c_first_name,c_last_name,c_preferred_cust_flag,c_birth_country,c_login,c_email_address,d_year


