create database olist_db;
use olist_db;


#########################################################
create table customers (
    customer_id               varchar(50) primary key,
    customer_unique_id        varchar(50),
    customer_zip_code_prefix  int,
    customer_city             varchar(100),
    customer_state            char(2)
);


#########################################################
create table orders (
    order_id                         varchar(50) primary key,
    customer_id                      varchar(50),
    order_status                     varchar(20),
    order_purchase_timestamp         datetime,
    order_approved_at                datetime,
    order_delivered_carrier_date     datetime,
    order_delivered_customer_date    datetime,
    order_estimated_delivery_date    datetime,
    constraint fk_orders_customer
        foreign key (customer_id) references customers(customer_id)
);


#########################################################
create table order_items (
    order_id            varchar(50),
    order_item_id       int,
    product_id          varchar(50),
    seller_id           varchar(50),
    shipping_limit_date datetime,
    price               decimal(10,2),
    freight_value       decimal(10,2),
    primary key (order_id, order_item_id)
);


#########################################################
create table order_payments (
    order_id             varchar(50),
    payment_sequential   int,
    payment_type         varchar(20),
    payment_installments int,
    payment_value        decimal(10,2),
    primary key (order_id, payment_sequential)
);


#########################################################
create table products (
    product_id                 varchar(50) primary key,
    product_category_name      varchar(100),
    product_name_lenght        int,
    product_description_lenght int,
    product_photos_qty         int,
    product_weight_g           int,
    product_length_cm          int,
    product_height_cm          int,
    product_width_cm           int
);

#########################################################
create table sellers (
    seller_id              varchar(50) primary key,
    seller_zip_code_prefix int,
    seller_city            varchar(100),
    seller_state           char(2)
);


#########################################################
create table fact_order_items_agg as
select
    oi.order_id,
    count(*)                     as item_count,
    sum(oi.price)                as total_price,
    sum(oi.freight_value)        as total_freight,
    min(p.product_category_name) as main_product_category
from order_items oi
left join products p
       on oi.product_id = p.product_id
group by oi.order_id;


#########################################################
create table fact_order_payments_agg as
select
    op.order_id,
    sum(op.payment_value) as total_payment_value,
    substring_index(
        group_concat(op.payment_type order by op.payment_value desc),
        ',',
        1
    ) as main_payment_type
from order_payments op
group by op.order_id;




#########################################################
create table fact_orders_enriched as
select
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    i.item_count,
    i.total_price,
    i.total_freight,
    i.main_product_category,
    p.total_payment_value,
    p.main_payment_type,

    timestampdiff(
        day,
        o.order_purchase_timestamp,
        o.order_delivered_customer_date
    ) as delivery_days,

    timestampdiff(
        day,
        o.order_purchase_timestamp,
        o.order_estimated_delivery_date
    ) as estimated_days,

    timestampdiff(
        day,
        o.order_estimated_delivery_date,
        o.order_delivered_customer_date
    ) as delay_days,

    case 
        when o.order_delivered_customer_date > o.order_estimated_delivery_date
        then 1
        else 0
    end as is_late,

    case 
        when i.total_price > 0 then i.total_freight / i.total_price
        else null
    end as freight_ratio
from orders o
left join fact_order_items_agg    i on o.order_id = i.order_id
left join fact_order_payments_agg p on o.order_id = p.order_id
left join customers               c on o.customer_id = c.customer_id
where o.order_status = 'delivered'
  and o.order_delivered_customer_date is not null
  and o.order_estimated_delivery_date is not null;
  
  
  
  
#########################################################
  
create or replace view monthly_kpi as
select
    date_format(order_purchase_timestamp, '%Y-%m') as order_month,
    count(*)                                       as order_count,
    sum(total_price)                               as gmv,
    avg(total_price)                               as avg_order_value
from fact_orders_enriched
group by date_format(order_purchase_timestamp, '%Y-%m')
order by order_month;

select * from monthly_kpi;


#########################################################

create or replace view state_delivery_kpi as
select
    customer_state,
    count(*)                   as order_count,
    avg(delivery_days)         as avg_delivery_days,
    avg(is_late)               as late_rate
from fact_orders_enriched
group by customer_state
having order_count >= 100
order by late_rate desc;

select * from state_delivery_kpi;


#########################################################

create or replace view category_kpi as
select
    main_product_category,
    count(*)           as order_count,
    sum(total_price)   as gmv,
    avg(freight_ratio) as avg_freight_ratio
from fact_orders_enriched
group by main_product_category
having gmv is not null
order by gmv desc
limit 20;

select * from category_kpi;

#########################################################

create or replace view payment_kpi as
select
    main_payment_type,
    count(*)         as order_count,
    sum(total_price) as gmv,
    avg(total_price) as avg_order_value
from fact_orders_enriched
group by main_payment_type
order by gmv desc;

select * from payment_kpi;

#########################################################

create table customer_orders_summary as
select
    customer_unique_id,
    count(*)                            as order_count,
    min(order_purchase_timestamp)       as first_order_date,
    max(order_purchase_timestamp)       as last_order_date
from fact_orders_enriched
group by customer_unique_id;

select
    count(*)                                             as customer_cnt,
    sum(case when order_count > 1 then 1 else 0 end)     as repeat_customer_cnt,
    sum(case when order_count > 1 then 1 else 0 end) / count(*) as repeat_customer_rate
from customer_orders_summary;

select
    order_count,
    count(*) as customer_cnt
from customer_orders_summary
group by order_count
order by order_count;






















