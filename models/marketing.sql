with marketing_channel as (

    select * from {{ ref('stg_marketing_channel') }}

),
marketing_cost as (
    
    select * from {{ ref('stg_marketing_channel_cost') }}

),
payments as (

    select * from {{ ref('stg_payments') }}

),
orders as (

    select * from {{ ref('orders') }}

),
order_payments as (

    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.status,
        p.payment_method,
        p.amount

    from orders as o
    left join payments as p on o.order_id = p.order_id
    
)

select
    mc.marketing_id,
    count(distinct mc.order_id) as total_orders,
    mc.marketing_channel,
    sum(o.cost) as total_cost,
    sum(o.amount) as total_revenue,
    round(sum(o.amount) / sum(o.cost) * 100, 1) as roi
from
    marketing_channel as mc
left join
    orders as o on mc.order_id = o.order_id
group by
    mc.marketing_id, mc.marketing_channel
