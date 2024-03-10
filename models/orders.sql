{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

with orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payments') }}

),

marketing_channel as (

    select * from {{ ref('stg_marketing_channel') }}

),
marketing_cost as (
    
    select * from {{ ref('stg_marketing_channel_cost') }}

),

marketing_data as(

    select
        mc.marketing_id,
        mc.marketing_channel,
        mc.order_id,
        mac.cost
    from
        marketing_channel as mc
    left join
        marketing_cost as mac on mc.marketing_id = mac.marketing_id
),

order_payments as (

    select
        order_id,

        {% for payment_method in payment_methods -%}
        sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount,
        {% endfor -%}

        sum(amount) as total_amount

    from payments

    group by order_id

),

final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        orders.status,

        {% for payment_method in payment_methods -%}

        order_payments.{{ payment_method }}_amount,

        {% endfor -%}

        order_payments.total_amount as amount,
        md.marketing_channel,
        md.cost

    from orders


    left join order_payments
        on orders.order_id = order_payments.order_id
    left join marketing_data as md
        on orders.order_id = md.order_id

)

select * from final
