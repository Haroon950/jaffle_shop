with source as (

    select * from {{ ref('raw_marketing_channel_cost') }}

),

renamed as (

    select
        case
            when marketing_channel = 'cpc' then 1
            when marketing_channel = 'remarketing' then 2
            when marketing_channel = 'tv' then 3
            when marketing_channel = 'vg' then 4
            when marketing_channel = 'youtube' then 5
            else null
        end as marketing_id,
        marketing_channel,
        cost

    from source

)

select * from renamed