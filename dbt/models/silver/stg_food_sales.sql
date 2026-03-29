{{
    config(
        materialized = 'incremental',
        unique_key   = 'line_item_id',
        schema       = 'CORE'
    )
}}

-- Flattens food sale line items from VARIANT array
-- into one row per item so you can query individual products

with source as (

    select *
    from {{ source('raw', 'RAW_FOOD_SALE') }}

    {% if is_incremental() %}
        where LOADED_AT > (select max(LOADED_AT) from {{ this }})
    {% endif %}

),

flattened as (

    -- FLATTEN unpacks the ITEMS array into individual rows
    select
        s.EVENT_ID                                  as event_id,
        s.ORDER_ID                                  as order_id,
        s.GUEST_ID                                  as guest_id,
        lower(s.PARK)                               as park,
        lower(s.VENUE)                              as venue,
        s.TOTAL_PRICE                               as order_total,
        lower(s.PAYMENT_METHOD)                     as payment_method,
        s.EVENT_TIMESTAMP                           as event_timestamp,
        date(s.EVENT_TIMESTAMP)                     as event_date,
        hour(s.EVENT_TIMESTAMP)                     as event_hour,

        -- line item fields from the flattened array
        f.value:name::varchar                       as item_name,
        f.value:category::varchar                   as item_category,
        f.value:quantity::int                       as quantity,
        f.value:unit_price::number(10,2)            as unit_price,
        f.value:subtotal::number(10,2)              as subtotal,

        -- synthetic unique key per line item
        s.EVENT_ID || '_' || f.index::varchar       as line_item_id,
        s.LOADED_AT                                 as loaded_at

    from source s,
    lateral flatten(input => s.ITEMS) f

),

deduped as (

    select *
    from flattened

    qualify row_number() over (
        partition by line_item_id
        order by loaded_at desc
    ) = 1

)

select * from deduped
