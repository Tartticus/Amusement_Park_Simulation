{{
    config(
        materialized = 'incremental',
        unique_key   = 'event_id',
        schema       = 'CORE'
    )
}}

-- Unions online and gate ticket sales into one clean table
-- so downstream gold models don't need to join two sources

with online as (

    select
        EVENT_ID,
        ORDER_ID,
        GUEST_ID,
        PARK,
        null            as gate,
        TICKET_TYPE,
        QUANTITY,
        UNIT_PRICE,
        TOTAL_PRICE,
        PAYMENT_METHOD,
        CHANNEL,
        EVENT_TIMESTAMP,
        LOADED_AT,
        S3_FILE_NAME
    from {{ source('raw', 'RAW_TICKET_SALE_ONLINE') }}

    {% if is_incremental() %}
        where LOADED_AT > (select max(LOADED_AT) from {{ this }})
    {% endif %}

),

gate as (

    select
        EVENT_ID,
        ORDER_ID,
        GUEST_ID,
        PARK,
        GATE            as gate,
        TICKET_TYPE,
        QUANTITY,
        UNIT_PRICE,
        TOTAL_PRICE,
        PAYMENT_METHOD,
        CHANNEL,
        EVENT_TIMESTAMP,
        LOADED_AT,
        S3_FILE_NAME
    from {{ source('raw', 'RAW_TICKET_SALE_GATE') }}

    {% if is_incremental() %}
        where LOADED_AT > (select max(LOADED_AT) from {{ this }})
    {% endif %}

),

combined as (

    select * from online
    union all
    select * from gate

),

cleaned as (

    select
        EVENT_ID                                    as event_id,
        ORDER_ID                                    as order_id,
        GUEST_ID                                    as guest_id,
        lower(PARK)                                 as park,
        lower(gate)                                 as gate,
        lower(TICKET_TYPE)                          as ticket_type,
        QUANTITY                                    as quantity,
        UNIT_PRICE                                  as unit_price,
        TOTAL_PRICE                                 as total_price,
        lower(PAYMENT_METHOD)                       as payment_method,
        lower(CHANNEL)                              as channel,
        EVENT_TIMESTAMP                             as event_timestamp,
        date(EVENT_TIMESTAMP)                       as event_date,
        hour(EVENT_TIMESTAMP)                       as event_hour,
        LOADED_AT                                   as loaded_at,
        S3_FILE_NAME                                as s3_file_name

    from combined

    qualify row_number() over (
        partition by EVENT_ID
        order by LOADED_AT desc
    ) = 1

)

select * from cleaned
