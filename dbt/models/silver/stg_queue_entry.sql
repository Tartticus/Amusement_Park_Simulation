{{
    config(
        materialized = 'incremental',
        unique_key   = 'event_id',
        schema       = 'CORE'
    )
}}

with source as (

    select *
    from {{ source('raw', 'RAW_QUEUE_ENTRY') }}

    {% if is_incremental() %}
        where LOADED_AT > (select max(LOADED_AT) from {{ this }})
    {% endif %}

),

cleaned as (

    select
        EVENT_ID                                    as event_id,
        GUEST_ID                                    as guest_id,
        lower(PARK)                                 as park,
        lower(RIDE)                                 as ride,
        WAIT_TIME_MINS                              as wait_time_mins,
        case
            when WAIT_TIME_MINS < 15  then 'short'
            when WAIT_TIME_MINS < 45  then 'medium'
            else                           'long'
        end                                         as wait_category,
        EVENT_TIMESTAMP                             as event_timestamp,
        date(EVENT_TIMESTAMP)                       as event_date,
        hour(EVENT_TIMESTAMP)                       as event_hour,
        LOADED_AT                                   as loaded_at,
        S3_FILE_NAME                                as s3_file_name

    from source

    qualify row_number() over (
        partition by EVENT_ID
        order by LOADED_AT desc
    ) = 1

)

select * from cleaned
