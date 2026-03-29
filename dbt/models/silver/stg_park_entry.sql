{{
    config(
        materialized = 'incremental',
        unique_key   = 'event_id',
        schema       = 'CORE'
    )
}}
 
with source as (
 
    select *
    from {{ source('raw', 'RAW_PARK_ENTRY') }}
 
    {% if is_incremental() %}
        -- only process rows loaded since the last run
        where LOADED_AT > (select max(LOADED_AT) from {{ this }})
    {% endif %}
 
),
 
cleaned as (
 
    select
        EVENT_ID                                    as event_id,
        GUEST_ID                                    as guest_id,
        lower(PARK)                                 as park,
        lower(GATE)                                 as gate,
        lower(TICKET_TYPE)                          as ticket_type,
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