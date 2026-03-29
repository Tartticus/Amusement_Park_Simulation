

with source as (

    select *
    from AMUSEMENTPARK.RAW.RAW_HOTEL_CHECKIN

    

),

cleaned as (

    select
        EVENT_ID                                    as event_id,
        RESERVATION_ID                              as reservation_id,
        lower(RESORT)                               as resort,
        lower(ROOM_TYPE)                            as room_type,
        PARTY_SIZE                                  as party_size,
        upper(ORIGIN_STATE)                         as origin_state,
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