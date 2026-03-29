{{
    config(
        materialized = 'incremental',
        unique_key   = 'event_id',
        schema       = 'GOLD'
    )
}}
 
-- Populates EVENT_HEADER and FACT_PARK_ENTRY
-- Joins to dim tables to replace text keys with surrogate IDs
 
with silver as (
 
    select *
    from {{ ref('stg_park_entry') }}
 
    {% if is_incremental() %}
        where loaded_at > (select max(loaded_at) from {{ this }})
    {% endif %}
 
),
 
with_dims as (
 
    select
        s.event_id,
        s.guest_id,
        p.park_id,
        t.ticket_type_id,
        d.date_id,
        s.event_hour,
        s.gate,
        s.event_timestamp,
        s.loaded_at,
        s.s3_file_name
 
    from silver s
    join AMUSEMENTPARK.GOLD.DIM_PARK p
        on p.park_name = s.park
    join AMUSEMENTPARK.GOLD.DIM_TICKET_TYPE t
        on t.ticket_name = s.ticket_type
    join AMUSEMENTPARK.GOLD.DIM_DATE d
        on d.full_date = s.event_date
 
)
 
select * from with_dims
