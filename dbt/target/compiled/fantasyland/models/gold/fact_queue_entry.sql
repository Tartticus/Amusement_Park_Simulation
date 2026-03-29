

with silver as (

    select *
    from AMUSEMENTPARK.CORE.stg_queue_entry

    
        where loaded_at > (select max(event_timestamp) from AMUSEMENTPARK.GOLD.fact_queue_entry)
    

),

with_dims as (

    select
        s.event_id,
        s.guest_id,
        p.park_id,
        r.ride_id,
        d.date_id,
        s.event_hour,
        s.wait_time_mins,
        s.wait_category,
        s.event_timestamp,
        s.loaded_at,
        s.s3_file_name

    from silver s
    join AMUSEMENTPARK.GOLD.DIM_PARK p
        on p.park_name = s.park
    join AMUSEMENTPARK.GOLD.DIM_RIDE r
        on r.ride_name = s.ride
    join AMUSEMENTPARK.GOLD.DIM_DATE d
        on d.full_date = s.event_date

)

select * from with_dims