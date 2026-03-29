

with silver as (

    select *
    from AMUSEMENTPARK.CORE.stg_food_sales

    
        where loaded_at > (select max(event_timestamp) from AMUSEMENTPARK.GOLD.fact_food_sale)
    

),

with_dims as (

    select
        s.line_item_id,
        s.event_id,
        s.order_id,
        s.guest_id,
        p.park_id,
        d.date_id,
        s.event_hour,
        s.venue,
        s.item_name,
        s.item_category,
        s.quantity,
        s.unit_price,
        s.subtotal,
        s.payment_method,
        s.event_timestamp,
        s.loaded_at

    from silver s
    join AMUSEMENTPARK.GOLD.DIM_PARK p
        on p.park_name = s.park
    join AMUSEMENTPARK.GOLD.DIM_DATE d
        on d.full_date = s.event_date

)

select * from with_dims