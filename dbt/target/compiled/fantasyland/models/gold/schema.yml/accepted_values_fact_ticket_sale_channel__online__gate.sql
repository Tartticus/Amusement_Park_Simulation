
    
    

with all_values as (

    select
        channel as value_field,
        count(*) as n_records

    from AMUSEMENTPARK.GOLD.fact_ticket_sale
    group by channel

)

select *
from all_values
where value_field not in (
    'online','gate'
)


