
    
    

with all_values as (

    select
        wait_category as value_field,
        count(*) as n_records

    from AMUSEMENTPARK.CORE.stg_queue_entry
    group by wait_category

)

select *
from all_values
where value_field not in (
    'short','medium','long'
)


