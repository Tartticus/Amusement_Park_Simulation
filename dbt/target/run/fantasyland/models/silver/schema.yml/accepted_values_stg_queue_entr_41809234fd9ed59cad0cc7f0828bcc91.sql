select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

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



      
    ) dbt_internal_test