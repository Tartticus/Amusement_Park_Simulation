select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        ticket_type as value_field,
        count(*) as n_records

    from AMUSEMENTPARK.CORE.stg_park_entry
    group by ticket_type

)

select *
from all_values
where value_field not in (
    'day_pass','annual_pass','vip_pass','family_bundle'
)



      
    ) dbt_internal_test