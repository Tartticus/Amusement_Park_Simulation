select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        park as value_field,
        count(*) as n_records

    from AMUSEMENTPARK.CORE.stg_park_entry
    group by park

)

select *
from all_values
where value_field not in (
    'mythic_kingdom','fantasy_world','wonder_cove'
)



      
    ) dbt_internal_test