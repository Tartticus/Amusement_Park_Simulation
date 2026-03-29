select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select park
from AMUSEMENTPARK.CORE.stg_park_entry
where park is null



      
    ) dbt_internal_test