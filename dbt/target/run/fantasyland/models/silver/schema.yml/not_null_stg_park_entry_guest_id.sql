select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select guest_id
from AMUSEMENTPARK.CORE.stg_park_entry
where guest_id is null



      
    ) dbt_internal_test