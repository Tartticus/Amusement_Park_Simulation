select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select wait_time_mins
from AMUSEMENTPARK.CORE.stg_queue_entry
where wait_time_mins is null



      
    ) dbt_internal_test