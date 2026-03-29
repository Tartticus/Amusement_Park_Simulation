select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select EVENT_ID
from AMUSEMENTPARK.RAW.RAW_PARK_ENTRY
where EVENT_ID is null



      
    ) dbt_internal_test