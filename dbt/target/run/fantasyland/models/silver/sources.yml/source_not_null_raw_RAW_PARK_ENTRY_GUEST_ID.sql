select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select GUEST_ID
from AMUSEMENTPARK.RAW.RAW_PARK_ENTRY
where GUEST_ID is null



      
    ) dbt_internal_test