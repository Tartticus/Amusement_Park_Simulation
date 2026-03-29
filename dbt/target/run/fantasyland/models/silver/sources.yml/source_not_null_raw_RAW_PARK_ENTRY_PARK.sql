select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select PARK
from AMUSEMENTPARK.RAW.RAW_PARK_ENTRY
where PARK is null



      
    ) dbt_internal_test