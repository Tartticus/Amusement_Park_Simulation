select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select party_size
from AMUSEMENTPARK.CORE.stg_hotel_checkin
where party_size is null



      
    ) dbt_internal_test