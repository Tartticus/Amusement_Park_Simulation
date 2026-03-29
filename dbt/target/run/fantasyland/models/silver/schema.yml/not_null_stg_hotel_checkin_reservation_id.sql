select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select reservation_id
from AMUSEMENTPARK.CORE.stg_hotel_checkin
where reservation_id is null



      
    ) dbt_internal_test