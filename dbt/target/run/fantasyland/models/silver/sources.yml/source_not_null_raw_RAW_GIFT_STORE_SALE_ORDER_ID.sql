select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select ORDER_ID
from AMUSEMENTPARK.RAW.RAW_GIFT_STORE_SALE
where ORDER_ID is null



      
    ) dbt_internal_test