select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select TOTAL_PRICE
from AMUSEMENTPARK.RAW.RAW_TICKET_SALE_ONLINE
where TOTAL_PRICE is null



      
    ) dbt_internal_test