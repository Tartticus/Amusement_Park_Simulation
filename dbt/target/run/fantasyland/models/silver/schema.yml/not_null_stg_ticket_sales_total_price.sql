select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select total_price
from AMUSEMENTPARK.CORE.stg_ticket_sales
where total_price is null



      
    ) dbt_internal_test