select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select total_price
from AMUSEMENTPARK.GOLD.fact_ticket_sale
where total_price is null



      
    ) dbt_internal_test