select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select subtotal
from AMUSEMENTPARK.GOLD.fact_gift_store_sale
where subtotal is null



      
    ) dbt_internal_test