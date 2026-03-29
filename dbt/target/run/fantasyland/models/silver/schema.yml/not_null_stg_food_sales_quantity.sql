select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select quantity
from AMUSEMENTPARK.CORE.stg_food_sales
where quantity is null



      
    ) dbt_internal_test