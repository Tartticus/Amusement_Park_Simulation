select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select item_name
from AMUSEMENTPARK.CORE.stg_gift_store_sales
where item_name is null



      
    ) dbt_internal_test