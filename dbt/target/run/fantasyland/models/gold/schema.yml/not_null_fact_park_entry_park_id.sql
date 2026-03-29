select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select park_id
from AMUSEMENTPARK.GOLD.fact_park_entry
where park_id is null



      
    ) dbt_internal_test