select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    EVENT_ID as unique_field,
    count(*) as n_records

from AMUSEMENTPARK.RAW.RAW_FOOD_SALE
where EVENT_ID is not null
group by EVENT_ID
having count(*) > 1



      
    ) dbt_internal_test