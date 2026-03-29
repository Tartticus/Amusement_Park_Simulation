select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    line_item_id as unique_field,
    count(*) as n_records

from AMUSEMENTPARK.CORE.stg_gift_store_sales
where line_item_id is not null
group by line_item_id
having count(*) > 1



      
    ) dbt_internal_test