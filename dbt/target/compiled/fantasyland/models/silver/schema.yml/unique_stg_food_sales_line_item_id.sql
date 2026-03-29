
    
    

select
    line_item_id as unique_field,
    count(*) as n_records

from AMUSEMENTPARK.CORE.stg_food_sales
where line_item_id is not null
group by line_item_id
having count(*) > 1


