
    
    

select
    line_item_id as unique_field,
    count(*) as n_records

from AMUSEMENTPARK.GOLD.fact_food_sale
where line_item_id is not null
group by line_item_id
having count(*) > 1


