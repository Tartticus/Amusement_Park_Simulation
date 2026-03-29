
    
    

select
    EVENT_ID as unique_field,
    count(*) as n_records

from AMUSEMENTPARK.RAW.RAW_FOOD_SALE
where EVENT_ID is not null
group by EVENT_ID
having count(*) > 1


