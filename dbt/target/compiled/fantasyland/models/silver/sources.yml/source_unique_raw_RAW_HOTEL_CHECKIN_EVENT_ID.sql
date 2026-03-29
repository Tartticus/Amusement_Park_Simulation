
    
    

select
    EVENT_ID as unique_field,
    count(*) as n_records

from AMUSEMENTPARK.RAW.RAW_HOTEL_CHECKIN
where EVENT_ID is not null
group by EVENT_ID
having count(*) > 1


