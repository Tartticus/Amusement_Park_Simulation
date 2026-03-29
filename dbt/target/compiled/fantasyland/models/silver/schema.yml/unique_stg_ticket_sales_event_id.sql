
    
    

select
    event_id as unique_field,
    count(*) as n_records

from AMUSEMENTPARK.CORE.stg_ticket_sales
where event_id is not null
group by event_id
having count(*) > 1


