select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        channel as value_field,
        count(*) as n_records

    from AMUSEMENTPARK.CORE.stg_ticket_sales
    group by channel

)

select *
from all_values
where value_field not in (
    'online','gate'
)



      
    ) dbt_internal_test