
    
    

with all_values as (

    select
        park as value_field,
        count(*) as n_records

    from AMUSEMENTPARK.CORE.stg_park_entry
    group by park

)

select *
from all_values
where value_field not in (
    'mythic_kingdom','fantasy_world','wonder_cove'
)


