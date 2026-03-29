-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into AMUSEMENTPARK.GOLD.fact_park_entry as DBT_INTERNAL_DEST
        using AMUSEMENTPARK.GOLD.fact_park_entry__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.event_id = DBT_INTERNAL_DEST.event_id
            )

    
    when matched then update set
        "EVENT_ID" = DBT_INTERNAL_SOURCE."EVENT_ID","GUEST_ID" = DBT_INTERNAL_SOURCE."GUEST_ID","PARK_ID" = DBT_INTERNAL_SOURCE."PARK_ID","DATE_ID" = DBT_INTERNAL_SOURCE."DATE_ID","EVENT_HOUR" = DBT_INTERNAL_SOURCE."EVENT_HOUR","GATE" = DBT_INTERNAL_SOURCE."GATE","TICKET_TYPE_ID" = DBT_INTERNAL_SOURCE."TICKET_TYPE_ID","EVENT_TIMESTAMP" = DBT_INTERNAL_SOURCE."EVENT_TIMESTAMP"
    

    when not matched then insert
        ("EVENT_ID", "GUEST_ID", "PARK_ID", "DATE_ID", "EVENT_HOUR", "GATE", "TICKET_TYPE_ID", "EVENT_TIMESTAMP")
    values
        ("EVENT_ID", "GUEST_ID", "PARK_ID", "DATE_ID", "EVENT_HOUR", "GATE", "TICKET_TYPE_ID", "EVENT_TIMESTAMP")

;
    commit;