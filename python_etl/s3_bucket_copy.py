

import sys
import os
from datetime import datetime, timezone
from snowflake_connection import conn, cur
 

VALID_TOPICS = ["arrivals", "transactions"]
 
if len(sys.argv) < 2:
    print("Usage: python load_raw_from_s3.py <topic> [date] [hour]")
    print("  topic: arrivals or transactions")
    sys.exit(1)
 
TOPIC = sys.argv[1].lower()
 
if TOPIC not in VALID_TOPICS:
    print(f"[Loader] ERROR — unknown topic '{TOPIC}'. Must be one of: {VALID_TOPICS}")
    sys.exit(1)
 
if len(sys.argv) == 4:
    date_str = sys.argv[2]
    hour     = int(sys.argv[3])
    dt       = datetime.strptime(f"{date_str} {hour}", "%Y-%m-%d %H")
else:
    dt = datetime.now(timezone.utc)
 
YEAR      = dt.year
MONTH     = f"{dt.month:02d}"
DAY       = f"{dt.day:02d}"
HOUR      = f"{dt.hour:02d}"
HOUR_PATH = f"year={YEAR}/month={MONTH}/day={DAY}/hour={HOUR}"
 
print(f"[Loader] Topic: {TOPIC} | Partition: {HOUR_PATH}")
 
 
# ─────────────────────────────────────────────
# COPY INTO statements per topic
# ─────────────────────────────────────────────
 
ARRIVALS_STATEMENTS = [
 
    ("RAW_PARK_ENTRY", f"""
        COPY INTO AMUSEMENTPARK.RAW.RAW_PARK_ENTRY (
            EVENT_ID, GUEST_ID, PARK, GATE,
            TICKET_TYPE, EVENT_TIMESTAMP, S3_FILE_NAME, RAW_PAYLOAD
        )
        FROM (
            SELECT
                $1:event_id::VARCHAR,
                $1:guest_id::VARCHAR,
                $1:park::VARCHAR,
                $1:gate::VARCHAR,
                $1:ticket_type::VARCHAR,
                $1:timestamp::TIMESTAMP_TZ,
                METADATA$FILENAME,
                $1
            FROM @AMUSEMENTPARK.RAW.S3_STAGE/arrivals/{HOUR_PATH}/
        )
        FILE_FORMAT = (TYPE = 'JSON')
        PATTERN     = '.*PARK_ENTRY.*\\.json'
        ON_ERROR    = 'CONTINUE'
    """),
 
    ("RAW_HOTEL_CHECKIN", f"""
        COPY INTO AMUSEMENTPARK.RAW.RAW_HOTEL_CHECKIN (
            EVENT_ID, RESERVATION_ID, RESORT, ROOM_TYPE,
            PARTY_SIZE, ORIGIN_STATE, EVENT_TIMESTAMP, S3_FILE_NAME, RAW_PAYLOAD
        )
        FROM (
            SELECT
                $1:event_id::VARCHAR,
                $1:reservation_id::VARCHAR,
                $1:resort::VARCHAR,
                $1:room_type::VARCHAR,
                $1:party_size::INT,
                $1:origin_state::VARCHAR,
                $1:timestamp::TIMESTAMP_TZ,
                METADATA$FILENAME,
                $1
            FROM @AMUSEMENTPARK.RAW.S3_STAGE/arrivals/{HOUR_PATH}/
        )
        FILE_FORMAT = (TYPE = 'JSON')
        PATTERN     = '.*HOTEL_CHECKIN.*\\.json'
        ON_ERROR    = 'CONTINUE'
    """),
 
    ("RAW_QUEUE_ENTRY", f"""
        COPY INTO AMUSEMENTPARK.RAW.RAW_QUEUE_ENTRY (
            EVENT_ID, GUEST_ID, PARK, RIDE,
            WAIT_TIME_MINS, EVENT_TIMESTAMP, S3_FILE_NAME, RAW_PAYLOAD
        )
        FROM (
            SELECT
                $1:event_id::VARCHAR,
                $1:guest_id::VARCHAR,
                $1:park::VARCHAR,
                $1:ride::VARCHAR,
                $1:wait_time_mins::INT,
                $1:timestamp::TIMESTAMP_TZ,
                METADATA$FILENAME,
                $1
            FROM @AMUSEMENTPARK.RAW.S3_STAGE/arrivals/{HOUR_PATH}/
        )
        FILE_FORMAT = (TYPE = 'JSON')
        PATTERN     = '.*QUEUE_ENTRY.*\\.json'
        ON_ERROR    = 'CONTINUE'
    """),
]
 
TRANSACTIONS_STATEMENTS = [
 
    ("RAW_TICKET_SALE_ONLINE", f"""
        COPY INTO AMUSEMENTPARK.RAW.RAW_TICKET_SALE_ONLINE (
            EVENT_ID, ORDER_ID, GUEST_ID, PARK, TICKET_TYPE,
            QUANTITY, UNIT_PRICE, TOTAL_PRICE, PAYMENT_METHOD,
            CHANNEL, EVENT_TIMESTAMP, S3_FILE_NAME, RAW_PAYLOAD
        )
        FROM (
            SELECT
                $1:event_id::VARCHAR,
                $1:order_id::VARCHAR,
                $1:guest_id::VARCHAR,
                $1:park::VARCHAR,
                $1:ticket_type::VARCHAR,
                $1:quantity::INT,
                $1:unit_price::NUMBER(10,2),
                $1:total_price::NUMBER(10,2),
                $1:payment_method::VARCHAR,
                $1:channel::VARCHAR,
                $1:timestamp::TIMESTAMP_TZ,
                METADATA$FILENAME,
                $1
            FROM @AMUSEMENTPARK.RAW.S3_STAGE/transactions/{HOUR_PATH}/
        )
        FILE_FORMAT = (TYPE = 'JSON')
        PATTERN     = '.*TICKET_SALE_ONLINE.*\\.json'
        ON_ERROR    = 'CONTINUE'
    """),
 
    ("RAW_TICKET_SALE_GATE", f"""
        COPY INTO AMUSEMENTPARK.RAW.RAW_TICKET_SALE_GATE (
            EVENT_ID, ORDER_ID, GUEST_ID, PARK, GATE, TICKET_TYPE,
            QUANTITY, UNIT_PRICE, TOTAL_PRICE, PAYMENT_METHOD,
            CHANNEL, EVENT_TIMESTAMP, S3_FILE_NAME, RAW_PAYLOAD
        )
        FROM (
            SELECT
                $1:event_id::VARCHAR,
                $1:order_id::VARCHAR,
                $1:guest_id::VARCHAR,
                $1:park::VARCHAR,
                $1:gate::VARCHAR,
                $1:ticket_type::VARCHAR,
                $1:quantity::INT,
                $1:unit_price::NUMBER(10,2),
                $1:total_price::NUMBER(10,2),
                $1:payment_method::VARCHAR,
                $1:channel::VARCHAR,
                $1:timestamp::TIMESTAMP_TZ,
                METADATA$FILENAME,
                $1
            FROM @AMUSEMENTPARK.RAW.S3_STAGE/transactions/{HOUR_PATH}/
        )
        FILE_FORMAT = (TYPE = 'JSON')
        PATTERN     = '.*TICKET_SALE_GATE.*\\.json'
        ON_ERROR    = 'CONTINUE'
    """),
 
    ("RAW_FOOD_SALE", f"""
        COPY INTO AMUSEMENTPARK.RAW.RAW_FOOD_SALE (
            EVENT_ID, ORDER_ID, GUEST_ID, PARK, VENUE,
            ITEMS, TOTAL_PRICE, PAYMENT_METHOD,
            EVENT_TIMESTAMP, S3_FILE_NAME, RAW_PAYLOAD
        )
        FROM (
            SELECT
                $1:event_id::VARCHAR,
                $1:order_id::VARCHAR,
                $1:guest_id::VARCHAR,
                $1:park::VARCHAR,
                $1:venue::VARCHAR,
                $1:items::VARIANT,
                $1:total_price::NUMBER(10,2),
                $1:payment_method::VARCHAR,
                $1:timestamp::TIMESTAMP_TZ,
                METADATA$FILENAME,
                $1
            FROM @AMUSEMENTPARK.RAW.S3_STAGE/transactions/{HOUR_PATH}/
        )
        FILE_FORMAT = (TYPE = 'JSON')
        PATTERN     = '.*FOOD_SALE.*\\.json'
        ON_ERROR    = 'CONTINUE'
    """),
 
    ("RAW_GIFT_STORE_SALE", f"""
        COPY INTO AMUSEMENTPARK.RAW.RAW_GIFT_STORE_SALE (
            EVENT_ID, ORDER_ID, GUEST_ID, PARK, STORE,
            ITEMS, TOTAL_PRICE, PAYMENT_METHOD,
            EVENT_TIMESTAMP, S3_FILE_NAME, RAW_PAYLOAD
        )
        FROM (
            SELECT
                $1:event_id::VARCHAR,
                $1:order_id::VARCHAR,
                $1:guest_id::VARCHAR,
                $1:park::VARCHAR,
                $1:store::VARCHAR,
                $1:items::VARIANT,
                $1:total_price::NUMBER(10,2),
                $1:payment_method::VARCHAR,
                $1:timestamp::TIMESTAMP_TZ,
                METADATA$FILENAME,
                $1
            FROM @AMUSEMENTPARK.RAW.S3_STAGE/transactions/{HOUR_PATH}/
        )
        FILE_FORMAT = (TYPE = 'JSON')
        PATTERN     = '.*GIFT_STORE_SALE.*\\.json'
        ON_ERROR    = 'CONTINUE'
    """),
]
 
# Pick which set of statements to run
STATEMENTS = ARRIVALS_STATEMENTS if TOPIC == "arrivals" else TRANSACTIONS_STATEMENTS
 

total_loaded = 0
failed       = 0
 
print(f"[Loader] Running {len(STATEMENTS)} COPY INTO statements...")
 
for table_name, stmt in STATEMENTS:
    try:
        cur.execute(stmt)
        result = cur.fetchall()
        print(result)
        if result is None:
            rows_loaded = 0
        else:
            try:
                rows_loaded = sum(row[3] for row in result)
                print(rows_loaded)
            except Exception as e:
                print(e)
                rows_loaded = 0
                pass    
        total_loaded += rows_loaded
        status = "SUCCESS"
        print(f"  ✓ {table_name:<35} {total_loaded:>6} rows loaded")
    except Exception as e:
        rows_loaded = 0
        status = f"FAILED: {e}"
        failed += 1
        print(f"  ✗ {table_name:<35} {status}")

    results.append({
        "table":       table_name,
        "rows_loaded": rows_loaded,
        "status":      status,
    })
 
conn.commit()
print(f"\n[Loader] Done — {total_loaded} rows loaded, {failed} failures.")
cur.close()
conn.close()