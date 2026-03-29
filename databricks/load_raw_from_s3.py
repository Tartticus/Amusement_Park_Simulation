


dbutils.widgets.text("topic", "arrivals", "Topic (arrivals or transactions)")
dbutils.widgets.text("date",  "",         "Date (YYYY-MM-DD, blank = today)")
dbutils.widgets.text("hour",  "",         "Hour (HH, blank = current hour)")

from datetime import datetime, timezone

TOPIC = dbutils.widgets.get("topic").lower()
date  = dbutils.widgets.get("date")
hour  = dbutils.widgets.get("hour")

# Default to current UTC hour if not passed
if not date or not hour:
    now  = datetime.now(timezone.utc)
    date = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")

YEAR  = date.split("-")[0]
MONTH = date.split("-")[1]
DAY   = date.split("-")[2]
HOUR  = hour.zfill(2)

HOUR_PATH = f"year={YEAR}/month={MONTH}/day={DAY}/hour={HOUR}"

print(f"Loading topic={TOPIC} partition={HOUR_PATH}")


# ─────────────────────────────────────────────
# Snowflake connection
# Credentials stored in Databricks secrets
# ─────────────────────────────────────────────

import snowflake.connector

SNOWFLAKE_OPTIONS = {
    "user":      dbutils.secrets.get("fantasyland", "snowflake_user"),
    "password":  dbutils.secrets.get("fantasyland", "snowflake_password"),
    "account":   dbutils.secrets.get("fantasyland", "snowflake_account"),
    "warehouse": "COMPUTE_WH",
    "database":  "AMUSEMENTPARK",
    "schema":    "RAW",
    "role":      "SYSADMIN",
}

conn = snowflake.connector.connect(**SNOWFLAKE_OPTIONS)
cur  = conn.cursor()
print("Connected to Snowflake")


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

STATEMENTS = ARRIVALS_STATEMENTS if TOPIC == "arrivals" else TRANSACTIONS_STATEMENTS


# ─────────────────────────────────────────────
# Run COPY INTO statements
# ─────────────────────────────────────────────

total_loaded = 0
failed       = 0
results      = []

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
cur.close()
conn.close()

print(f"\nDone — {total_loaded} rows loaded, {failed} failures.")

# Return results as a Databricks display table
display(spark.createDataFrame(results))

# Fail the job if any statement failed
# so Airflow sees the failure and retries
if failed > 0:
    raise Exception(f"{failed} COPY INTO statements failed — check logs above")