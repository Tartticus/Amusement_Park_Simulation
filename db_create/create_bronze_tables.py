

import os
from snowflake_connection import conn, cur



S3_BUCKET         = os.getenv("S3_BUCKET",            "your-bucket-name")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID_FANTASY",    "your-access-key-here")
AWS_SECRET_KEY    = os.getenv("AWS_SECRET_ACCESS_KEY_FANTASY", "your-secret-key-here")



STATEMENTS = [

    # Schema
    "CREATE SCHEMA IF NOT EXISTS AMUSEMENTPARK.RAW",

    # S3 external stage
    f"""
    CREATE STAGE IF NOT EXISTS AMUSEMENTPARK.RAW.S3_STAGE
        URL = 's3://fantasyland-project/'
        CREDENTIALS = (
            AWS_KEY_ID     = '{AWS_ACCESS_KEY_ID}'
            AWS_SECRET_KEY = '{AWS_SECRET_KEY}'
        )
        FILE_FORMAT = (
            TYPE              = 'JSON'
            STRIP_OUTER_ARRAY = FALSE
            IGNORE_UTF8_ERRORS = TRUE
        )
        COMMENT = 'Fantasyland S3 Bronze bucket'
    """,

    # 1. Park Entry
    """
    CREATE TABLE IF NOT EXISTS AMUSEMENTPARK.RAW.RAW_PARK_ENTRY (
        RAW_ID          INT             AUTOINCREMENT PRIMARY KEY,
        EVENT_ID        VARCHAR(50)     NOT NULL,
        GUEST_ID        VARCHAR(20)     NOT NULL,
        PARK            VARCHAR(50)     NOT NULL,
        GATE            VARCHAR(50)     NOT NULL,
        TICKET_TYPE     VARCHAR(30)     NOT NULL,
        EVENT_TIMESTAMP TIMESTAMP_TZ    NOT NULL,
        LOADED_AT       TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
        S3_FILE_NAME    VARCHAR(500),
        RAW_PAYLOAD     VARIANT
    )
    """,

    # 2. Hotel Checkin
    """
    CREATE TABLE IF NOT EXISTS AMUSEMENTPARK.RAW.RAW_HOTEL_CHECKIN (
        RAW_ID          INT             AUTOINCREMENT PRIMARY KEY,
        EVENT_ID        VARCHAR(50)     NOT NULL,
        RESERVATION_ID  VARCHAR(30)     NOT NULL,
        RESORT          VARCHAR(50)     NOT NULL,
        ROOM_TYPE       VARCHAR(20)     NOT NULL,
        PARTY_SIZE      INT             NOT NULL,
        ORIGIN_STATE    VARCHAR(5),
        EVENT_TIMESTAMP TIMESTAMP_TZ    NOT NULL,
        LOADED_AT       TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
        S3_FILE_NAME    VARCHAR(500),
        RAW_PAYLOAD     VARIANT
    )
    """,

    # 3. Queue Entry
    """
    CREATE TABLE IF NOT EXISTS AMUSEMENTPARK.RAW.RAW_QUEUE_ENTRY (
        RAW_ID          INT             AUTOINCREMENT PRIMARY KEY,
        EVENT_ID        VARCHAR(50)     NOT NULL,
        GUEST_ID        VARCHAR(20)     NOT NULL,
        PARK            VARCHAR(50)     NOT NULL,
        RIDE            VARCHAR(100)    NOT NULL,
        WAIT_TIME_MINS  INT             NOT NULL,
        EVENT_TIMESTAMP TIMESTAMP_TZ    NOT NULL,
        LOADED_AT       TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
        S3_FILE_NAME    VARCHAR(500),
        RAW_PAYLOAD     VARIANT
    )
    """,

    # 4. Ticket Sale Online
    """
    CREATE TABLE IF NOT EXISTS AMUSEMENTPARK.RAW.RAW_TICKET_SALE_ONLINE (
        RAW_ID          INT             AUTOINCREMENT PRIMARY KEY,
        EVENT_ID        VARCHAR(50)     NOT NULL,
        ORDER_ID        VARCHAR(30)     NOT NULL,
        GUEST_ID        VARCHAR(20)     NOT NULL,
        PARK            VARCHAR(50)     NOT NULL,
        TICKET_TYPE     VARCHAR(30)     NOT NULL,
        QUANTITY        INT             NOT NULL,
        UNIT_PRICE      NUMBER(10, 2)   NOT NULL,
        TOTAL_PRICE     NUMBER(10, 2)   NOT NULL,
        PAYMENT_METHOD  VARCHAR(20)     NOT NULL,
        CHANNEL         VARCHAR(10)     NOT NULL,
        EVENT_TIMESTAMP TIMESTAMP_TZ    NOT NULL,
        LOADED_AT       TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
        S3_FILE_NAME    VARCHAR(500),
        RAW_PAYLOAD     VARIANT
    )
    """,

    # 5. Ticket Sale Gate
    """
    CREATE TABLE IF NOT EXISTS AMUSEMENTPARK.RAW.RAW_TICKET_SALE_GATE (
        RAW_ID          INT             AUTOINCREMENT PRIMARY KEY,
        EVENT_ID        VARCHAR(50)     NOT NULL,
        ORDER_ID        VARCHAR(30)     NOT NULL,
        GUEST_ID        VARCHAR(20)     NOT NULL,
        PARK            VARCHAR(50)     NOT NULL,
        GATE            VARCHAR(50)     NOT NULL,
        TICKET_TYPE     VARCHAR(30)     NOT NULL,
        QUANTITY        INT             NOT NULL,
        UNIT_PRICE      NUMBER(10, 2)   NOT NULL,
        TOTAL_PRICE     NUMBER(10, 2)   NOT NULL,
        PAYMENT_METHOD  VARCHAR(20)     NOT NULL,
        CHANNEL         VARCHAR(10)     NOT NULL,
        EVENT_TIMESTAMP TIMESTAMP_TZ    NOT NULL,
        LOADED_AT       TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
        S3_FILE_NAME    VARCHAR(500),
        RAW_PAYLOAD     VARIANT
    )
    """,

    # 6. Food Sale
    """
    CREATE TABLE IF NOT EXISTS AMUSEMENTPARK.RAW.RAW_FOOD_SALE (
        RAW_ID          INT             AUTOINCREMENT PRIMARY KEY,
        EVENT_ID        VARCHAR(50)     NOT NULL,
        ORDER_ID        VARCHAR(30)     NOT NULL,
        GUEST_ID        VARCHAR(20)     NOT NULL,
        PARK            VARCHAR(50)     NOT NULL,
        VENUE           VARCHAR(100)    NOT NULL,
        ITEMS           VARIANT         NOT NULL,
        TOTAL_PRICE     NUMBER(10, 2)   NOT NULL,
        PAYMENT_METHOD  VARCHAR(20)     NOT NULL,
        EVENT_TIMESTAMP TIMESTAMP_TZ    NOT NULL,
        LOADED_AT       TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
        S3_FILE_NAME    VARCHAR(500),
        RAW_PAYLOAD     VARIANT
    )
    """,

    # 7. Gift Store Sale
    """
    CREATE TABLE IF NOT EXISTS AMUSEMENTPARK.RAW.RAW_GIFT_STORE_SALE (
        RAW_ID          INT             AUTOINCREMENT PRIMARY KEY,
        EVENT_ID        VARCHAR(50)     NOT NULL,
        ORDER_ID        VARCHAR(30)     NOT NULL,
        GUEST_ID        VARCHAR(20)     NOT NULL,
        PARK            VARCHAR(50)     NOT NULL,
        STORE           VARCHAR(100)    NOT NULL,
        ITEMS           VARIANT         NOT NULL,
        TOTAL_PRICE     NUMBER(10, 2)   NOT NULL,
        PAYMENT_METHOD  VARCHAR(20)     NOT NULL,
        EVENT_TIMESTAMP TIMESTAMP_TZ    NOT NULL,
        LOADED_AT       TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
        S3_FILE_NAME    VARCHAR(500),
        RAW_PAYLOAD     VARIANT
    )
    """,
]



print(f"[Setup] Running {len(STATEMENTS)} DDL statements...\n")

failed = 0
for stmt in STATEMENTS:
    preview = " ".join(stmt.split())[:80]
    try:
        cur.execute(stmt)
        print(f"  ✓ {preview}...")
    except Exception as e:
        print(f"  ✗ FAILED: {preview}")
        print(f"    Error: {e}")
        failed += 1

conn.commit()


# ─────────────────────────────────────────────
# Verify
# ─────────────────────────────────────────────

print("\n[Setup] Verifying tables in AMUSEMENTPARK.RAW...")
cur.execute("""
    SELECT TABLE_NAME
    FROM AMUSEMENTPARK.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'RAW'
    ORDER BY TABLE_NAME
""")
tables = cur.fetchall()
for t in tables:
    print(f"  ✓ AMUSEMENTPARK.RAW.{t[0]}")

print(f"\n[Setup] Done — {len(tables)} tables ready, {failed} failures.")
cur.close()
conn.close()