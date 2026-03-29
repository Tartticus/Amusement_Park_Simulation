

import os
from snowflake_connection import conn, cur

STATEMENTS = [

    "USE ROLE SYSADMIN",
    "USE DATABASE AMUSEMENTPARK",
    "USE WAREHOUSE COMPUTE_WH",

    # ── Schemas ───────────────────────────────
    "CREATE SCHEMA IF NOT EXISTS AMUSEMENTPARK.GOLD",

    # Grant privileges
    "GRANT ALL PRIVILEGES ON SCHEMA AMUSEMENTPARK.GOLD TO ROLE SYSADMIN",


    # ─────────────────────────────────────────
    # EVENT_HEADER
    # Shared audit table — every fact table
    # references this via event_id.
    # Keeps S3 metadata out of fact tables.
    # ─────────────────────────────────────────

    """
    CREATE TABLE IF NOT EXISTS AMUSEMENTPARK.GOLD.EVENT_HEADER (
        EVENT_ID        VARCHAR(50)     NOT NULL PRIMARY KEY,
        EVENT_TYPE      VARCHAR(50)     NOT NULL,
        LOADED_AT       TIMESTAMP_TZ    NOT NULL,
        S3_FILE_NAME    VARCHAR(500)    NOT NULL,
        CREATED_AT      TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
    )
    """,


    # ─────────────────────────────────────────
    # DIMENSIONS
    # ─────────────────────────────────────────

    # dim_park
    """
    CREATE TABLE IF NOT EXISTS AMUSEMENTPARK.GOLD.DIM_PARK (
        PARK_ID         INT             AUTOINCREMENT PRIMARY KEY,
        PARK_NAME       VARCHAR(50)     NOT NULL UNIQUE,
        TIMEZONE        VARCHAR(50)     DEFAULT 'America/New_York',
        CREATED_AT      TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
    )
    """,

    # Seed dim_park with known parks
    """
    INSERT INTO AMUSEMENTPARK.GOLD.DIM_PARK (PARK_NAME)
    SELECT park_name FROM (VALUES
        ('mythic_kingdom'),
        ('fantasy_world'),
        ('wonder_cove')
    ) AS t(park_name)
    WHERE NOT EXISTS (
        SELECT 1 FROM AMUSEMENTPARK.GOLD.DIM_PARK
    )
    """,

    # dim_ride
    """
    CREATE TABLE IF NOT EXISTS AMUSEMENTPARK.GOLD.DIM_RIDE (
        RIDE_ID         INT             AUTOINCREMENT PRIMARY KEY,
        RIDE_NAME       VARCHAR(100)    NOT NULL UNIQUE,
        PARK_ID         INT             NOT NULL,
        RIDE_TYPE       VARCHAR(30),
        CREATED_AT      TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
        FOREIGN KEY (PARK_ID) REFERENCES AMUSEMENTPARK.GOLD.DIM_PARK(PARK_ID)
    )
    """,

    # Seed dim_ride
    """
    INSERT INTO AMUSEMENTPARK.GOLD.DIM_RIDE (RIDE_NAME, PARK_ID, RIDE_TYPE)
    SELECT r.ride_name, p.park_id, r.ride_type
    FROM (VALUES
        ('thunder_mountain_coaster', 'mythic_kingdom', 'coaster'),
        ('rapids_run',               'mythic_kingdom', 'water'),
        ('sky_tower',                'mythic_kingdom', 'observation'),
        ('jungle_safari',            'mythic_kingdom', 'dark_ride'),
        ('volcanic_plunge',          'mythic_kingdom', 'water'),
        ('lost_temple_trek',         'mythic_kingdom', 'dark_ride'),
        ('sky_gondola',              'mythic_kingdom', 'transport'),
        ('canyon_rapids',            'mythic_kingdom', 'water'),
        ('night_safari_express',     'mythic_kingdom', 'train'),
        ('summit_launch',            'mythic_kingdom', 'coaster'),
        ('dragon_flight',            'fantasy_world',  'coaster'),
        ('enchanted_carousel',       'fantasy_world',  'classic'),
        ('mirror_maze',              'fantasy_world',  'walk_through'),
        ('starfall_drop',            'fantasy_world',  'drop_tower'),
        ('crystal_caves',            'fantasy_world',  'dark_ride'),
        ('witches_hollow',           'fantasy_world',  'dark_ride'),
        ('giant_ferris_wheel',       'fantasy_world',  'classic'),
        ('phantom_manor_tour',       'fantasy_world',  'dark_ride'),
        ('moonbeam_express',         'fantasy_world',  'train'),
        ('spellbound_swing',         'fantasy_world',  'thrill'),
        ('reef_explorer',            'wonder_cove',    'submarine'),
        ('wave_rider',               'wonder_cove',    'water'),
        ('deep_dive_simulator',      'wonder_cove',    'simulator'),
        ('kraken_encounter',         'wonder_cove',    'dark_ride'),
        ('tidal_twist',              'wonder_cove',    'spinning'),
        ('pearl_lagoon_cruise',      'wonder_cove',    'boat'),
        ('submarine_voyage',         'wonder_cove',    'submarine'),
        ('aqua_racers',              'wonder_cove',    'water')
    ) AS r(ride_name, park_name, ride_type)
    JOIN AMUSEMENTPARK.GOLD.DIM_PARK p ON p.PARK_NAME = r.park_name
    WHERE NOT EXISTS (
        SELECT 1 FROM AMUSEMENTPARK.GOLD.DIM_RIDE
    )
    """,

    # dim_ticket_type
    """
    CREATE TABLE IF NOT EXISTS AMUSEMENTPARK.GOLD.DIM_TICKET_TYPE (
        TICKET_TYPE_ID  INT             AUTOINCREMENT PRIMARY KEY,
        TICKET_NAME     VARCHAR(30)     NOT NULL UNIQUE,
        PRICE_TIER      VARCHAR(20)     NOT NULL,
        BASE_PRICE      NUMBER(10,2),
        CREATED_AT      TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
    )
    """,

    # Seed dim_ticket_type
    """
    INSERT INTO AMUSEMENTPARK.GOLD.DIM_TICKET_TYPE
        (TICKET_NAME, PRICE_TIER, BASE_PRICE)
    SELECT ticket_name, price_tier, base_price FROM (VALUES
        ('day_pass',      'standard', 109.99),
        ('annual_pass',   'premium',  399.00),
        ('vip_pass',      'premium',  249.99),
        ('family_bundle', 'value',    349.99)
    ) AS t(ticket_name, price_tier, base_price)
    WHERE NOT EXISTS (
        SELECT 1 FROM AMUSEMENTPARK.GOLD.DIM_TICKET_TYPE
    )
    """,

    # dim_date — pre-populated for all of 2026
    """
    CREATE TABLE IF NOT EXISTS AMUSEMENTPARK.GOLD.DIM_DATE (
        DATE_ID         INT             NOT NULL PRIMARY KEY,
        FULL_DATE       DATE            NOT NULL UNIQUE,
        DAY_OF_WEEK     VARCHAR(10)     NOT NULL,
        DAY_NUM         INT             NOT NULL,
        WEEK_NUM        INT             NOT NULL,
        MONTH_NUM       INT             NOT NULL,
        MONTH_NAME      VARCHAR(10)     NOT NULL,
        QUARTER         INT             NOT NULL,
        YEAR            INT             NOT NULL,
        IS_WEEKEND      BOOLEAN         NOT NULL,
        
    )
    """,

    # Seed dim_date for 2026
    """
    INSERT INTO AMUSEMENTPARK.GOLD.DIM_DATE
    SELECT
        TO_NUMBER(TO_CHAR(d.date_val, 'YYYYMMDD'))  AS date_id,
        d.date_val                                  AS full_date,
        DAYNAME(d.date_val)                         AS day_of_week,
        DAYOFWEEK(d.date_val)                       AS day_num,
        WEEKOFYEAR(d.date_val)                      AS week_num,
        MONTH(d.date_val)                           AS month_num,
        MONTHNAME(d.date_val)                       AS month_name,
        QUARTER(d.date_val)                         AS quarter,
        YEAR(d.date_val)                            AS year,
        DAYOFWEEK(d.date_val) IN (1, 7)             AS is_weekend
    FROM (
        SELECT DATEADD(day, seq4(), '2026-01-01'::date) AS date_val
        FROM TABLE(GENERATOR(ROWCOUNT => 365))
    ) d
    WHERE NOT EXISTS (
        SELECT 1 FROM AMUSEMENTPARK.GOLD.DIM_DATE
    )
    """,





    """
    CREATE TABLE IF NOT EXISTS AMUSEMENTPARK.GOLD.FACT_PARK_ENTRY (
        EVENT_ID        VARCHAR(50)     NOT NULL PRIMARY KEY,
        GUEST_ID        VARCHAR(20)     NOT NULL,
        PARK_ID         INT             NOT NULL,
        DATE_ID         INT             NOT NULL,
        EVENT_HOUR      INT             NOT NULL,
        GATE            VARCHAR(50)     NOT NULL,
        TICKET_TYPE_ID  INT             NOT NULL,
        EVENT_TIMESTAMP TIMESTAMP_TZ    NOT NULL,
        FOREIGN KEY (EVENT_ID)       REFERENCES AMUSEMENTPARK.GOLD.EVENT_HEADER(EVENT_ID),
        FOREIGN KEY (PARK_ID)        REFERENCES AMUSEMENTPARK.GOLD.DIM_PARK(PARK_ID),
        FOREIGN KEY (DATE_ID)        REFERENCES AMUSEMENTPARK.GOLD.DIM_DATE(DATE_ID),
        FOREIGN KEY (TICKET_TYPE_ID) REFERENCES AMUSEMENTPARK.GOLD.DIM_TICKET_TYPE(TICKET_TYPE_ID)
    )
    """,

    # fact_queue_entry
    """
    CREATE TABLE IF NOT EXISTS AMUSEMENTPARK.GOLD.FACT_QUEUE_ENTRY (
        EVENT_ID        VARCHAR(50)     NOT NULL PRIMARY KEY,
        GUEST_ID        VARCHAR(20)     NOT NULL,
        PARK_ID         INT             NOT NULL,
        RIDE_ID         INT             NOT NULL,
        DATE_ID         INT             NOT NULL,
        EVENT_HOUR      INT             NOT NULL,
        WAIT_TIME_MINS  INT             NOT NULL,
        WAIT_CATEGORY   VARCHAR(10)     NOT NULL,
        EVENT_TIMESTAMP TIMESTAMP_TZ    NOT NULL,
        FOREIGN KEY (EVENT_ID)  REFERENCES AMUSEMENTPARK.GOLD.EVENT_HEADER(EVENT_ID),
        FOREIGN KEY (PARK_ID)   REFERENCES AMUSEMENTPARK.GOLD.DIM_PARK(PARK_ID),
        FOREIGN KEY (RIDE_ID)   REFERENCES AMUSEMENTPARK.GOLD.DIM_RIDE(RIDE_ID),
        FOREIGN KEY (DATE_ID)   REFERENCES AMUSEMENTPARK.GOLD.DIM_DATE(DATE_ID)
    )
    """,

    # fact_ticket_sale
    """
    CREATE TABLE IF NOT EXISTS AMUSEMENTPARK.GOLD.FACT_TICKET_SALE (
        EVENT_ID        VARCHAR(50)     NOT NULL PRIMARY KEY,
        ORDER_ID        VARCHAR(30)     NOT NULL,
        GUEST_ID        VARCHAR(20)     NOT NULL,
        PARK_ID         INT             NOT NULL,
        DATE_ID         INT             NOT NULL,
        EVENT_HOUR      INT             NOT NULL,
        TICKET_TYPE_ID  INT             NOT NULL,
        QUANTITY        INT             NOT NULL,
        UNIT_PRICE      NUMBER(10,2)    NOT NULL,
        TOTAL_PRICE     NUMBER(10,2)    NOT NULL,
        PAYMENT_METHOD  VARCHAR(20)     NOT NULL,
        CHANNEL         VARCHAR(10)     NOT NULL,
        EVENT_TIMESTAMP TIMESTAMP_TZ    NOT NULL,
        FOREIGN KEY (EVENT_ID)       REFERENCES AMUSEMENTPARK.GOLD.EVENT_HEADER(EVENT_ID),
        FOREIGN KEY (PARK_ID)        REFERENCES AMUSEMENTPARK.GOLD.DIM_PARK(PARK_ID),
        FOREIGN KEY (DATE_ID)        REFERENCES AMUSEMENTPARK.GOLD.DIM_DATE(DATE_ID),
        FOREIGN KEY (TICKET_TYPE_ID) REFERENCES AMUSEMENTPARK.GOLD.DIM_TICKET_TYPE(TICKET_TYPE_ID)
    )
    """,

    # fact_food_sale
    """
    CREATE TABLE IF NOT EXISTS AMUSEMENTPARK.GOLD.FACT_FOOD_SALE (
        LINE_ITEM_ID    VARCHAR(70)     NOT NULL PRIMARY KEY,
        EVENT_ID        VARCHAR(50)     NOT NULL,
        ORDER_ID        VARCHAR(30)     NOT NULL,
        GUEST_ID        VARCHAR(20)     NOT NULL,
        PARK_ID         INT             NOT NULL,
        DATE_ID         INT             NOT NULL,
        EVENT_HOUR      INT             NOT NULL,
        VENUE           VARCHAR(100)    NOT NULL,
        ITEM_NAME       VARCHAR(100)    NOT NULL,
        ITEM_CATEGORY   VARCHAR(30)     NOT NULL,
        QUANTITY        INT             NOT NULL,
        UNIT_PRICE      NUMBER(10,2)    NOT NULL,
        SUBTOTAL        NUMBER(10,2)    NOT NULL,
        PAYMENT_METHOD  VARCHAR(20)     NOT NULL,
        EVENT_TIMESTAMP TIMESTAMP_TZ    NOT NULL,
        FOREIGN KEY (EVENT_ID)  REFERENCES AMUSEMENTPARK.GOLD.EVENT_HEADER(EVENT_ID),
        FOREIGN KEY (PARK_ID)   REFERENCES AMUSEMENTPARK.GOLD.DIM_PARK(PARK_ID),
        FOREIGN KEY (DATE_ID)   REFERENCES AMUSEMENTPARK.GOLD.DIM_DATE(DATE_ID)
    )
    """,

    # fact_gift_store_sale
    """
    CREATE TABLE IF NOT EXISTS AMUSEMENTPARK.GOLD.FACT_GIFT_STORE_SALE (
        LINE_ITEM_ID    VARCHAR(70)     NOT NULL PRIMARY KEY,
        EVENT_ID        VARCHAR(50)     NOT NULL,
        ORDER_ID        VARCHAR(30)     NOT NULL,
        GUEST_ID        VARCHAR(20)     NOT NULL,
        PARK_ID         INT             NOT NULL,
        DATE_ID         INT             NOT NULL,
        EVENT_HOUR      INT             NOT NULL,
        STORE           VARCHAR(100)    NOT NULL,
        ITEM_NAME       VARCHAR(100)    NOT NULL,
        ITEM_CATEGORY   VARCHAR(30)     NOT NULL,
        QUANTITY        INT             NOT NULL,
        UNIT_PRICE      NUMBER(10,2)    NOT NULL,
        SUBTOTAL        NUMBER(10,2)    NOT NULL,
        PAYMENT_METHOD  VARCHAR(20)     NOT NULL,
        EVENT_TIMESTAMP TIMESTAMP_TZ    NOT NULL,
        FOREIGN KEY (EVENT_ID)  REFERENCES AMUSEMENTPARK.GOLD.EVENT_HEADER(EVENT_ID),
        FOREIGN KEY (PARK_ID)   REFERENCES AMUSEMENTPARK.GOLD.DIM_PARK(PARK_ID),
        FOREIGN KEY (DATE_ID)   REFERENCES AMUSEMENTPARK.GOLD.DIM_DATE(DATE_ID)
    )
    """,
]




print(f"[Setup] Running {len(STATEMENTS)} statements...\n")

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

# Verify
print("\n[Setup] Verifying tables in AMUSEMENTPARK.GOLD...")
cur.execute("""
    SELECT TABLE_NAME, TABLE_TYPE
    FROM AMUSEMENTPARK.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'GOLD'
    ORDER BY TABLE_TYPE, TABLE_NAME
""")
for row in cur.fetchall():
    print(f"  ✓ {row[0]}")

print(f"\n[Setup] Done — {failed} failures.")
cur.close()
conn.close()