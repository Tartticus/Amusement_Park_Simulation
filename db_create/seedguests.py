# -*- coding: utf-8 -*-
"""
Created on Sat Mar 28 22:36:57 2026

@author: matth
"""

#Script to add guests

import os
import csv
import uuid
import random
import tempfile
from snowflake_connection import conn, cur
 

 
GUEST_COUNT = 10000
 
ORIGIN_STATES = ["NY", "CA", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "WA",
                 "AZ", "CO", "WA", "MN", "MI", "VA", "NJ", "TN", "MO", "MD"]
 
LOYALTY_TIERS = ["standard", "bronze", "silver", "gold", "vip"]
 
FIRST_NAMES = [
    "James", "Maria", "David", "Sarah", "Michael", "Emily", "Chris", "Jessica",
    "Daniel", "Ashley", "Matthew", "Amanda", "Andrew", "Megan", "Joshua", "Samantha",
    "Ryan", "Lauren", "Justin", "Rachel", "Tyler", "Brittany", "Brandon", "Stephanie",
    "Ethan", "Nicole", "Noah", "Hannah", "Logan", "Kayla",
    "Olivia", "Liam", "Ava", "William", "Sophia", "Benjamin", "Isabella", "Lucas",
    "Mia", "Henry", "Charlotte", "Alexander", "Amelia", "Mason", "Harper", "Elijah",
    "Evelyn", "Oliver", "Abigail", "Jacob", "Ella", "Sebastian", "Scarlett", "Jack",
    "Grace", "Owen", "Chloe", "Aiden", "Penelope", "Carter",
    "Zoe", "Luke", "Lily", "Gabriel", "Aria", "Anthony", "Layla", "Dylan", "Nora",
    "Isaac", "Riley", "Lincoln", "Zoey", "Nathan", "Stella", "Hunter", "Hazel",
    "Christian", "Ellie", "Connor", "Aurora", "Eli", "Savannah", "Landon", "Audrey",
    "Adrian", "Brooklyn", "Jonathan", "Bella", "Nolan", "Claire", "Dominic", "Skylar",
    "Colton", "Lucy", "Aaron", "Paisley", "Carson", "Everly",
]
 
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
    "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen",
    "Hill", "Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera",
    "Campbell", "Mitchell", "Carter", "Roberts", "Phillips", "Evans", "Turner",
    "Parker", "Collins", "Edwards", "Stewart", "Flores", "Morris", "Nguyen",
    "Murphy", "Cook", "Rogers", "Morgan", "Peterson", "Cooper", "Reed", "Bailey",
    "Bell", "Gomez", "Kelly", "Howard", "Ward", "Cox", "Diaz", "Richardson",
    "Wood", "Watson", "Brooks", "Bennett", "Gray", "James", "Reyes", "Cruz",
    "Hughes", "Price", "Myers", "Long", "Foster", "Sanders", "Ross", "Morales",
    "Powell", "Sullivan", "Russell", "Ortiz", "Jenkins", "Gutierrez", "Perry",
    "Butler", "Barnes", "Fisher", "Henderson", "Coleman", "Simmons", "Patterson",
]
 
 
# ─────────────────────────────────────────────
# Create schema + table if they don't exist
# ─────────────────────────────────────────────
 
cur.execute("CREATE SCHEMA IF NOT EXISTS AMUSEMENTPARK.CORE")
 
cur.execute("""
    CREATE TABLE IF NOT EXISTS AMUSEMENTPARK.CORE.GUESTS (
        GUEST_ID        VARCHAR(20)   PRIMARY KEY,
        FIRST_NAME      VARCHAR(100),
        LAST_NAME       VARCHAR(100),
        EMAIL           VARCHAR(255),
        ORIGIN_STATE    VARCHAR(5),
        LOYALTY_TIER    VARCHAR(20),
        FIRST_SEEN_AT   TIMESTAMP_TZ,
        LAST_SEEN_AT    TIMESTAMP_TZ,
        CREATED_AT      TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP()
    )
""")
print("[Seed] GUESTS table ready.")
 
 
# ─────────────────────────────────────────────
# Generate guests into a temp CSV
# ─────────────────────────────────────────────
 
print(f"[Seed] Generating {GUEST_COUNT:,} guests...")
 
csv_path = os.path.join(tempfile.gettempdir(), "fantasyland_guests.csv")
 
with open(csv_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "GUEST_ID", "FIRST_NAME", "LAST_NAME",
        "EMAIL", "ORIGIN_STATE", "LOYALTY_TIER"
    ])
    for _ in range(GUEST_COUNT):
        first = random.choice(FIRST_NAMES)
        last  = random.choice(LAST_NAMES)
        gid   = f"g_{uuid.uuid4().hex[:8]}"
        email = f"{first.lower()}.{last.lower()}{random.randint(1, 9999)}@example.com"
        writer.writerow([
            gid, first, last, email,
            random.choice(ORIGIN_STATES),
            random.choice(LOYALTY_TIERS),
        ])
 
print(f"[Seed] CSV written → {csv_path}")
 
 
# ─────────────────────────────────────────────
# PUT to internal stage → COPY INTO table
# ─────────────────────────────────────────────
 
print("[Seed] Uploading to Snowflake internal stage...")
cur.execute(f"PUT file://{csv_path} @AMUSEMENTPARK.CORE.GUESTS AUTO_COMPRESS=TRUE")
print("[Seed] File staged.")
 
print("[Seed] Running COPY INTO...")
cur.execute("""
    COPY INTO AMUSEMENTPARK.CORE.GUESTS (
        GUEST_ID, FIRST_NAME, LAST_NAME,
        EMAIL, ORIGIN_STATE, LOYALTY_TIER
    )
    FROM @%AMUSEMENTPARK.CORE.GUESTS
    FILE_FORMAT = (
        TYPE                           = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY   = '"'
        SKIP_HEADER                    = 1
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    )
    ON_ERROR = 'CONTINUE'
    PURGE    = TRUE
""")
 
print(f"[Seed] COPY INTO result: {cur.fetchall()}")
conn.commit()
 
 
# ─────────────────────────────────────────────
# Verify + cleanup
# ─────────────────────────────────────────────
 
cur.execute("SELECT COUNT(*) FROM AMUSEMENTPARK.CORE.GUESTS")
total = cur.fetchone()[0]
print(f"[Seed] Done — {total:,} total guests in CORE.GUESTS")
 
os.remove(csv_path)
print("[Seed] Local CSV cleaned up.")
 
cur.close()
conn.close()