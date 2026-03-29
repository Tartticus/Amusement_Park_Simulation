# -*- coding: utf-8 -*-
"""
Created on Sat Mar 28 23:00:45 2026

@author: matth
"""

import csv
import os
from performance_schedule import SCHEDULE
 
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
 
PERFORMANCES_CSV        = os.path.join(OUTPUT_DIR, "performances.csv")
PERFORMANCE_CAST_CSV    = os.path.join(OUTPUT_DIR, "performance_cast.csv")
PERFORMANCE_CHARS_CSV   = os.path.join(OUTPUT_DIR, "performance_characters.csv")
 
 
# ─────────────────────────────────────────────
# Export performances.csv
# One row per scheduled performance
# ─────────────────────────────────────────────
USE DATABASE AMUSEMENTPARK;
USE SCHEMA CORE;
 
 
-- ─────────────────────────────────────────────────────────────
-- 1. PERFORMANCES
--    One row per scheduled performance instance.
--    STATUS is intentionally excluded — it is derived at
--    query time based on SCHEDULED_START / SCHEDULED_END
--    vs CURRENT_TIMESTAMP(), so it never goes stale.
-- ─────────────────────────────────────────────────────────────
 
CREATE TABLE IF NOT EXISTS AMUSEMENTPARK.CORE.PERFORMANCES (
    PERFORMANCE_ID      VARCHAR(30)     NOT NULL PRIMARY KEY,
    SHOW_NAME           VARCHAR(100)    NOT NULL,
    TYPE                VARCHAR(20)     NOT NULL,   -- 'show' or 'parade'
    PARK                VARCHAR(50)     NOT NULL,
    VENUE               VARCHAR(100)    NOT NULL,
    DURATION_MINS       INT             NOT NULL,
    CAPACITY            INT             NOT NULL,
    FLOAT_COUNT         INT,                        -- NULL for non-parades
    SCHEDULED_START     TIMESTAMP_TZ    NOT NULL,
    SCHEDULED_END       TIMESTAMP_TZ    NOT NULL,
    CREATED_AT          TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
);
 
 
-- ─────────────────────────────────────────────────────────────
-- 2. PERFORMANCE_CAST
--    One row per cast member per performance.
--    Joins to PERFORMANCES on PERFORMANCE_ID.
--    Same cast member (CAST_ID) will appear many times
--    across the year for their assigned show.
-- ─────────────────────────────────────────────────────────────
 
CREATE TABLE IF NOT EXISTS AMUSEMENTPARK.CORE.PERFORMANCE_CAST (
    ID                  INT             AUTOINCREMENT PRIMARY KEY,
    PERFORMANCE_ID      VARCHAR(30)     NOT NULL,
    CAST_ID             VARCHAR(20)     NOT NULL,
    FULL_NAME           VARCHAR(150)    NOT NULL,
    ROLE                VARCHAR(50)     NOT NULL,
    CREATED_AT          TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
 
    FOREIGN KEY (PERFORMANCE_ID) REFERENCES AMUSEMENTPARK.CORE.PERFORMANCES(PERFORMANCE_ID)
);
 
 
-- ─────────────────────────────────────────────────────────────
-- 3. PERFORMANCE_CHARACTERS
--    One row per character appearance per performance.
--    Joins to PERFORMANCES on PERFORMANCE_ID.
-- ─────────────────────────────────────────────────────────────
 
CREATE TABLE IF NOT EXISTS AMUSEMENTPARK.CORE.PERFORMANCE_CHARACTERS (
    ID                  INT             AUTOINCREMENT PRIMARY KEY,
    PERFORMANCE_ID      VARCHAR(30)     NOT NULL,
    PARK                VARCHAR(50)     NOT NULL,
    CHARACTER_NAME      VARCHAR(100)    NOT NULL,
    CREATED_AT          TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
 
    FOREIGN KEY (PERFORMANCE_ID) REFERENCES AMUSEMENTPARK.CORE.PERFORMANCES(PERFORMANCE_ID)
);
 
with open(PERFORMANCES_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "PERFORMANCE_ID",
        "SHOW_NAME",
        "TYPE",
        "PARK",
        "VENUE",
        "DURATION_MINS",
        "CAPACITY",
        "FLOAT_COUNT",
        "SCHEDULED_START",
        "SCHEDULED_END",
    ])
    for p in SCHEDULE:
        writer.writerow([
            p["performance_id"],
            p["show_name"],
            p["type"],
            p["park"],
            p["venue"],
            p["duration_mins"],
            p["capacity"],
            p.get("float_count", ""),   # NULL for non-parades
            p["scheduled_start"],
            p["scheduled_end"],
        ])
 
print(f"[Export] performances.csv         → {len(SCHEDULE):,} rows")
 
 
# ─────────────────────────────────────────────
# Export performance_cast.csv
# One row per cast member per performance
# ─────────────────────────────────────────────
 
cast_rows = []
for p in SCHEDULE:
    for member in p["cast_members"]:
        cast_rows.append([
            p["performance_id"],
            member["cast_id"],
            member["full_name"],
            member["role"],
        ])
 
with open(PERFORMANCE_CAST_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "PERFORMANCE_ID",
        "CAST_ID",
        "FULL_NAME",
        "ROLE",
    ])
    writer.writerows(cast_rows)
 
print(f"[Export] performance_cast.csv     → {len(cast_rows):,} rows")
 
 
# ─────────────────────────────────────────────
# Export performance_characters.csv
# One row per character per performance
# ─────────────────────────────────────────────
 
char_rows = []
for p in SCHEDULE:
    for character in p["characters"]:
        char_rows.append([
            p["performance_id"],
            p["park"],
            character,
        ])
 
with open(PERFORMANCE_CHARS_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "PERFORMANCE_ID",
        "PARK",
        "CHARACTER_NAME",
    ])
    writer.writerows(char_rows)
 
print(f"[Export] performance_characters.csv → {len(char_rows):,} rows")
print(f"\n[Export] Done. Files written to: {OUTPUT_DIR}")
print(f"  → {PERFORMANCES_CSV}")
print(f"  → {PERFORMANCE_CAST_CSV}")
print(f"  → {PERFORMANCE_CHARS_CSV}")