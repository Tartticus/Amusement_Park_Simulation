

import random
from snowflake_connection import conn, cur
 
# Helper code for other apis
 
print("[Guests] Loading guest IDs from AMUSEMENTPARK.CORE.GUESTS...")
 
cur.execute("SELECT GUEST_ID FROM AMUSEMENTPARK.CORE.GUESTS")
rows = cur.fetchall()
 
if not rows:
    raise RuntimeError(
        "[Guests] No guests found in CORE.GUESTS. "
        "Run seed_guests.py first before starting the APIs."
    )
 
GUEST_IDS: list = [row[0] for row in rows]
 
print(f"[Guests] Loaded {len(GUEST_IDS):,} guest IDs from Snowflake.")
 

 
def random_guest_id() -> str:
    """Pick a random guest_id from the Snowflake-backed pool."""
    return random.choice(GUEST_IDS)
 
 
def random_guest_ids(n: int) -> list:
    """Pick n random guest_ids (with replacement) from the pool."""
    return random.choices(GUEST_IDS, k=n)
 