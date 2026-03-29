
from fastapi import FastAPI, Query
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timezone
from snowflake_guests import random_guest_id
import random
import uuid
import uvicorn
 
app = FastAPI(
    title="Fantasyland — Arrivals API",
    description="Simulates guest arrival events across hotel check-ins, park gates, and ride queues.",
    version="1.0.0"
)
 

RESORTS = ["fantasyland_grand_hotel", "enchanted_inn", "stardust_suites"]
ROOM_TYPES = ["standard", "deluxe", "suite", "villa"]
TICKET_TYPES = ["day_pass", "annual_pass", "vip_pass", "family_bundle"]
 
PARKS = ["mythic_kingdom", "fantasy_world", "wonder_cove"]
 
GATES = {
    "mythic_kingdom": ["north_gate", "south_gate", "main_plaza_gate"],
    "fantasy_world":  ["castle_entrance", "east_gate", "west_gate"],
    "wonder_cove":    ["harbor_entrance", "lagoon_gate"],
}
 
RIDES = {
    "mythic_kingdom": [
        "thunder_mountain_coaster",
        "rapids_run",
        "sky_tower",
        "jungle_safari",
        "volcanic_plunge",
        "lost_temple_trek",
        "sky_gondola",
        "canyon_rapids",
        "night_safari_express",
        "summit_launch",
    ],
    "fantasy_world": [
        "dragon_flight",
        "enchanted_carousel",
        "mirror_maze",
        "starfall_drop",
        "crystal_caves",
        "witches_hollow",
        "giant_ferris_wheel",
        "phantom_manor_tour",
        "moonbeam_express",
        "spellbound_swing",
    ],
    "wonder_cove": [
        "reef_explorer",
        "wave_rider",
        "deep_dive_simulator",
        "kraken_encounter",
        "tidal_twist",
        "pearl_lagoon_cruise",
        "submarine_voyage",
        "aqua_racers",
    ],
}
 
 
# ─────────────────────────────────────────────
# Response models
# ─────────────────────────────────────────────
 
class HotelCheckinEvent(BaseModel):
    event_type: str
    event_id: str
    reservation_id: str
    resort: str
    room_type: str
    party_size: int
    origin_state: str
    timestamp: str
 
class ParkEntryEvent(BaseModel):
    event_type: str
    event_id: str
    guest_id: str
    park: str
    gate: str
    ticket_type: str
    timestamp: str
 
class QueueEntryEvent(BaseModel):
    event_type: str
    event_id: str
    guest_id: str
    park: str
    ride: str
    wait_time_mins: int
    timestamp: str
 
 

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
 
def random_reservation_id() -> str:
    return f"res_{random.randint(10000000, 99999999)}"
 
def random_count() -> int:
    """Returns a random batch size between 1 and 50."""
    return random.randint(1, 50)
 
 
# ─────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────
 
@app.get("/")
def root():
    return {
        "api": "Fantasyland Arrivals API",
        "version": "1.0.0",
        "endpoints": [
            "/guests",
            "/events/hotel-checkin",
            "/events/park-entry",
            "/events/queue-entry",
        ]
    }
 
 
@app.get("/guests")
def list_guests():
    """Returns all guest IDs loaded from Snowflake."""
    from snowflake_guests import GUEST_IDS
    return {"total": len(GUEST_IDS), "guest_ids": GUEST_IDS[:50], "note": "showing first 50 of total"}
 
 
@app.get("/events/hotel-checkin", response_model=List[HotelCheckinEvent])
def hotel_checkin(
    resort: Optional[str] = Query(None, description="Force a specific resort name"),
    party_size: Optional[int] = Query(None, ge=1, le=12, description="Override party size"),
):
    """
    Simulates guests checking into a resort hotel.
    Returns a random batch of 1–50 HOTEL_CHECKIN events per call.
    Guest origin_state is pulled from the shared guest pool profile.
    """
    from guests import get_guest
    count = random_count()
    results = []
    for _ in range(count):
        gid = random_guest_id()
        guest = get_guest(gid)
        results.append(HotelCheckinEvent(
            event_type="HOTEL_CHECKIN",
            event_id=str(uuid.uuid4()),
            reservation_id=random_reservation_id(),
            resort=resort or random.choice(RESORTS),
            room_type=random.choice(ROOM_TYPES),
            party_size=party_size or random.randint(1, 6),
            origin_state=guest["origin_state"],
            timestamp=now_iso(),
        ))
    return results
 
 
@app.get("/events/park-entry", response_model=List[ParkEntryEvent])
def park_entry(
    park: Optional[str] = Query(None, description="Force a specific park"),
):
    """
    Simulates guests tapping through a park gate.
    Returns a random batch of 1–50 PARK_ENTRY events per call.
    """
    count = random_count()
    results = []
    for _ in range(count):
        chosen_park = park or random.choice(PARKS)
        if chosen_park not in GATES:
            chosen_park = random.choice(PARKS)
        results.append(ParkEntryEvent(
            event_type="PARK_ENTRY",
            event_id=str(uuid.uuid4()),
            guest_id=random_guest_id(),
            park=chosen_park,
            gate=random.choice(GATES[chosen_park]),
            ticket_type=random.choice(TICKET_TYPES),
            timestamp=now_iso(),
        ))
    return results
 
 
@app.get("/events/queue-entry", response_model=List[QueueEntryEvent])
def queue_entry(
    park: Optional[str] = Query(None, description="Force a specific park"),
    ride: Optional[str] = Query(None, description="Force a specific ride"),
):
    """
    Simulates guests joining a ride queue.
    Returns a random batch of 1–50 QUEUE_ENTRY events per call.
    """
    count = random_count()
    results = []
    for _ in range(count):
        chosen_park = park or random.choice(PARKS)
        if chosen_park not in RIDES:
            chosen_park = random.choice(PARKS)
        available_rides = RIDES[chosen_park]
        chosen_ride = ride if (ride in available_rides) else random.choice(available_rides)
        results.append(QueueEntryEvent(
            event_type="QUEUE_ENTRY",
            event_id=str(uuid.uuid4()),
            guest_id=random_guest_id(),
            park=chosen_park,
            ride=chosen_ride,
            wait_time_mins=random.randint(5, 90),
            timestamp=now_iso(),
        ))
    return results


if __name__ == "__main__":
    uvicorn.run("arrivals_api:app", port = 8000, reload=True)