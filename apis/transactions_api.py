# -*- coding: utf-8 -*-

from fastapi import FastAPI, Query
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timezone
from snowflake_guests import random_guest_id
import random
import uuid
import uvicorn

app = FastAPI(
    title="Fantasyland — Transactions API",
    description="Simulates ticket sales (online and gate), food & beverage, and gift store purchases.",
    version="1.0.0"
)



PARKS = ["mythic_kingdom", "fantasy_world", "wonder_cove"]

TICKET_TYPES = ["day_pass", "annual_pass", "vip_pass", "family_bundle"]

TICKET_PRICES = {
    "day_pass":      {"base": 109.99, "variance": 20.00},
    "annual_pass":   {"base": 399.00, "variance": 0.00},
    "vip_pass":      {"base": 249.99, "variance": 30.00},
    "family_bundle": {"base": 349.99, "variance": 40.00},
}

PAYMENT_METHODS = ["credit_card", "debit_card", "apple_pay", "google_pay", "gift_card"]

GATE_LOCATIONS = {
    "mythic_kingdom": ["north_gate", "south_gate", "main_plaza_gate"],
    "fantasy_world":  ["castle_entrance", "east_gate", "west_gate"],
    "wonder_cove":    ["harbor_entrance", "lagoon_gate"],
}

FOOD_VENUES = {
    "mythic_kingdom": [
        "jungle_grill", "summit_snacks", "canyon_cantina",
        "explorer_cafe", "treetop_treats",
    ],
    "fantasy_world": [
        "dragon_bistro", "enchanted_eats", "castle_creamery",
        "starfall_sweets", "phantom_pub",
    ],
    "wonder_cove": [
        "coral_kitchen", "lagoon_bites", "pearl_patisserie",
        "wave_shack", "captains_grill",
    ],
}

MENU_ITEMS = [
    {"name": "cheeseburger",       "price": 14.99, "category": "entree"},
    {"name": "chicken_tenders",    "price": 12.99, "category": "entree"},
    {"name": "fish_tacos",         "price": 13.49, "category": "entree"},
    {"name": "veggie_wrap",        "price": 11.99, "category": "entree"},
    {"name": "kids_meal",          "price": 9.99,  "category": "entree"},
    {"name": "loaded_nachos",      "price": 10.99, "category": "snack"},
    {"name": "soft_pretzel",       "price": 6.99,  "category": "snack"},
    {"name": "funnel_cake",        "price": 8.49,  "category": "snack"},
    {"name": "ice_cream_cone",     "price": 5.99,  "category": "snack"},
    {"name": "churro",             "price": 4.99,  "category": "snack"},
    {"name": "fresh_lemonade",     "price": 5.49,  "category": "drink"},
    {"name": "fountain_soda",      "price": 4.49,  "category": "drink"},
    {"name": "bottled_water",      "price": 3.99,  "category": "drink"},
    {"name": "frozen_slushie",     "price": 6.99,  "category": "drink"},
    {"name": "specialty_cocktail", "price": 14.99, "category": "drink"},
]

GIFT_STORES = {
    "mythic_kingdom": [
        "jungle_outpost_shop", "summit_souvenirs", "explorers_emporium",
    ],
    "fantasy_world": [
        "castle_gifts", "dragon_den_store", "enchanted_boutique",
    ],
    "wonder_cove": [
        "coral_reef_shop", "treasure_cove_gifts", "lagoon_market",
    ],
}

GIFT_ITEMS = [
    {"name": "character_plush",      "price": 24.99, "category": "toy"},
    {"name": "logo_tshirt",          "price": 34.99, "category": "apparel"},
    {"name": "kids_costume",         "price": 49.99, "category": "apparel"},
    {"name": "snapback_hat",         "price": 29.99, "category": "apparel"},
    {"name": "hoodie",               "price": 64.99, "category": "apparel"},
    {"name": "photo_frame",          "price": 19.99, "category": "home"},
    {"name": "coffee_mug",           "price": 16.99, "category": "home"},
    {"name": "ornament",             "price": 12.99, "category": "home"},
    {"name": "snow_globe",           "price": 22.99, "category": "home"},
    {"name": "keychain",             "price": 9.99,  "category": "accessory"},
    {"name": "magnet_set",           "price": 7.99,  "category": "accessory"},
    {"name": "pin_badge",            "price": 11.99, "category": "accessory"},
    {"name": "tote_bag",             "price": 18.99, "category": "accessory"},
    {"name": "collectible_figurine", "price": 39.99, "category": "collectible"},
    {"name": "limited_edition_print","price": 54.99, "category": "collectible"},
]


# ─────────────────────────────────────────────
# Response models
# ─────────────────────────────────────────────

class TicketSaleOnlineEvent(BaseModel):
    event_type: str
    event_id: str
    order_id: str
    guest_id: str
    park: str
    ticket_type: str
    quantity: int
    unit_price: float
    total_price: float
    payment_method: str
    channel: str
    timestamp: str

class TicketSaleGateEvent(BaseModel):
    event_type: str
    event_id: str
    order_id: str
    guest_id: str
    park: str
    gate: str
    ticket_type: str
    quantity: int
    unit_price: float
    total_price: float
    payment_method: str
    channel: str
    timestamp: str

class SaleLineItem(BaseModel):
    name: str
    category: str
    quantity: int
    unit_price: float
    subtotal: float

class FoodSaleEvent(BaseModel):
    event_type: str
    event_id: str
    order_id: str
    guest_id: str
    park: str
    venue: str
    items: List[SaleLineItem]
    total_price: float
    payment_method: str
    timestamp: str

class GiftStoreSaleEvent(BaseModel):
    event_type: str
    event_id: str
    order_id: str
    guest_id: str
    park: str
    store: str
    items: List[SaleLineItem]
    total_price: float
    payment_method: str
    timestamp: str



def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def random_order_id() -> str:
    return f"ord_{uuid.uuid4().hex[:10]}"

def random_count() -> int:
    return random.randint(1, 50)

def ticket_price(ticket_type: str) -> float:
    cfg = TICKET_PRICES[ticket_type]
    raw = cfg["base"] + random.uniform(-cfg["variance"], cfg["variance"])
    return round(max(raw, cfg["base"] * 0.8), 2)

def random_basket(catalog: list, max_items: int = 4) -> List[SaleLineItem]:
    """Pick 1–max_items random products and build line items."""
    picks = random.sample(catalog, k=random.randint(1, min(max_items, len(catalog))))
    items = []
    for product in picks:
        qty = random.randint(1, 3)
        subtotal = round(product["price"] * qty, 2)
        items.append(SaleLineItem(
            name=product["name"],
            category=product["category"],
            quantity=qty,
            unit_price=product["price"],
            subtotal=subtotal,
        ))
    return items



@app.get("/")
def root():
    return {
        "api": "Fantasyland Transactions API",
        "version": "1.0.0",
        "endpoints": [
            "/guests",
            "/transactions/ticket-online",
            "/transactions/ticket-gate",
            "/transactions/food-sale",
            "/transactions/gift-store-sale",
        ]
    }


@app.get("/guests")
def list_guests():
    """Returns all guest IDs loaded from Snowflake."""
    from snowflake_guests import GUEST_IDS
    return {"total": len(GUEST_IDS), "guest_ids": GUEST_IDS[:50], "note": "showing first 50 of total"}


@app.get("/transactions/ticket-online", response_model=List[TicketSaleOnlineEvent])
def ticket_online(
    park: Optional[str] = Query(None, description="Force a specific park"),
    ticket_type: Optional[str] = Query(None, description="Force a ticket type"),
):
    """
    Simulates tickets purchased online before visiting the park.
    Returns a random batch of 1–50 TICKET_SALE_ONLINE events.
    """
    results = []
    for _ in range(random_count()):
        chosen_park = park or random.choice(PARKS)
        chosen_type = ticket_type or random.choice(TICKET_TYPES)
        if chosen_type not in TICKET_PRICES:
            chosen_type = random.choice(TICKET_TYPES)
        qty = random.randint(1, 6)
        unit = ticket_price(chosen_type)
        results.append(TicketSaleOnlineEvent(
            event_type="TICKET_SALE_ONLINE",
            event_id=str(uuid.uuid4()),
            order_id=random_order_id(),
            guest_id=random_guest_id(),
            park=chosen_park,
            ticket_type=chosen_type,
            quantity=qty,
            unit_price=unit,
            total_price=round(unit * qty, 2),
            payment_method=random.choice(PAYMENT_METHODS),
            channel="online",
            timestamp=now_iso(),
        ))
    return results


@app.get("/transactions/ticket-gate", response_model=List[TicketSaleGateEvent])
def ticket_gate(
    park: Optional[str] = Query(None, description="Force a specific park"),
    ticket_type: Optional[str] = Query(None, description="Force a ticket type"),
):
    """
    Simulates tickets purchased at the park gate on arrival.
    Returns a random batch of 1–50 TICKET_SALE_GATE events.
    """
    results = []
    for _ in range(random_count()):
        chosen_park = park or random.choice(PARKS)
        if chosen_park not in GATE_LOCATIONS:
            chosen_park = random.choice(PARKS)
        chosen_type = ticket_type or random.choice(TICKET_TYPES)
        if chosen_type not in TICKET_PRICES:
            chosen_type = random.choice(TICKET_TYPES)
        qty = random.randint(1, 6)
        unit = ticket_price(chosen_type)
        results.append(TicketSaleGateEvent(
            event_type="TICKET_SALE_GATE",
            event_id=str(uuid.uuid4()),
            order_id=random_order_id(),
            guest_id=random_guest_id(),
            park=chosen_park,
            gate=random.choice(GATE_LOCATIONS[chosen_park]),
            ticket_type=chosen_type,
            quantity=qty,
            unit_price=unit,
            total_price=round(unit * qty, 2),
            payment_method=random.choice(PAYMENT_METHODS),
            channel="gate",
            timestamp=now_iso(),
        ))
    return results


@app.get("/transactions/food-sale", response_model=List[FoodSaleEvent])
def food_sale(
    park: Optional[str] = Query(None, description="Force a specific park"),
    venue: Optional[str] = Query(None, description="Force a specific venue"),
):
    """
    Simulates food & beverage purchases at park venues.
    Returns a random batch of 1–50 FOOD_SALE events, each with 1–4 line items.
    """
    results = []
    for _ in range(random_count()):
        chosen_park = park or random.choice(PARKS)
        if chosen_park not in FOOD_VENUES:
            chosen_park = random.choice(PARKS)
        venues = FOOD_VENUES[chosen_park]
        chosen_venue = venue if (venue in venues) else random.choice(venues)
        items = random_basket(MENU_ITEMS, max_items=4)
        total = round(sum(i.subtotal for i in items), 2)
        results.append(FoodSaleEvent(
            event_type="FOOD_SALE",
            event_id=str(uuid.uuid4()),
            order_id=random_order_id(),
            guest_id=random_guest_id(),
            park=chosen_park,
            venue=chosen_venue,
            items=items,
            total_price=total,
            payment_method=random.choice(PAYMENT_METHODS),
            timestamp=now_iso(),
        ))
    return results


@app.get("/transactions/gift-store-sale", response_model=List[GiftStoreSaleEvent])
def gift_store_sale(
    park: Optional[str] = Query(None, description="Force a specific park"),
    store: Optional[str] = Query(None, description="Force a specific store"),
):
    """
    Simulates merchandise purchases at gift stores.
    Returns a random batch of 1–50 GIFT_STORE_SALE events, each with 1–4 line items.
    """
    results = []
    for _ in range(random_count()):
        chosen_park = park or random.choice(PARKS)
        if chosen_park not in GIFT_STORES:
            chosen_park = random.choice(PARKS)
        stores = GIFT_STORES[chosen_park]
        chosen_store = store if (store in stores) else random.choice(stores)
        items = random_basket(GIFT_ITEMS, max_items=4)
        total = round(sum(i.subtotal for i in items), 2)
        results.append(GiftStoreSaleEvent(
            event_type="GIFT_STORE_SALE",
            event_id=str(uuid.uuid4()),
            order_id=random_order_id(),
            guest_id=random_guest_id(),
            park=chosen_park,
            store=chosen_store,
            items=items,
            total_price=total,
            payment_method=random.choice(PAYMENT_METHODS),
            timestamp=now_iso(),
        ))
    return results


if __name__ == "__main__":
    uvicorn.run("transactions_api:app", port = 8001, reload=True)