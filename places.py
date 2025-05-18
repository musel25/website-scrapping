#!/usr/bin/env python3
"""
tijuana_targeted_no_website.py
Exhaustively scans urban Tijuana, MX and keeps only businesses

  • whose Google record has **no websiteUri**
  • whose place type is in PROMO_TYPES  (edit this list as you wish)

Outputs → tijuana_no_website.csv   (columns: name,address,phone)

Prereqs
-------
pip install requests     • Places API (New) enabled
export GOOGLE_API_KEY=YOUR_KEY     # or fish →  set -Ux GOOGLE_API_KEY YOUR_KEY
"""

import csv, os, sys, time, requests
from itertools import product

# ─────────────────────────── 1 · customise here ─────────────────────────────
PROMO_TYPES = {
    # food & beverage
    "restaurant", "cafe", "coffee_shop", "bakery", "ice_cream_shop",
    "pizza_restaurant", "dessert_shop", "bar", "pub",

    # beauty & personal care
    "barber_shop", "beauty_salon", "hair_salon", "nail_salon",
    "spa", "massage", "wellness_center", "yoga_studio",

    # small health practices
    "dental_clinic", "physiotherapist", "chiropractor",

    # lodging & rentals
    "bed_and_breakfast", "guest_house", "hostel", "inn",

    # events & recreation
    "wedding_venue", "event_venue", "community_center",
    "tour_agency", "travel_agency",

    # retail & specialty shops
    "clothing_store", "gift_shop", "book_store", "jewelry_store",
    "furniture_store", "pet_store", "sporting_goods_store", "florist",

    # home & repair services
    "electrician", "plumber", "locksmith", "painter", "laundry",
}

# Add / remove values freely — full list: https://developers.google.com/maps/documentation/places/web-service/place-types

# ─────────────────────────── 2 · static config ──────────────────────────────
API_KEY = os.getenv("GOOGLE_API_KEY")
if not API_KEY:
    sys.exit("Set GOOGLE_API_KEY first (export or set -Ux in fish)")

SEARCH_URL  = "https://places.googleapis.com/v1/places:searchNearby"
DETAILS_URL = "https://places.googleapis.com/v1/places/{}"

HEADERS = {"Content-Type": "application/json", "X-Goog-Api-Key": API_KEY}

# Bounding box that covers the city
LAT_MIN, LAT_MAX = 32.42, 32.59
LON_MIN, LON_MAX = -117.13, -116.88
LAT_STEP = LON_STEP = 0.02          # ≈ 2 km grid
RADIUS_M = 1_500.0                  # circle radius at each point (m)

# ─────────────────────────── helpers ────────────────────────────────────────
def frange(start, stop, step):
    while start < stop:
        yield round(start, 6)       # trim FP noise
        start += step

def nearby(lat, lon, page_token=None):
    body = {"pageToken": page_token} if page_token else {
        "maxResultCount": 20,
        "locationRestriction": {
            "circle": {
                "center": {"latitude": lat, "longitude": lon},
                "radius": RADIUS_M,
            }
        }
    }
    mask = "places.id"
    r = requests.post(
        SEARCH_URL, json=body,
        headers=HEADERS | {"X-Goog-FieldMask": mask},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()

def details(pid):
    mask = (
        "displayName,formattedAddress,"
        "internationalPhoneNumber,nationalPhoneNumber,"
        "websiteUri,types,primaryType"
    )
    r = requests.get(
        DETAILS_URL.format(pid),
        headers=HEADERS | {"X-Goog-FieldMask": mask},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()

# ─────────────────────────── crawl ──────────────────────────────────────────
seen, rows = set(), []
grid = list(product(frange(LAT_MIN, LAT_MAX, LAT_STEP),
                    frange(LON_MIN, LON_MAX, LON_STEP)))
print(f"Grid points: {len(grid)} — this may take a few minutes…")

for idx, (lat, lon) in enumerate(grid, 1):
    print(f"\n◾ Point {idx}/{len(grid)}  ({lat:.4f}, {lon:.4f})")
    token, page = None, 0
    while True:
        resp = nearby(lat, lon, token)
        page += 1
        pids = [p["id"] for p in resp.get("places", [])]
        print(f"  page {page}: {len(pids)} places")

        for pid in pids:
            if pid in seen:
                continue
            seen.add(pid)

            det = details(pid)
            if "websiteUri" in det:                # already online → skip
                continue

            # collect place type strings
            place_types = set(det.get("types", []))
            if det.get("primaryType"):
                place_types.add(det["primaryType"])

            if not place_types & PROMO_TYPES:      # no overlap with target
                continue

            rows.append({
                "name":    det["displayName"]["text"],
                "address": det["formattedAddress"],
                "phone":   det.get("internationalPhoneNumber")
                           or det.get("nationalPhoneNumber", ""),
            })

        token = resp.get("nextPageToken")
        if not token:
            break
        time.sleep(2)

# ─────────────────────────── export ─────────────────────────────────────────
print(f"\nMatched businesses (no website + promo type): {len(rows)}")
if rows:
    out = "tijuana_no_website.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["name", "address", "phone"])
        w.writeheader(); w.writerows(rows)
    print("Saved →", out)
else:
    print("No matches found.")
