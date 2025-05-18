#!/usr/bin/env python3
"""
tijuana_no_website.py
Lists every business inside ~20 km of downtown Tijuana that does NOT have a
website, then writes name/address/phone to tijuana_no_website.csv.

• Python ≥3.9
• pip install requests
• export (or set -Ux in fish) GOOGLE_API_KEY=your_key
• Places API (New) enabled for that key
"""

import csv, os, sys, time, requests

API_KEY = os.getenv("GOOGLE_API_KEY")
if not API_KEY:
    sys.exit("GOOGLE_API_KEY env var not set")

SEARCH_URL  = "https://places.googleapis.com/v1/places:searchNearby"
DETAILS_URL = "https://places.googleapis.com/v1/places/{}"          # {place_id}

BASE_HEADERS = {
    "Content-Type": "application/json",
    "X-Goog-Api-Key": API_KEY,
}

# ───────────────────────── helpers ───────────────────────────────────────────

def nearby_page(page_token: str | None = None) -> dict:
    """One call to Nearby Search (v1)."""
    if page_token:                         # follow-on request
        body = {"pageToken": page_token}
    else:                                  # first request
        body = {
            "maxResultCount": 20,          # API cap
            "locationRestriction": {
                "circle": {
                    "center":  {"latitude": 32.529014, "longitude": -117.033050},
                    "radius": 20000.0      # metres
                }
            }
            # no includedTypes → return ALL place types
        }

    field_mask = "places.id,places.displayName,places.formattedAddress"

    r = requests.post(
        SEARCH_URL,
        json=body,
        headers=BASE_HEADERS | {"X-Goog-FieldMask": field_mask},
        timeout=10,
    )
    if r.status_code != 200:
        print("Nearby search error:", r.status_code, r.text, end="\n\n")
        r.raise_for_status()
    return r.json()


def details(place_id: str) -> dict:
    """Minimal Place Details request (v1)."""
    mask = (
        "displayName,formattedAddress,"
        "internationalPhoneNumber,nationalPhoneNumber,websiteUri"
    )
    r = requests.get(
        DETAILS_URL.format(place_id),
        headers=BASE_HEADERS | {"X-Goog-FieldMask": mask},
        timeout=10,
    )
    if r.status_code != 200:
        print("Details error:", r.status_code, place_id, r.text, end="\n\n")
        r.raise_for_status()
    return r.json()

# ───────────────────────── crawl & collect ──────────────────────────────────

found: list[dict] = []
token: str | None = None
page  = 0

while True:
    data  = nearby_page(token)
    page += 1
    places = data.get("places", [])
    print(f"Page {page}: {len(places)} places")

    for p in places:
        pid  = p["id"]
        det  = details(pid)

        # skip if Google knows a website
        if "websiteUri" in det:
            continue

        found.append(
            {
                "name":    det["displayName"]["text"],
                "address": det["formattedAddress"],
                "phone":   det.get("internationalPhoneNumber")
                           or det.get("nationalPhoneNumber", ""),
            }
        )

    token = data.get("nextPageToken")
    if not token:
        break
    time.sleep(2)               # token becomes valid ~2 s after it’s issued

print(f"\nBusinesses WITHOUT a website: {len(found)}")

# ───────────────────────── CSV export ───────────────────────────────────────

if found:
    outfile = "tijuana_no_website.csv"
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["name", "address", "phone"])
        w.writeheader()
        w.writerows(found)
    print("Saved →", outfile)
else:
    print("No matches found.")
