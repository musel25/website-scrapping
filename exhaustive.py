#!/usr/bin/env python3
"""
tijuana_full_no_website.py
Crawls (nearly) every Google-indexed business in Tijuana, MX and
keeps only the ones WITHOUT a website.  Results → tijuana_no_website.csv

Prereqs
-------
• Python ≥3.9          • pip install requests
• GOOGLE_API_KEY set   • Places API (New) enabled

Run (fish) once:
    set -Ux GOOGLE_API_KEY AIzaSyYourKeyHere
    python3 tijuana_full_no_website.py
"""

import csv, os, sys, time, math, requests
from itertools import product

API_KEY = os.getenv("GOOGLE_API_KEY")
if not API_KEY:
    sys.exit("GOOGLE_API_KEY env var not set")

SEARCH_URL  = "https://places.googleapis.com/v1/places:searchNearby"
DETAILS_URL = "https://places.googleapis.com/v1/places/{}"          # {place_id}

BASE_HEADERS = {
    "Content-Type": "application/json",
    "X-Goog-Api-Key": API_KEY,
}

# ───────────────────────── geometry helpers ─────────────────────────────────

def frange(start: float, stop: float, step: float):
    """Range that yields floats (inclusive of start, exclusive of stop)."""
    x = start
    while x < stop:
        yield x
        x += step

# Bounding box that comfortably covers urban Tijuana
LAT_MIN, LAT_MAX = 32.42, 32.59            # degrees north
LON_MIN, LON_MAX = -117.13, -116.88        # degrees east -→ west (neg. = W)

LAT_STEP = 0.02    # ≈ 2.2 km north-south
LON_STEP = 0.02    # ≈ 1.7 km east-west (at 32.5°N)

RADIUS_M = 1_500.0  # 1.5 km circle at each grid point

# ───────────────────────── API wrappers ─────────────────────────────────────

def nearby(center_lat: float, center_lon: float, page_token: str | None = None):
    """One Nearby-Search request (first or follow-on page)."""
    if page_token:
        body = {"pageToken": page_token}
    else:
        body = {
            "maxResultCount": 20,
            "locationRestriction": {
                "circle": {
                    "center": {"latitude": center_lat, "longitude": center_lon},
                    "radius": RADIUS_M,
                }
            },
            # no includedTypes → ask for *all* place categories
        }

    mask = "places.id"

    r = requests.post(
        SEARCH_URL,
        json=body,
        headers=BASE_HEADERS | {"X-Goog-FieldMask": mask},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()


def details(place_id: str) -> dict:
    """Place Details with just the fields we need."""
    mask = (
        "displayName,formattedAddress,"
        "internationalPhoneNumber,nationalPhoneNumber,websiteUri"
    )
    r = requests.get(
        DETAILS_URL.format(place_id),
        headers=BASE_HEADERS | {"X-Goog-FieldMask": mask},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()

# ───────────────────────── crawl ────────────────────────────────────────────

seen_ids:   set[str]   = set()
rows:       list[dict] = []

grid_points = list(product(frange(LAT_MIN, LAT_MAX, LAT_STEP),
                           frange(LON_MIN, LON_MAX, LON_STEP)))
print(f"Grid points to scan: {len(grid_points)} "
      f"(every {LAT_STEP}° × {LON_STEP}° ≈ 2 km)")

for idx, (lat, lon) in enumerate(grid_points, 1):
    print(f"\n◾ Point {idx}/{len(grid_points)}  ({lat:.4f}, {lon:.4f})")
    page_token = None
    page_num   = 0

    while True:
        resp = nearby(lat, lon, page_token)
        page_num += 1
        place_ids = [p["id"] for p in resp.get("places", [])]
        print(f"  page {page_num}: {len(place_ids)} places")

        for pid in place_ids:
            if pid in seen_ids:
                continue        # already processed via overlap
            seen_ids.add(pid)

            det = details(pid)

            if "websiteUri" in det:
                continue        # business *has* a site → skip

            rows.append(
                {
                    "name":    det["displayName"]["text"],
                    "address": det["formattedAddress"],
                    "phone":   det.get("internationalPhoneNumber")
                               or det.get("nationalPhoneNumber", ""),
                }
            )

        page_token = resp.get("nextPageToken")
        if not page_token:
            break
        time.sleep(2)          # token becomes valid ~2 s after issue

# ───────────────────────── write CSV ────────────────────────────────────────

print(f"\nUnique businesses WITHOUT website: {len(rows)}")
if rows:
    out = "tijuana_no_website.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["name", "address", "phone"])
        w.writeheader()
        w.writerows(rows)
    print("Saved →", out)
else:
    print("No matches found.")
