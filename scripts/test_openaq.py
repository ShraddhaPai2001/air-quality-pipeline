import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()  # reads the .env file and loads its values into os.environ

API_KEY = os.getenv("OPENAQ_API_KEY", "9e6a441a40e3e6a80a5c6068feec9bc41026befbeaeea62ede8cc7af6b22f5ab")
BASE_URL = "https://api.openaq.org/v3/locations"

# OpenAQ v3 doesn't filter by city name directly — use lat/lon + radius (meters) instead.
CITIES = {
    "Bangalore": (12.9716, 77.5946),
    "Mumbai": (19.0760, 72.8777),
    "Delhi": (28.7041, 77.1025),
    "Mangalore": (12.9141, 74.8560),
}

RADIUS_METERS = 25000  # 25 km


def fetch_locations(lat: float, lon: float):
    headers = {"X-API-Key": API_KEY}
    params = {
        "coordinates": f"{lat},{lon}",
        "radius": RADIUS_METERS,
        "limit": 10,
    }
    response = requests.get(BASE_URL, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


if __name__ == "__main__":
    for city, (lat, lon) in CITIES.items():
        print(f"\n--- {city} ---")
        try:
            data = fetch_locations(lat, lon)
            results = data.get("results", [])
            if not results:
                print("No stations found for this city.")
                continue
            for loc in results:
                print(json.dumps({
                    "name": loc.get("name"),
                    "id": loc.get("id"),
                    "country": loc.get("country"),
                    "parameters": [p.get("parameter") for p in loc.get("parameters", [])]
                }, indent=2))
        except requests.exceptions.HTTPError as e:
            print(f"Error fetching {city}: {e}")