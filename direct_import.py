import psycopg2
import requests
import gzip
import io
import json
from datetime import datetime, timezone
from tqdm import tqdm

# Postgres connection
conn = psycopg2.connect(
    dbname='postgis_db',
    user='postgres',
    password='password',
    host='host.docker.internal',
    port=5432
)
cur = conn.cursor()

BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2025/04/01"

# === Helper ===
def generate_sample_timestamps(count=10):
    timestamps = []
    for h in range(24):
        for m in range(60):
            for s in range(0, 60, 5):
                timestamps.append(f"{h:02}{m:02}{s:02}Z")
                # if len(timestamps) >= count:
                #     return timestamps
    return timestamps

def insert_aircraft(timestamp, ac):
    lat, lon = ac.get('lat'), ac.get('lon')
    if lat is None or lon is None:
        return

    cur.execute("""
        INSERT INTO public.adsb (
            timestamp, hex, flight, registration, aircraft_type,
            altitude, ground_speed, nav_heading, track, geom, raw
        ) VALUES (
            %(timestamp)s, %(hex)s, %(flight)s, %(registration)s, %(aircraft_type)s,
            %(altitude)s, %(ground_speed)s, %(nav_heading)s, %(track)s, ST_MakePoint(%(lon)s, %(lat)s), %(raw)s
        );
    """, {
        'timestamp': timestamp,
        'hex': ac.get('hex'),
        'flight': ac.get('flight', '').strip(),
        'registration': ac.get('r'),
        'aircraft_type': ac.get('t'),
        'altitude': parse_int(ac.get('alt_baro')),
        'ground_speed': parse_float(ac.get('gs')),
        'nav_heading': parse_float(ac.get('nav_heading')),
        'track': parse_float(ac.get('track')),
        'lon': lon,
        'lat': lat,
        'raw': json.dumps(ac)
    })

def parse_int(val):
    try:
        return int(val)
    except (TypeError, ValueError):
        return None

def parse_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None

def safe_alt(val):
    # For alt_baro, treat "ground" or other strings as None
    if isinstance(val, (int, float)):
        return int(val)
    return None


def process_snapshot_file(time_str):
    url = f"{BASE_URL}/{time_str}.json.gz"
    try:
        response = requests.get(url, timeout=10, verify=False)
        if response.status_code != 200:
            print(f"[WARN] Skipping {time_str}: HTTP {response.status_code}")
            return

        # Try decompressing as gzip; if it fails, treat as plain JSON
        try:
            with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
                data = json.load(f)
        except OSError:
            # Not gzipped, treat as plain JSON
            data = json.loads(response.content.decode('utf-8'))

        timestamp = datetime.fromtimestamp(data['now']+1, tz=timezone.utc)
        for ac in data['aircraft']:
            insert_aircraft(timestamp, ac)

        conn.commit()
        print(f"[INFO] Imported {time_str} ({len(data['aircraft'])} aircraft)")

    except Exception as e:
        print(f"[ERROR] {time_str}: {e}")

# === Main ===
def main():
    test_times = generate_sample_timestamps(count=3600)  # 10 minutes = 120, 1hr = 720
    for ts in tqdm(test_times, desc="Importing ADS-B snapshots"):
        process_snapshot_file(ts)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
