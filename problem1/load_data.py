# load_data.py
# Purpose: create schema and load all CSVs in correct order.
# Only psycopg2 + stdlib as required.

import argparse, csv, os
import psycopg2

def connect(a):
    # Simple connection wrapper
    return psycopg2.connect(
        host=a.host, port=a.port, dbname=a.dbname, user=a.user, password=a.password
    )

def exec_file(cur, path):
    # Run entire SQL file (idempotent schema with IF NOT EXISTS on indexes)
    with open(path, "r", encoding="utf-8") as f:
        cur.execute(f.read())

def upsert_lines(cur, data_dir):
    path = os.path.join(data_dir, "lines.csv")
    n = 0
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            cur.execute(
                "INSERT INTO lines(line_name, vehicle_type) VALUES (%s,%s) "
                "ON CONFLICT (line_name) DO NOTHING",
                (r["line_name"], r["vehicle_type"]),
            ); n += 1
    return n

def upsert_stops(cur, data_dir):
    path = os.path.join(data_dir, "stops.csv")
    n = 0
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            cur.execute(
                "INSERT INTO stops(stop_name, latitude, longitude) VALUES (%s,%s,%s) "
                "ON CONFLICT (stop_name) DO NOTHING",
                (r["stop_name"], float(r["latitude"]), float(r["longitude"])),
            ); n += 1
    return n

def maps(cur):
    # Cache name→id lookups for fast inserts
    cur.execute("SELECT line_id, line_name FROM lines")
    line_map = {name: lid for (lid, name) in cur.fetchall()}
    cur.execute("SELECT stop_id, stop_name FROM stops")
    stop_map = {name: sid for (sid, name) in cur.fetchall()}
    return line_map, stop_map

def load_line_stops(cur, data_dir, line_map, stop_map):
    path = os.path.join(data_dir, "line_stops.csv")
    n = 0
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            cur.execute(
                "INSERT INTO line_stops(line_id, stop_id, sequence, time_offset) "
                "VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (line_map[r["line_name"]], stop_map[r["stop_name"]],
                 int(r["sequence"]), int(r["time_offset"])),
            ); n += 1
    return n

def load_trips(cur, data_dir, line_map):
    path = os.path.join(data_dir, "trips.csv")
    n = 0
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            cur.execute(
                "INSERT INTO trips(trip_id, line_id, scheduled_departure, vehicle_id) "
                "VALUES (%s,%s,%s,%s) ON CONFLICT (trip_id) DO NOTHING",
                (r["trip_id"], line_map[r["line_name"]], r["scheduled_departure"], r["vehicle_id"]),
            ); n += 1
    return n

def load_stop_events(cur, data_dir, stop_map):
    path = os.path.join(data_dir, "stop_events.csv")
    n = 0
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            cur.execute(
                "INSERT INTO stop_events(trip_id, stop_id, scheduled, actual, passengers_on, passengers_off) "
                "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (r["trip_id"], stop_map[r["stop_name"]], r["scheduled"], r["actual"],
                 int(r["passengers_on"]), int(r["passengers_off"])),
            ); n += 1
    return n

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=5432)
    ap.add_argument("--dbname", default="transit")
    ap.add_argument("--user", default="transit")
    ap.add_argument("--password", default="transit123")
    ap.add_argument("--data_dir", default="data")
    ap.add_argument("--schema", default="schema.sql")
    a = ap.parse_args()

    with connect(a) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            print("Creating schema…")
            exec_file(cur, a.schema)

            print("Loading lines…"); n1 = upsert_lines(cur, a.data_dir)
            print("Loading stops…"); n2 = upsert_stops(cur, a.data_dir)
            line_map, stop_map = maps(cur)

            print("Loading line_stops…"); n3 = load_line_stops(cur, a.data_dir, line_map, stop_map)
            print("Loading trips…"); n4 = load_trips(cur, a.data_dir, line_map)
            print("Loading stop_events…"); n5 = load_stop_events(cur, a.data_dir, stop_map)

            print(f"Done. rows inserted (attempted): {n1+n2+n3+n4+n5}")

if __name__ == "__main__":
    main()
