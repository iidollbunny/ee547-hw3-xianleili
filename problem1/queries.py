# queries.py
# Purpose: run Q1..Q10 and print JSON. Fast queries via indexes in schema.sql

import argparse, json, psycopg2

DESCRIPTIONS = {
  "Q1": "List all stops on Route 20 in order",
  "Q2": "Trips during morning rush (7–9 AM)",
  "Q3": "Transfer stops (stops on ≥2 routes)",
  "Q4": "Ordered stops (with offsets) for trip T0001",
  "Q5": "Routes serving both Wilshire/Veteran and Le Conte/Broxton",
  "Q6": "Average ridership by line",
  "Q7": "Top 10 busiest stops (board+alight)",
  "Q8": "Late events (>2 min) count per line",
  "Q9": "Trips with ≥3 delayed stops",
  "Q10":"Stops with above-average boardings"
}

def connect(a):
    return psycopg2.connect(
        host=a.host, port=a.port, dbname=a.dbname, user=a.user, password=a.password
    )

def fetch_dicts(cur):
    cols = [d.name for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

def run(cur, q):
    # Note: fixed constants match the assignment’s sample values
    if q == "Q1":
        cur.execute("""
          SELECT s.stop_name, ls.sequence, ls.time_offset
          FROM line_stops ls
          JOIN lines l ON l.line_id = ls.line_id
          JOIN stops s ON s.stop_id = ls.stop_id
          WHERE l.line_name = 'Route 20'
          ORDER BY ls.sequence;
        """)
    elif q == "Q2":
        cur.execute("""
          SELECT t.trip_id, l.line_name, t.scheduled_departure
          FROM trips t
          JOIN lines l ON l.line_id = t.line_id
          WHERE EXTRACT(HOUR FROM t.scheduled_departure) BETWEEN 7 AND 8
          ORDER BY t.scheduled_departure;
        """)
    elif q == "Q3":
        cur.execute("""
          SELECT s.stop_name, COUNT(DISTINCT l.line_id) AS line_count
          FROM line_stops ls
          JOIN stops s ON s.stop_id = ls.stop_id
          JOIN lines l ON l.line_id = ls.line_id
          GROUP BY s.stop_id, s.stop_name
          HAVING COUNT(DISTINCT l.line_id) >= 2
          ORDER BY line_count DESC, s.stop_name;
        """)
    elif q == "Q4":
        cur.execute("""
          SELECT s.stop_name, ls.sequence, ls.time_offset
          FROM trips t
          JOIN line_stops ls ON ls.line_id = t.line_id
          JOIN stops s ON s.stop_id = ls.stop_id
          WHERE t.trip_id = 'T0001'
          ORDER BY ls.sequence;
        """)
    elif q == "Q5":
        cur.execute("""
          WITH target AS (
            SELECT stop_id FROM stops
            WHERE stop_name IN ('Wilshire / Veteran','Le Conte / Broxton')
          )
          SELECT l.line_name
          FROM line_stops ls
          JOIN lines l ON l.line_id = ls.line_id
          WHERE ls.stop_id IN (SELECT stop_id FROM target)
          GROUP BY l.line_id, l.line_name
          HAVING COUNT(DISTINCT ls.stop_id) = 2
          ORDER BY l.line_name;
        """)
    elif q == "Q6":
        cur.execute("""
          SELECT l.line_name,
                 ROUND(AVG(se.passengers_on + se.passengers_off)::numeric, 2) AS avg_passengers
          FROM stop_events se
          JOIN trips t ON t.trip_id = se.trip_id
          JOIN lines l ON l.line_id = t.line_id
          GROUP BY l.line_id, l.line_name
          ORDER BY avg_passengers DESC, l.line_name;
        """)
    elif q == "Q7":
        cur.execute("""
          SELECT s.stop_name,
                 SUM(se.passengers_on + se.passengers_off) AS total_activity
          FROM stop_events se
          JOIN stops s ON s.stop_id = se.stop_id
          GROUP BY s.stop_id, s.stop_name
          ORDER BY total_activity DESC, s.stop_name
          LIMIT 10;
        """)
    elif q == "Q8":
        cur.execute("""
          SELECT l.line_name, COUNT(*) AS delay_count
          FROM stop_events se
          JOIN trips t ON t.trip_id = se.trip_id
          JOIN lines l ON l.line_id = t.line_id
          WHERE se.actual > se.scheduled + INTERVAL '2 minutes'
          GROUP BY l.line_id, l.line_name
          ORDER BY delay_count DESC, l.line_name;
        """)
    elif q == "Q9":
        cur.execute("""
          SELECT se.trip_id, COUNT(*) AS delayed_stop_count
          FROM stop_events se
          WHERE se.actual > se.scheduled + INTERVAL '2 minutes'
          GROUP BY se.trip_id
          HAVING COUNT(*) >= 3
          ORDER BY delayed_stop_count DESC, se.trip_id;
        """)
    elif q == "Q10":
        cur.execute("""
          WITH per_stop AS (
            SELECT s.stop_id, s.stop_name,
                   SUM(se.passengers_on) AS total_boardings
            FROM stop_events se
            JOIN stops s ON s.stop_id = se.stop_id
            GROUP BY s.stop_id, s.stop_name
          ),
          avg_all AS (SELECT AVG(total_boardings) AS avg_b FROM per_stop)
          SELECT p.stop_name, p.total_boardings
          FROM per_stop p, avg_all a
          WHERE p.total_boardings > a.avg_b
          ORDER BY p.total_boardings DESC, p.stop_name;
        """)
    else:
        raise ValueError("Unknown query name")

    rows = fetch_dicts(cur)
    return {"query": q, "description": DESCRIPTIONS[q], "count": len(rows), "results": rows}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=5432)
    ap.add_argument("--dbname", default="transit")
    ap.add_argument("--user", default="transit")
    ap.add_argument("--password", default="transit123")
    ap.add_argument("--query", choices=[f"Q{i}" for i in range(1, 11)])
    ap.add_argument("--all", action="store_true")
    a = ap.parse_args()

    with connect(a) as conn, conn.cursor() as cur:
        if a.all:
            out = [run(cur, f"Q{i}") for i in range(1, 11)]
        else:
            out = [run(cur, a.query)]
        print(json.dumps(out if a.all else out[0], default=str, indent=2))

if __name__ == "__main__":
    main()
