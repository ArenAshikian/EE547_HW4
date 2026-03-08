import os
import csv
import time
import argparse
import psycopg2


def get_connection(args):
    return psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password
    )


def wait_for_db(args, retries=20, delay=2):
    last_error = None
    for _ in range(retries):
        try:
            conn = get_connection(args)
            return conn
        except Exception as e:
            last_error = e
            time.sleep(delay)
    raise last_error


def run_schema(cur, schema_path):
    with open(schema_path, "r", encoding="utf-8") as f:
        sql = f.read()
    cur.execute(sql)


def clean(value):
    if value is None:
        return None
    value = value.strip()
    if value == "":
        return None
    return value


def load_lines(cur, path):
    count = 0
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cur.execute(
                """
                INSERT INTO lines (line_name, vehicle_type)
                VALUES (%s, %s)
                """,
                (
                    clean(row["line_name"]),
                    clean(row["vehicle_type"]),
                )
            )
            count += 1
    return count


def load_stops(cur, path):
    count = 0
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cur.execute(
                """
                INSERT INTO stops (stop_name, latitude, longitude)
                VALUES (%s, %s, %s)
                ON CONFLICT (stop_name) DO NOTHING
                """,
                (
                    clean(row["stop_name"]),
                    float(clean(row["latitude"])),
                    float(clean(row["longitude"])),
                )
            )
            count += 1
    return count


def load_line_stops(cur, path):
    count = 0
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cur.execute(
                """
                INSERT INTO line_stops (line_name, stop_name, sequence, time_offset)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (
                    clean(row["line_name"]),
                    clean(row["stop_name"]),
                    int(clean(row["sequence"])),
                    int(clean(row["time_offset"])),
                )
            )
            count += 1
    return count


def load_trips(cur, path):
    count = 0
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cur.execute(
                """
                INSERT INTO trips (trip_id, line_name, scheduled_departure, vehicle_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (trip_id) DO NOTHING
                """,
                (
                    clean(row["trip_id"]),
                    clean(row["line_name"]),
                    clean(row["scheduled_departure"]),
                    clean(row["vehicle_id"]),
                )
            )
            count += 1
    return count


def load_stop_events(cur, path):
    count = 0
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cur.execute(
                """
                INSERT INTO stop_events
                (trip_id, stop_name, scheduled, actual, passengers_on, passengers_off)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (trip_id, stop_name, scheduled) DO NOTHING
                """,
                (
                    clean(row["trip_id"]),
                    clean(row["stop_name"]),
                    clean(row["scheduled"]),
                    clean(row["actual"]),
                    int(clean(row["passengers_on"])),
                    int(clean(row["passengers_off"])),
                )
            )
            count += 1
    return count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--datadir", required=True, help="Path to directory containing CSV files")
    parser.add_argument("--schema", default="schema.sql", help="Path to schema.sql")
    parser.add_argument("--host", default=os.getenv("PGHOST", "db"))
    parser.add_argument("--port", default=os.getenv("PGPORT", "5432"))
    parser.add_argument("--dbname", default=os.getenv("PGDATABASE", "transit"))
    parser.add_argument("--user", default=os.getenv("PGUSER", "transit"))
    parser.add_argument("--password", default=os.getenv("PGPASSWORD", "transit123"))
    args = parser.parse_args()

    conn = None
    try:
        conn = wait_for_db(args)
        cur = conn.cursor()

        print(f"Connected to {args.dbname}@{args.host}")
        print("Creating schema...")

        run_schema(cur, args.schema)
        conn.commit()

        print("Tables created: lines, stops, line_stops, trips, stop_events")
        print()

        total = 0

        lines_path = os.path.join(args.datadir, "lines.csv")
        stops_path = os.path.join(args.datadir, "stops.csv")
        line_stops_path = os.path.join(args.datadir, "line_stops.csv")
        trips_path = os.path.join(args.datadir, "trips.csv")
        stop_events_path = os.path.join(args.datadir, "stop_events.csv")

        count = load_lines(cur, lines_path)
        conn.commit()
        print(f"Loading {lines_path}... {count} rows")
        total += count

        count = load_stops(cur, stops_path)
        conn.commit()
        print(f"Loading {stops_path}... {count} rows")
        total += count

        count = load_line_stops(cur, line_stops_path)
        conn.commit()
        print(f"Loading {line_stops_path}... {count} rows")
        total += count

        count = load_trips(cur, trips_path)
        conn.commit()
        print(f"Loading {trips_path}... {count} rows")
        total += count

        count = load_stop_events(cur, stop_events_path)
        conn.commit()
        print(f"Loading {stop_events_path}... {count} rows")
        total += count

        print()
        print(f"Total: {total:,} rows loaded")

        cur.close()
        conn.close()

    except Exception as e:
        if conn is not None:
            conn.rollback()
            conn.close()
        print("Error:", e)
        raise


if __name__ == "__main__":
    main()