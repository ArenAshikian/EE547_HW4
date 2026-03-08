import os
import json
import argparse
import psycopg2
from psycopg2.extras import RealDictCursor


QUERIES = {
    "Q1": {
        "description": "Route 20 stops in order",
        "sql": """
            SELECT s.stop_name, ls.sequence, ls.time_offset
            FROM line_stops ls
            JOIN lines l ON ls.line_name = l.line_name
            JOIN stops s ON ls.stop_name = s.stop_name
            WHERE l.line_name = 'Route 20'
            ORDER BY ls.sequence;
        """
    },
    "Q2": {
        "description": "Trips during morning rush (7-9 AM)",
        "sql": """
            SELECT trip_id, line_name, scheduled_departure
            FROM trips
            WHERE scheduled_departure::time >= TIME '07:00:00'
              AND scheduled_departure::time < TIME '09:00:00'
            ORDER BY scheduled_departure, trip_id;
        """
    },
    "Q3": {
        "description": "Transfer stops (stops on 2+ routes)",
        "sql": """
            SELECT stop_name, COUNT(DISTINCT line_name) AS line_count
            FROM line_stops
            GROUP BY stop_name
            HAVING COUNT(DISTINCT line_name) >= 2
            ORDER BY line_count DESC, stop_name;
        """
    },
    "Q4": {
        "description": "Complete route for trip T0001",
        "sql": """
            SELECT t.trip_id, se.stop_name, ls.sequence, se.scheduled, se.actual
            FROM trips t
            JOIN stop_events se ON t.trip_id = se.trip_id
            JOIN line_stops ls
              ON t.line_name = ls.line_name
             AND se.stop_name = ls.stop_name
            WHERE t.trip_id = 'T0001'
            ORDER BY ls.sequence;
        """
    },
    "Q5": {
        "description": "Routes serving both Wilshire / Veteran and Le Conte / Broxton",
        "sql": """
            SELECT line_name
            FROM line_stops
            WHERE stop_name IN ('Wilshire / Veteran', 'Le Conte / Broxton')
            GROUP BY line_name
            HAVING COUNT(DISTINCT stop_name) = 2
            ORDER BY line_name;
        """
    },
    "Q6": {
        "description": "Average ridership by line",
        "sql": """
            SELECT t.line_name,
                   AVG(se.passengers_on + se.passengers_off) AS avg_passengers
            FROM trips t
            JOIN stop_events se ON t.trip_id = se.trip_id
            GROUP BY t.line_name
            ORDER BY t.line_name;
        """
    },
    "Q7": {
        "description": "Top 10 busiest stops",
        "sql": """
            SELECT stop_name,
                   SUM(passengers_on + passengers_off) AS total_activity
            FROM stop_events
            GROUP BY stop_name
            ORDER BY total_activity DESC, stop_name
            LIMIT 10;
        """
    },
    "Q8": {
        "description": "Count delays by line (>2 min late)",
        "sql": """
            SELECT t.line_name,
                   COUNT(*) AS delay_count
            FROM trips t
            JOIN stop_events se ON t.trip_id = se.trip_id
            WHERE se.actual > se.scheduled + INTERVAL '2 minutes'
            GROUP BY t.line_name
            ORDER BY delay_count DESC, t.line_name;
        """
    },
    "Q9": {
        "description": "Trips with 3+ delayed stops",
        "sql": """
            SELECT se.trip_id,
                   COUNT(*) AS delayed_stop_count
            FROM stop_events se
            WHERE se.actual > se.scheduled + INTERVAL '2 minutes'
            GROUP BY se.trip_id
            HAVING COUNT(*) >= 3
            ORDER BY delayed_stop_count DESC, se.trip_id;
        """
    },
    "Q10": {
        "description": "Stops with above-average ridership",
        "sql": """
            SELECT stop_name, total_boardings
            FROM (
                SELECT stop_name, SUM(passengers_on) AS total_boardings
                FROM stop_events
                GROUP BY stop_name
            ) s
            WHERE total_boardings > (
                SELECT AVG(stop_total)
                FROM (
                    SELECT SUM(passengers_on) AS stop_total
                    FROM stop_events
                    GROUP BY stop_name
                ) avg_table
            )
            ORDER BY total_boardings DESC, stop_name;
        """
    }
}


def get_connection(args):
    return psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password
    )


def run_query(cur, query_name):
    info = QUERIES[query_name]
    cur.execute(info["sql"])
    rows = cur.fetchall()

    return {
        "query": query_name,
        "description": info["description"],
        "results": rows,
        "count": len(rows)
    }


def print_text(result):
    print(f"{result['query']}: {result['description']}")
    print(f"Count: {result['count']}")
    for row in result["results"]:
        print(dict(row))
    print()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("query", help="Q1-Q10 or all")
    parser.add_argument("--format", choices=["json", "text"], default="text")
    parser.add_argument("--host", default=os.getenv("PGHOST", "db"))
    parser.add_argument("--port", default=os.getenv("PGPORT", "5432"))
    parser.add_argument("--dbname", default=os.getenv("PGDATABASE", "transit"))
    parser.add_argument("--user", default=os.getenv("PGUSER", "transit"))
    parser.add_argument("--password", default=os.getenv("PGPASSWORD", "transit123"))
    args = parser.parse_args()

    query_name = args.query.upper()

    if query_name != "ALL" and query_name not in QUERIES:
        raise ValueError("Query must be one of Q1-Q10 or all")

    conn = None
    try:
        conn = get_connection(args)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if query_name == "ALL":
            all_results = []
            for q in sorted(QUERIES.keys(), key=lambda x: int(x[1:])):
                result = run_query(cur, q)
                all_results.append(result)

            if args.format == "json":
                print(json.dumps(all_results, indent=2, default=str))
            else:
                for result in all_results:
                    print_text(result)
        else:
            result = run_query(cur, query_name)

            if args.format == "json":
                print(json.dumps(result, indent=2, default=str))
            else:
                print_text(result)

        cur.close()
        conn.close()

    except Exception as e:
        if conn is not None:
            conn.close()
        print("Error:", e)
        raise


if __name__ == "__main__":
    main()