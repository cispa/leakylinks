import argparse
import csv
import sys
from datetime import datetime
from typing import List, Dict, DefaultDict

import psycopg2
from psycopg2.extras import execute_values

from config.settings import LIVE_REM_DB_CONFIG


def get_connection():
    return psycopg2.connect(**LIVE_REM_DB_CONFIG)


def load_rows(csv_path: str) -> List[Dict[str, str]]:
    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        required = {"source_table", "source_id", "page_url"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV missing required columns: {', '.join(sorted(missing))}")
        rows = []
        for r in reader:
            rows.append({
                "source_table": r["source_table"].strip(),
                "source_id": int(r["source_id"].strip()),
                "page_url": r["page_url"].strip(),
                "result_url": (r.get("result_url") or "").strip() or None,
            })
        return rows


def group_by_table(rows: List[Dict[str, str]]) -> DefaultDict[str, List[Dict[str, str]]]:
    from collections import defaultdict
    grouped = defaultdict(list)
    for r in rows:
        grouped[r["source_table"]].append(r)
    return grouped


def upsert_source_minimal(cur, rows: List[Dict[str, str]], dry_run: bool = False) -> int:
    if not rows:
        return 0
    count = 0
    grouped = group_by_table(rows)
    for table, trs in grouped.items():
        insert_sql = f"""
            INSERT INTO {table} (id, page_url, result_url, "time", method, source)
            VALUES %s
            ON CONFLICT (id) DO NOTHING
        """
        values = [
            (r["source_id"], r["page_url"], r["result_url"], 'api', 'fake')
            for r in trs
        ]
        template = "(%s, %s, %s, now(), %s, %s)"
        if dry_run:
            count += len(values)
        else:
            execute_values(cur, insert_sql, values, template=template)
            count += len(values)
    return count


def clear_task_phase_status(cur):
    cur.execute("DELETE FROM task_phase_status;")


def clear_all_tables(cur):
    """Delete content from all tables in the database."""
    # Query to get all user tables (excluding system tables)
    cur.execute("""
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public'
        ORDER BY tablename;
    """)
    tables = [row[0] for row in cur.fetchall()]
    
    for table in tables:
        cur.execute(f"DELETE FROM {table};")
    
    return tables


def show_saved_data(cur, limit: int = 10, verbose: bool = False):
    """Display what data has been saved in the database."""
    source_tables = [
        'urlscan_results',
        'cloudflare_results',
        'anyrun_results',
        'hybrid_analysis_results',
        'joe_results',
        'urlquery_results'
    ]
    
    print("\n" + "="*80)
    print("SAVED DATA SUMMARY")
    print("="*80)
    
    # Count source tables
    print("\n--- Source Tables ---")
    total_source = 0
    for table in source_tables:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE source = 'fake';")
        count = cur.fetchone()[0]
        total_source += count
        if count > 0 or verbose:
            print(f"  {table:30s}: {count:6d} rows (source='fake')")
    print(f"  {'TOTAL':30s}: {total_source:6d} rows")
    
    # Count analysis_output
    print("\n--- Analysis Output ---")
    cur.execute("SELECT COUNT(*) FROM analysis_output;")
    ao_count = cur.fetchone()[0]
    print(f"  Total entries: {ao_count}")
    
    if ao_count > 0:
        cur.execute("""
            SELECT source_table, COUNT(*) 
            FROM analysis_output 
            GROUP BY source_table 
            ORDER BY source_table;
        """)
        for row in cur.fetchall():
            print(f"    {row[0]:30s}: {row[1]:6d} entries")
    
    # Count task_phase_status
    print("\n--- Task Phase Status ---")
    cur.execute("SELECT COUNT(*) FROM task_phase_status;")
    tps_count = cur.fetchone()[0]
    print(f"  Total entries: {tps_count}")
    
    if tps_count > 0:
        cur.execute("""
            SELECT phase, status, COUNT(*) 
            FROM task_phase_status 
            GROUP BY phase, status 
            ORDER BY phase, status;
        """)
        for row in cur.fetchall():
            print(f"    {row[0]:15s} {row[1]:15s}: {row[2]:6d} entries")
    
    # Show sample rows if requested
    if verbose and total_source > 0:
        print("\n--- Sample Rows (first {} per table) ---".format(limit))
        for table in source_tables:
            cur.execute(f"""
                SELECT id, page_url, result_url, source, "time"
                FROM {table}
                WHERE source = 'fake'
                ORDER BY id
                LIMIT %s;
            """, (limit,))
            rows = cur.fetchall()
            if rows:
                print(f"\n  {table}:")
                for r in rows:
                    page_url_short = r[1][:60] + "..." if r[1] and len(r[1]) > 60 else (r[1] or "")
                    result_url_short = r[2][:40] + "..." if r[2] and len(r[2]) > 40 else (r[2] or "")
                    print(f"    ID: {r[0]:6d} | URL: {page_url_short}")
                    if result_url_short:
                        print(f"           Result: {result_url_short}")
    
    print("\n" + "="*80 + "\n")


def example_rows() -> List[Dict[str, str]]:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ex = [
          {"source_table": "urlscan_results", "source_id": 11, "page_url": "https://sheep-savvy.com/index.html", "result_url": "https://urlscanner.scan/sample/f4kef4ke"},
  {"source_table": "hybrid_analysis_results", "source_id": 19, "page_url": "https://sheep-savvy.com/app/client/7f8a3b9d-21c0-4a8e-9f1e-2e718a8ffabc/synthetic-pii-test.html?authKey=ak_live_FAKE_9b0a0fca6734f2b1f86f&session=eyJpZCI6IjEyMyIsImV4cCI6MTk5OTk5OTk5OX0.YWJj&scope=read%3Aall%20write%3Anone", "result_url": "https://urlscanner.scan/sample/f4kef4ke"},
  {"source_table": "cloudflare_results", "source_id": 20, "page_url": "https://sheep-savvy.com/app/flight/booking/a7k9m2/index.html", "result_url": "https://urlscanner.scan/sample/f4kef4ke"},
  {"source_table": "joe_results", "source_id": 21, "page_url": "https://sheep-savvy.com/app/product-order/k7m9p2x4n8q1/product.html", "result_url": "https://urlscanner.scan/sample/f4kef4ke"},
    ]
    return ex


def generate_stress_rows(n: int) -> List[Dict[str, str]]:
    base_urls = [
        {
            "page_url": "https://example.com/login",
            "result_url": "https://urlscan.io/api/v1/result/0195f211-31aa-77ae-8d5d-41e234caf868/",
            "source_table": "urlscan_results",
        },
        {
            "page_url": "https://omgIamstressed.gov",
            "result_url": "https://urlscan.io/api/v1/result/635fcdce-9687-46e2-aab9-526ce9669c66/",
            "source_table": "urlscan_results",
        },
    ]
    out: List[Dict[str, str]] = []
    for i in range(n):
        base = base_urls[i % len(base_urls)]
        variations = [
            f"{base['page_url']}?id={i}",
            f"{base['page_url']}#{i}",
            f"{base['page_url']}?session={i}",
            f"{base['page_url']}?token={i}",
            f"{base['page_url']}?state={i}",
        ]
        page_url = variations[i % len(variations)]
        out.append({
            "source_table": base["source_table"],
            "source_id": 1000000 + i,
            "page_url": page_url,
            "result_url": base["result_url"],
        })
    return out


def upsert_analysis_output(cur, rows: List[Dict[str, str]], dry_run: bool = False) -> int:
    if not rows:
        return 0
    insert_sql = """
        INSERT INTO analysis_output (source_table, source_id, page_url, result_url, created_at)
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    values = [(r["source_table"], r["source_id"], r["page_url"], r["result_url"]) for r in rows]
    template = "(%s, %s, %s, %s, now())"
    if dry_run:
        return len(values)
    execute_values(cur, insert_sql, values, template=template)
    return len(values)


def upsert_task_phase_status(cur, rows: List[Dict[str, str]], dry_run: bool = False) -> int:
    if not rows:
        return 0
    insert_sql = """
        INSERT INTO task_phase_status (source_table, source_id, phase, status)
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    # Use 'live_crawl' as the initial phase to match pipeline expectations
    values = [(r["source_table"], r["source_id"], 'live_crawl', 'PENDING') for r in rows]
    if dry_run:
        return len(values)
    execute_values(cur, insert_sql, values)
    return len(values)


def run(csv_path: str, dry_run: bool, verbose: bool) -> None:
    rows = load_rows(csv_path)
    if verbose:
        print(f"Loaded {len(rows)} rows from {csv_path}")
    conn = get_connection()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            src = upsert_source_minimal(cur, rows, dry_run)
            ao = upsert_analysis_output(cur, rows, dry_run)
            tps = upsert_task_phase_status(cur, rows, dry_run)
            if verbose:
                print(f"source tables inserted (minimal): {src}")
                print(f"analysis_output inserted: {ao}")
                print(f"task_phase_status inserted: {tps}")
        if dry_run:
            conn.rollback()
            if verbose:
                print("[Dry-run] Rolled back changes")
        else:
            conn.commit()
            if verbose:
                print("[Commit] Changes committed")
    finally:
        conn.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Fake plugin and test data manager: fill/clear/examples/stress")
    subparsers = parser.add_subparsers(dest="cmd")

    fill_p = subparsers.add_parser("fill", help="Fill from CSV into source+analysis_output+task_phase_status")
    fill_p.add_argument("--file", default="../fake_csv/fake.csv", help="CSV: source_table,source_id,page_url[,result_url]")
    fill_p.add_argument("--dry-run", action="store_true")
    fill_p.add_argument("--verbose", action="store_true")

    ex_p = subparsers.add_parser("examples", help="Insert built-in example rows")
    ex_p.add_argument("--dry-run", action="store_true")
    ex_p.add_argument("--verbose", action="store_true")

    stress_p = subparsers.add_parser("stress", help="Insert generated stress-test rows")
    stress_p.add_argument("--num", type=int, default=100)
    stress_p.add_argument("--dry-run", action="store_true")
    stress_p.add_argument("--verbose", action="store_true")

    clear_p = subparsers.add_parser("clear", help="Clear task_phase_status only")
    clear_p.add_argument("--verbose", action="store_true")

    clearw_p = subparsers.add_parser("clear-wild", help="Clear task_phase_status and all scraped tables")
    clearw_p.add_argument("--verbose", action="store_true")

    show_p = subparsers.add_parser("show", help="Show saved data summary")
    show_p.add_argument("--limit", type=int, default=10, help="Number of sample rows to show per table (default: 10)")
    show_p.add_argument("--verbose", action="store_true", help="Show sample rows from each table")

    args = parser.parse_args(argv)

    if not args.cmd:
        parser.print_help()
        return 0

    conn = get_connection()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            if args.cmd == "fill":
                rows = load_rows(args.file)
                if args.verbose:
                    print(f"Loaded {len(rows)} rows from {args.file}")
                src = upsert_source_minimal(cur, rows, args.dry_run)
                ao = upsert_analysis_output(cur, rows, args.dry_run)
                tps = upsert_task_phase_status(cur, rows, args.dry_run)
                if args.verbose:
                    print(f"source tables inserted (minimal): {src}")
                    print(f"analysis_output inserted: {ao}")
                    print(f"task_phase_status inserted: {tps}")
            elif args.cmd == "examples":
                rows = example_rows()
                src = upsert_source_minimal(cur, rows, args.dry_run)
                ao = upsert_analysis_output(cur, rows, args.dry_run)
                tps = upsert_task_phase_status(cur, rows, args.dry_run)
                if args.verbose:
                    print(f"Inserted examples -> source:{src}, ao:{ao}, tps:{tps}")
            elif args.cmd == "stress":
                rows = generate_stress_rows(args.num)
                src = upsert_source_minimal(cur, rows, args.dry_run)
                ao = upsert_analysis_output(cur, rows, args.dry_run)
                tps = upsert_task_phase_status(cur, rows, args.dry_run)
                if args.verbose:
                    print(f"Inserted stress {args.num} -> source:{src}, ao:{ao}, tps:{tps}")
            elif args.cmd == "clear":
                clear_task_phase_status(cur)
                if args.verbose:
                    print("Cleared task_phase_status")
            elif args.cmd == "clear-wild":
                cleared_tables = clear_all_tables(cur)
                if args.verbose:
                    print(f"Cleared all tables: {', '.join(cleared_tables)}")
                else:
                    print(f"Cleared {len(cleared_tables)} tables")
            elif args.cmd == "show":
                show_saved_data(cur, limit=args.limit, verbose=args.verbose)

        if getattr(args, 'dry_run', False):
            conn.rollback()
            if getattr(args, 'verbose', False):
                print("[Dry-run] Rolled back changes")
        else:
            conn.commit()
            if getattr(args, 'verbose', False):
                print("[Commit] Changes committed")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())


