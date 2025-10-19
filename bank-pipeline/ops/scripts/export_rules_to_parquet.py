#!/usr/bin/env python3
import duckdb, os, sys, pathlib

ROOT = pathlib.Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "conf" / "sql" / "rules.duckdb"
OUT_DIR = ROOT / "data" / "rules_bundle"

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))

    # Ensure DB is reachable
    con.execute("PRAGMA database_list;")

    # Export normalized tables
    con.execute(f"COPY (SELECT * FROM rules)        TO '{OUT_DIR}/rules.parquet'        (FORMAT PARQUET);")
    con.execute(f"COPY (SELECT * FROM predicates)   TO '{OUT_DIR}/predicates.parquet'   (FORMAT PARQUET);")
    con.execute(f"COPY (SELECT * FROM actions)      TO '{OUT_DIR}/actions.parquet'      (FORMAT PARQUET);")
    con.execute(f"COPY (SELECT * FROM rule_profiles)TO '{OUT_DIR}/rule_profiles.parquet'(FORMAT PARQUET);")

    print(f"✅ Exported Parquet bundle to {OUT_DIR}")

if __name__ == "__main__":
    sys.exit(main())
