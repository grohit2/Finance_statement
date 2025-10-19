#!/usr/bin/env python3
import argparse, json, re, sys
from pathlib import Path

REQ = ("id","description","op","value","category","subcategory")

def lint_rules(dir_path: str) -> int:
    base = Path(dir_path)
    if not base.exists():
        print(f"ERROR: rules dir not found: {dir_path}")
        return 2
    errors = 0
    warnings = 0
    seen_ids = {}
    seen_patterns = {}
    for fp in sorted(base.glob("*.ndjson")):
        with fp.open("r", encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                try:
                    obj = json.loads(s)
                except json.JSONDecodeError as e:
                    print(f"ERROR {fp.name}:{i}: invalid JSON: {e}")
                    errors += 1
                    continue
                for k in REQ:
                    if k not in obj:
                        print(f"ERROR {fp.name}:{i}: missing field '{k}'")
                        errors += 1
                if obj.get("op","regex").lower() != "regex":
                    print(f"ERROR {fp.name}:{i}: only op='regex' supported; got '{obj.get('op')}'")
                    errors += 1
                pat = obj.get("value", "")
                try:
                    re.compile(pat)
                except re.error as e:
                    print(f"ERROR {fp.name}:{i}: regex does not compile: {e}")
                    errors += 1
                rid = obj.get("id")
                if rid in seen_ids:
                    print(f"WARN  {fp.name}:{i}: duplicate id '{rid}' also in {seen_ids[rid]}")
                    warnings += 1
                else:
                    seen_ids[rid] = f"{fp.name}:{i}"
                if pat in seen_patterns:
                    print(f"WARN  {fp.name}:{i}: duplicate regex pattern also in {seen_patterns[pat]}")
                    warnings += 1
                else:
                    seen_patterns[pat] = f"{fp.name}:{i}"
    if errors == 0:
        print(f"OK: rules valid ({len(seen_ids)} rules, {warnings} warnings)")
        return 0
    print(f"FAIL: {errors} error(s), {warnings} warning(s)")
    return 2

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dir", nargs="?", default="conf/rules")
    args = ap.parse_args()
    sys.exit(lint_rules(args.dir))

if __name__ == "__main__":
    main()

