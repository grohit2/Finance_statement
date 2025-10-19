\
import argparse, yaml, csv, os

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profiles", default="conf/profiles.yaml")
    ap.add_argument("--out", default="data/exports/csv/share_index.csv")
    args = ap.parse_args()

    with open(args.profiles, "r", encoding="utf-8") as f:
        profs = yaml.safe_load(f)["profiles"]

    rows = []
    for p in profs:
        rows.append((p["id"], p["id"]))
        for v in p.get("share_with", []):
            rows.append((p["id"], v))

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["owner_profile_id","viewer_profile_id"])
        w.writerows(rows)

    print(f"wrote {args.out}")

if __name__ == "__main__":
    main()
