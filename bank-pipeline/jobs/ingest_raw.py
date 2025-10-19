\
import argparse, os, hashlib, json, shutil, glob
from pathlib import Path
from pyspark.sql import functions as F
from libs.spark_session import get_spark

def _load_yaml(p):
    import yaml
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

DATE_CANDS = ["post_date","transaction date","transaction_date","date","txn_date","value date","posting date"]
DESC_CANDS = ["description","narration","details","transaction details","remarks"]
AMT_CANDS  = ["amount","amt","transaction amount","value"]
DEBIT_CANDS= ["debit","withdrawal amt.","dr amount","withdrawal amount","dr"]
CREDIT_CANDS=["credit","deposit amt.","cr amount","deposit amount","cr"]
BAL_CANDS  = ["balance","running balance","closing balance"]
CURR_CANDS = ["currency","curr"]
ACCT_CANDS = ["account_id","account number","account no","acct no","account"]

def _sha256_file(path: Path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1<<20), b""):
            h.update(chunk)
    return h.hexdigest()

def _lower_map(cols):
    return {c.lower(): c for c in cols}

def _canonicalize(df):
    cols = _lower_map(df.columns)
    def pick(cands):
        for c in cands:
            if c in cols: return cols[c]
        return None

    date_col  = pick(DATE_CANDS)
    desc_col  = pick(DESC_CANDS)
    amt_col   = pick(AMT_CANDS)
    debit_col = pick(DEBIT_CANDS)
    credit_col= pick(CREDIT_CANDS)
    bal_col   = pick(BAL_CANDS)
    cur_col   = pick(CURR_CANDS)
    acct_col  = pick(ACCT_CANDS)

    tmp = df
    tmp = tmp.withColumn("post_date", F.col(date_col).cast("string") if date_col else F.lit(None).cast("string"))
    tmp = tmp.withColumn("description", F.col(desc_col).cast("string") if desc_col else F.lit(None).cast("string"))

    if amt_col:
        tmp = tmp.withColumn("amount", F.col(amt_col).cast("double"))
    else:
        # Normalize sign: treat debit/credit fields as absolute amounts
        debit  = (F.abs(F.coalesce(F.col(debit_col).cast("double"), F.lit(0.0))) if debit_col else F.lit(0.0))
        credit = (F.abs(F.coalesce(F.col(credit_col).cast("double"), F.lit(0.0))) if credit_col else F.lit(0.0))
        tmp = tmp.withColumn("amount", credit - debit)

    tmp = tmp.withColumn("balance",  F.col(bal_col).cast("double") if bal_col else F.lit(None).cast("double"))
    tmp = tmp.withColumn("currency", F.col(cur_col).cast("string") if cur_col else F.lit(None).cast("string"))
    tmp = tmp.withColumn("account_id", F.col(acct_col).cast("string") if acct_col else F.lit(None).cast("string"))

    return tmp.select("post_date","description","amount","currency","account_id","balance")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--conf", default="conf/pipeline.yaml")
    args = ap.parse_args()

    cfg = _load_yaml(args.conf)
    inbox   = Path(cfg["paths"]["inbox"])
    archive = Path(cfg["paths"]["archive"])
    inbox.mkdir(parents=True, exist_ok=True)
    archive.mkdir(parents=True, exist_ok=True)
    bronze  = cfg["paths"]["bronze"]
    fmt     = cfg["formats"]["lake"]

    spark = get_spark("ingest_raw")
    spark.conf.set("spark.sql.session.timeZone","UTC")

    profiles = [p.name for p in inbox.iterdir() if p.is_dir()]
    for pid in profiles:
        files = sorted(glob.glob(str(inbox / pid / "*.csv")))
        if not files:
            continue

        for fp in files:
            fp_path = Path(fp)
            sha = _sha256_file(fp_path)

            registry = Path(cfg["paths"]["meta"]) / "file_registry" / f"{pid}.jsonl"
            registry.parent.mkdir(parents=True, exist_ok=True)
            already = False
            if registry.exists():
                with open(registry, "r", encoding="utf-8") as r:
                    for line in r:
                        try:
                            j = json.loads(line)
                            if j.get("sha256")==sha:
                                already = True
                                break
                        except: 
                            pass

            if not already:
                df = spark.read.option("header", True).option("inferSchema", True).csv(str(fp_path))
                df = _canonicalize(df).withColumn("profile_id", F.lit(pid))
                (df.write.mode("append").format(fmt).partitionBy("profile_id").save(bronze))
                with open(registry, "a", encoding="utf-8") as w:
                    import json as _json
                    w.write(_json.dumps({"file": fp_path.name, "sha256": sha})+"\n")
            else:
                print(f"[skip] Already ingested: {fp_path.name}")

            # move to archive/YYYY/MM/DD
            from datetime import datetime as _dt
            today = _dt.utcnow()
            dest = archive / pid / f"{today:%Y/%m/%d}"
            dest.mkdir(parents=True, exist_ok=True)
            shutil.move(str(fp_path), str(dest / fp_path.name))
            print(f"[ingest] Archived {fp_path.name} -> {dest}")

    spark.stop()

if __name__ == "__main__":
    main()
