import argparse, yaml, os, shutil
from libs.spark_session import get_spark
from libs.simple_rules_jsonl import load_rules_dir
from libs.apply_simple_rules import apply_rules

def _load_yaml(p):
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--conf", default="conf/pipeline.yaml")
    args = ap.parse_args()
    cfg = _load_yaml(args.conf)

    rules_dir = cfg["rules"]["dir"]  # e.g., conf/rules
    silver = cfg["paths"]["silver"]
    fmt = cfg["formats"]["lake"]

    spark = get_spark("categorize_simple")
    df = spark.read.format(fmt).load(silver)

    rules = load_rules_dir(rules_dir)
    out = apply_rules(df, rules)

    tmp_path = f"{silver}_tmp"
    # Write to a temp location to avoid read/write conflicts on the same path
    (out.write.mode("overwrite")
         .format(fmt).partitionBy("profile_id","year","month")
         .save(tmp_path))

    # Replace silver with the temp output atomically (best-effort on local FS)
    if os.path.exists(silver):
        shutil.rmtree(silver)
    os.rename(tmp_path, silver)
    spark.stop()

if __name__ == "__main__":
    main()
