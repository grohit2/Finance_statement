\
import argparse
from pyspark.sql import functions as F
from libs.spark_session import get_spark
from libs.io_utils import export_single_csv

def _load_yaml(p):
    import yaml
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--conf", default="conf/pipeline.yaml")
    args = ap.parse_args()
    cfg = _load_yaml(args.conf)

    spark = get_spark("export_csv")
    gold = cfg["paths"]["gold"]
    fmt  = cfg["formats"]["lake"]

    tx  = spark.read.format(fmt).load(f"{gold}/fact_transactions")
    agg = spark.read.format(fmt).load(f"{gold}/agg_category_monthly")

    profiles = [r["profile_id"] for r in tx.select("profile_id").distinct().collect()]
    for pid in profiles:
        dft = (tx.filter(F.col("profile_id")==pid)
                 .select("profile_id","txn_id","account_id","post_date","amount","currency",
                         "name","clean_desc","merchant","category","subcategory","operation","is_transfer"))
        export_single_csv(dft, f"{cfg['exports']['transactions']['path']}/profile={pid}",
                          cfg['exports']['transactions']['filename'])

        dfa = (agg.filter(F.col("profile_id")==pid)
                 .select("profile_id","year","month","category","subcategory","total_amount"))
        export_single_csv(dfa, f"{cfg['exports']['category_monthly']['path']}/profile={pid}",
                          cfg['exports']['category_monthly']['filename'])

        # Spend-only view (outflows as positive amounts)
        spend = (agg.filter((F.col("profile_id")==pid) & (F.col("total_amount") < 0))
                    .select(
                        "profile_id",
                        "year",
                        "month",
                        "category",
                        "subcategory",
                        (-F.col("total_amount")).alias("spend")
                    ))
        spend_out_dir = f"{cfg['paths']['exports_csv']}/category_spend_only/profile={pid}"
        export_single_csv(spend, spend_out_dir, "category_spend_only.csv")

    spark.stop()

if __name__ == "__main__":
    main()
