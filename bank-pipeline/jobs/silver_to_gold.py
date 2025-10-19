\
import argparse
from pyspark.sql import functions as F
from libs.spark_session import get_spark

def _load_yaml(p):
    import yaml
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--conf", default="conf/pipeline.yaml")
    args = ap.parse_args()
    cfg = _load_yaml(args.conf)

    spark  = get_spark("silver_to_gold")
    silver = cfg["paths"]["silver"]
    gold   = cfg["paths"]["gold"]
    fmt    = cfg["formats"]["lake"]

    df = spark.read.format(fmt).load(silver)

    fact_tx = df.select(
        "profile_id","txn_id","account_id","post_date","year","month",
        "amount","currency","name","clean_desc","merchant",
        "category","subcategory","applied_rules","operation","is_transfer","balance"
    )

    (fact_tx.write.mode("overwrite").format(fmt).partitionBy("profile_id","year","month").save(f"{gold}/fact_transactions"))

    agg = fact_tx.groupBy("profile_id","year","month","category","subcategory").agg(F.sum("amount").alias("total_amount"))
    (agg.write.mode("overwrite").format(fmt).partitionBy("profile_id","year","month").save(f"{gold}/agg_category_monthly"))

    spark.stop()

if __name__ == "__main__":
    main()
