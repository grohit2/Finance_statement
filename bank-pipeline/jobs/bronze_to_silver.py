\
import argparse
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from libs.spark_session import get_spark
from libs.keys import txn_id as txn_key
from dateutil import parser as dateparser

def _load_yaml(p):
    import yaml
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def _parse_date(s):
    if s is None: 
        return None
    try:
        return dateparser.parse(str(s), dayfirst=False, yearfirst=True).date().isoformat()
    except:
        try:
            return dateparser.parse(str(s), dayfirst=True).date().isoformat()
        except:
            return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--conf", default="conf/pipeline.yaml")
    args = ap.parse_args()
    cfg = _load_yaml(args.conf)

    spark = get_spark("bronze_to_silver")
    spark.udf.register("txn_id_udf", lambda pid, acc, d, amt, desc: txn_key(pid or "", acc or "", d or "", float(amt or 0.0), desc or ""), "string")
    spark.udf.register("parse_date", _parse_date, "string")

    bronze = cfg["paths"]["bronze"]
    silver = cfg["paths"]["silver"]
    fmt    = cfg["formats"]["lake"]

    df = spark.read.format(fmt).load(bronze)

    df = df.withColumn("post_date", F.to_date(F.expr("parse_date(post_date)"))) \
           .withColumn("amount", F.col("amount").cast("double")) \
           .withColumn("currency", F.col("currency").cast("string")) \
           .withColumn("account_id", F.col("account_id").cast("string")) \
           .withColumn("description", F.col("description").cast("string")) \
           .withColumn("name", F.col("description")) \
           .withColumn("balance", F.col("balance").cast("double")) \
           .withColumn("profile_id", F.col("profile_id").cast("string"))

    df = df.withColumn("clean_desc", F.lower(F.regexp_replace(F.col("description"), r"\s+", " ")))

    df = df.withColumn("txn_id", F.expr("txn_id_udf(profile_id, account_id, CAST(post_date AS STRING), amount, clean_desc)")) \
           .withColumn("year", F.year("post_date")) \
           .withColumn("month", F.month("post_date"))

    w = Window.partitionBy("txn_id").orderBy(F.col("post_date").desc_nulls_last())
    df = df.withColumn("rnk", F.row_number().over(w)).filter("rnk = 1").drop("rnk")

    df = df.withColumn("merchant", F.lit(None).cast("string"))
    df = df.withColumn("category", F.lit(None).cast("string"))
    df = df.withColumn("is_transfer", F.lit(False).cast("boolean"))

    (df.write.mode("overwrite").format(fmt).partitionBy("profile_id","year","month").save(silver))
    spark.stop()

if __name__ == "__main__":
    main()
