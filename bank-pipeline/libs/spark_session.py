from pyspark.sql import SparkSession

def get_spark(app_name: str = "bank-pipeline"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.ui.showConsoleProgress", "true")
        .getOrCreate()
    )
