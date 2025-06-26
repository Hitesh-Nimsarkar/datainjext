import yaml
from pyspark.sql.functions import col, lit, current_date

def load_config(config_path):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def read_data(spark, path, fmt):
    return spark.read.option("header", True).format(fmt).load(path)

def canonicalize_columns(df, column_mappings):
    for old, new in column_mappings.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df

def apply_rules(df, rules):
    for col_name in rules.get("drop_nulls", []):
        df = df.filter(col(col_name).isNotNull())
    return df

def enrich_data(df, store_name):
    return df.withColumn("store", lit(store_name)) \
             .withColumn("ingest_date", current_date())

def write_data(df, target_table):
    df.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("append") \
        .saveAsTable(target_table)
