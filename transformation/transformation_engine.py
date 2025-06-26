from pyspark.sql.functions import col, lit, current_date

def canonicalize_columns(df, mappings: dict):
    for src, tgt in mappings.items():
        if src in df.columns:
            df = df.withColumnRenamed(src, tgt)
    return df

def enrich(df, store_name: str):
    return df.withColumn("store", lit(store_name)) \
             .withColumn("ingest_date", current_date())
