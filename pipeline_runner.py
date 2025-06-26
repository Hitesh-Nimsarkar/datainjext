from pyspark.sql import SparkSession
from utils import load_config
from ingestion.ingestion_engine import read_data
from transformation.transformation_engine import canonicalize_columns, enrich
from validation.validation_engine import apply_validation

def run_pipeline(config_path):
    spark = SparkSession.builder.appName("RetailIngestion").getOrCreate()
    config = load_config(config_path)

    df = read_data(spark, config['raw_path'], config['file_format'])
    df = canonicalize_columns(df, config['column_mappings'])
    df = apply_validation(df, config['rules'])
    df = enrich(df, config['store'])

    df.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("append") \
        .saveAsTable(config['target_table'])

    print(f"✅ Loaded {df.count()} rows from {config['store']} into {config['target_table']}")
