from pyspark.sql import SparkSession
from utils import read_data, canonicalize_columns, apply_rules, enrich_data, write_data

def run_pipeline(config):
    spark = SparkSession.builder.appName("RetailIngestion").getOrCreate()

    df = read_data(spark, config['raw_path'], config['file_format'])
    df = canonicalize_columns(df, config['column_mappings'])
    df = apply_rules(df, config.get('rules', {}))
    df = enrich_data(df, config['store'])

    write_data(df, config['target_table'])

    print(f"✅ Successfully loaded {df.count()} records into {config['target_table']}")
