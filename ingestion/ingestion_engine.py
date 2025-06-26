def read_data(spark, path, fmt):
    if fmt == 'csv':
        return spark.read.option("header", True).csv(path)
    elif fmt == 'parquet':
        return spark.read.parquet(path)
    else:
        raise ValueError(f"Unsupported file format: {fmt}")
