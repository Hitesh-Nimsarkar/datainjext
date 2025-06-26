from pyspark.sql.functions import col

def apply_validation(df, rules):
    if rules.get("drop_nulls"):
        for col_name in rules["drop_nulls"]:
            df = df.filter(col(col_name).isNotNull())

    if rules.get("deduplicate_by"):
        df = df.dropDuplicates(rules["deduplicate_by"])
    
    return df
