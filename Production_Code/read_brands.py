import pandas as pd
import json
from pandas import json_normalize
import numpy as np
import psycopg2
from sqlalchemy import create_engine, Boolean, VARCHAR, TIMESTAMP, BIGINT

def read_and_parse_json(file_path):
    with open(file_path, 'r') as file:
        json_data = file.readlines()

    parsed_data = [json.loads(line) for line in json_data]

    df = json_normalize(parsed_data)

    return df

def check_null_percentage(df):
    null_counts = df.isnull().sum()
    total_rows = len(df)
    null_percentages = round((null_counts / total_rows) * 100, 2)
    print("Null Counts:")
    print(null_counts)
    print("\nNull Percentages:")
    print(null_percentages)

def check_duplicates(df, column):
    duplicate_count = df.duplicated(subset=[column]).sum()
    print("Duplicate Count based on", column, ":", duplicate_count)

def clean_data(df):
    df = df.replace({np.NaN: None})
    for col in df.columns:
        df[col] = df[col].replace('', np.nan)
    return df

def rename_columns(df):
    column_mapping = {
        '_id.$oid': 'brand_uuid',
        'cpg.$id.$oid': 'cpg_id',
        'cpg.$ref': 'cpg_ref',
    }
    df_mapped = df.rename(columns=column_mapping)
    return df_mapped

def reorder_columns(df_mapped):
    columns_sequence = ['brand_uuid', 'barcode', 'brandCode', 'category', 'categoryCode', 'cpg_id', 'cpg_ref', 'topBrand', 'name']
    df_reordered = df_mapped[columns_sequence]
    return df_reordered

def load_data_to_postgres(df, table_name, dtype_mapping, db_config):
    engine = create_engine('postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'.format(**db_config))
    df.to_sql(table_name, engine, if_exists='append', index=False, dtype=dtype_mapping)

def main():
    file_path = 'brands.json'
    df = read_and_parse_json(file_path)

    check_null_percentage(df)

    check_duplicates(df, '_id.$oid')

    df_cleaned = clean_data(df)

    df_renamed = rename_columns(df_cleaned)
    print("Renamed DataFrame:")
    print(df_renamed.head())

    df_reordered = reorder_columns(df_renamed)
    print("Reordered DataFrame:")
    print(df_reordered.head())

    db_config = {
        'user': 'postgres',
        'password': '9901',
        'host': 'localhost',
        'port': '5433',
        'database': 'postgres'
    }

    dtype_mapping = {
        'brand_uuid': VARCHAR(350),
        'barcode': VARCHAR(30),
        'brandCode': VARCHAR(500),
        'category': VARCHAR(500),
        'categoryCode': VARCHAR(350),
        'cpg_id': VARCHAR(350),
        'cpg_ref': VARCHAR(100),
        'topBrand': Boolean,
        'name': VARCHAR(350)
    }

    load_data_to_postgres(df_reordered, 'brand', dtype_mapping, db_config)

if __name__ == "__main__":
    main()
