import pandas as pd
import json
from pandas import json_normalize
import psycopg2
from sqlalchemy import create_engine, Boolean, VARCHAR, TIMESTAMP
from datetime import datetime

def load_data_to_postgres(df):

    db_config = {
        'user': 'postgres',
        'password': '9901',
        'host': 'localhost',
        'port': '5433',
        'database': 'postgres'
    }
    
    conn = psycopg2.connect(**db_config)

    dtype_mapping = {
        'active': Boolean,
        'role': VARCHAR(350),
        'signUpSource': VARCHAR(700),
        'state': VARCHAR(50),
        'user_id': VARCHAR(700),
        'createdDate': TIMESTAMP,
        'lastLogin': TIMESTAMP
    }

    df.to_sql('users', conn, if_exists='append', index=False, dtype=dtype_mapping)

    conn.commit()
    conn.close()

def check_null_percentage(df):
    null_counts = df.isnull().sum()
    total_rows = len(df)
    null_percentages = round((null_counts / total_rows) * 100, 2)
    return null_percentages

def check_unique_values(df, column):
    unique_count = df[column].nunique()
    return unique_count

def main():

    with open('users.json', 'r') as file:
        json_data = file.readlines()

    parsed_data = [json.loads(line) for line in json_data]

    df = pd.json_normalize(parsed_data)

    null_percentages = check_null_percentage(df)
    print("Null Percentages:")
    print(null_percentages)

    unique_count = check_unique_values(df, '_id.$oid')
    print("Count of Unique Values based on _id.$oid:")
    print(unique_count)

    df.replace('', None, inplace=True)
    df.fillna(value=pd.NA, inplace=True)

    df['createdDate'] = pd.to_datetime(df['createdDate'], unit='ms')
    df['lastLogin'] = pd.to_datetime(df['lastLogin'], unit='ms')

    df.drop_duplicates(subset=['user_id'], inplace=True)

    load_data_to_postgres(df)

if __name__ == "__main__":
    main()
