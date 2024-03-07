import pandas as pd
import json
from pandas import json_normalize
from datetime import datetime
import numpy as np
import psycopg2
from sqlalchemy import create_engine, BOOLEAN, VARCHAR, TIMESTAMP, FLOAT, INTEGER

def read_json_file(file_path):
    try:
        with open(file_path, 'r') as file:
            json_data = file.readlines()
        return [json.loads(line) for line in json_data]
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        exit()

def preprocess_receipts_data(json_data):
    try:
        receipts_fact_df = pd.json_normalize(json_data)

        receipts_fact_df.replace('', np.nan, inplace=True)
        receipts_fact_df.fillna(value=None, inplace=True)

        items_list = []
        for entry in json_data:
            receipt_id = entry['_id']['$oid']
            if 'rewardsReceiptItemList' in entry:
                items = entry['rewardsReceiptItemList']
                for item in items:
                    item['receipt_id'] = receipt_id
                    items_list.append(item)
        rewards_receipt_items_df = pd.DataFrame(items_list)

        receipts_fact_df.drop(columns=['rewardsReceiptItemList'], inplace=True)

        
        float_columns_items = ['finalPrice', 'itemPrice', 'userFlaggedPrice', 'userFlaggedQuantity', 
                         'discountedItemPrice', 'pointsEarned', 'originalFinalPrice', 
                         'originalMetaBriteItemPrice', 'priceAfterCoupon']
        rewards_receipt_items_df[float_columns_items] = rewards_receipt_items_df[float_columns_items].astype(float)
        
        float_columns_fact = ['pointsEarned','totalSpent']
        receipts_fact_df[float_columns_fact] = receipts_fact_df[float_columns_fact].astype(float)

        return receipts_fact_df, rewards_receipt_items_df
    
    except Exception as e:
        print(f"Error occurred during preprocessing: {e}")
        exit()

def create_integration_id(rewards_receipt_items_df):
    try:
        rewards_receipt_items_df['integration_id'] = rewards_receipt_items_df['partnerItemId'].astype(str) + '~' + rewards_receipt_items_df['receipt_id']
        return rewards_receipt_items_df
    except Exception as e:
        print(f"Error occurred while creating integration ID: {e}")
        exit()

def convert_epoch_to_datetime(epoch_time):
    try:
        if pd.isnull(epoch_time):
            return None
        else:
            return datetime.fromtimestamp(epoch_time / 1000)
    except Exception as e:
        print(f"Error occurred while converting epoch to datetime: {e}")
        exit()

def convert_dates(receipts_fact_df):
    try:
        date_columns = ['createDate', 'dateScanned', 'finishedDate', 'modifyDate', 'pointsAwardedDate', 'purchaseDate']
        for col in date_columns:
            receipts_fact_df[col] = receipts_fact_df[col].apply(convert_epoch_to_datetime)
        return receipts_fact_df
    except Exception as e:
        print(f"Error occurred while converting dates: {e}")
        exit()

def load_data_to_db(receipts_fact_df, rewards_receipt_items_df, db_config):
    try:
        engine = create_engine('postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'.format(**db_config))

        dtype_mapping_receipts = {
            'receipt_id': VARCHAR(700),
            'user_id': VARCHAR(700),
            'rewardsReceiptStatus': VARCHAR(500),
            'bonusPointsEarned': FLOAT, 
            'bonusPointsEarnedReason': VARCHAR(2000),
            'pointsEarned': FLOAT,
            'purchasedItemCount': FLOAT,
            'totalSpent': FLOAT, 
            'createDate': TIMESTAMP,
            'dateScanned': TIMESTAMP,
            'finishedDate': TIMESTAMP,
            'modifyDate': TIMESTAMP,
            'pointsAwardedDate': TIMESTAMP,
            'purchaseDate': TIMESTAMP
        }

        dtype_mapping_items = {
            'integration_id': VARCHAR(500),
            'receipt_id': VARCHAR(500),
            'partnerItemId': INTEGER,
            'pointsPayerId': VARCHAR(500),
            'barcode': VARCHAR(30),
            'description': VARCHAR(2000),
            'finalPrice': FLOAT,
            'itemPrice': FLOAT,
            'needsFetchReview': BOOLEAN,
            'preventTargetGapPoints': BOOLEAN,
            'quantityPurchased': FLOAT,
            'userFlaggedBarcode': VARCHAR(30),
            'userFlaggedNewItem': BOOLEAN,
            'userFlaggedPrice': FLOAT,
            'userFlaggedQuantity': FLOAT,
            'needsFetchReviewReason': VARCHAR(200),
            'pointsNotAwardedReason': VARCHAR(2000),
            'rewardsGroup': VARCHAR(500),
            'rewardsProductPartnerId': VARCHAR(500),
            'userFlaggedDescription': VARCHAR(500),
            'originalMetaBriteBarcode': VARCHAR(30),
            'originalMetaBriteDescription': VARCHAR(1000),
            'brandCode': VARCHAR(350),
            'competitorRewardsGroup': VARCHAR(500),
            'discountedItemPrice': FLOAT,
            'originalReceiptItemText': VARCHAR(500),
            'itemNumber': VARCHAR(30),
            'originalMetaBriteQuantityPurchased': INTEGER,
            'pointsEarned': FLOAT,
            'targetPrice': FLOAT,
            'competitiveProduct': BOOLEAN,
            'originalFinalPrice': FLOAT,
            'originalMetaBriteItemPrice': FLOAT,
            'deleted': BOOLEAN,
            'priceAfterCoupon': FLOAT,
            'metabriteCampaignId': VARCHAR(1000)
        }

        receipts_fact_df.rename(columns={
            'userId': 'user_id',
            '_id.$oid': 'receipt_id',
            'createDate.$date': 'createDate',
            'dateScanned.$date': 'dateScanned',
            'finishedDate.$date': 'finishedDate',
            'modifyDate.$date': 'modifyDate',
            'pointsAwardedDate.$date': 'pointsAwardedDate',
            'purchaseDate.$date': 'purchaseDate'
        }, inplace=True)

        receipts_fact_df.to_sql('receipts_fact', engine, if_exists='append', index=False, dtype=dtype_mapping_receipts)
        rewards_receipt_items_df.to_sql('receipts_line_item', engine, if_exists='append', index=False, dtype=dtype_mapping_items)

    except Exception as e:
        print(f"Error occurred while loading data to database: {e}")
        exit()

def check_unique_id(df, column):
    unique_count = df[column].nunique()
    if unique_count != len(df):
        print(f"Error: {column} is not unique. Ingestion stopped.")
        exit()

def main():
    json_file_path = 'receipts.json'
    db_config = {
        'user': 'postgres',
        'password': '9901',
        'host': 'localhost',
        'port': '5433',
        'database': 'postgres'
    }

    json_data = read_json_file(json_file_path)
    
    receipts_fact_df, rewards_receipt_items_df = preprocess_receipts_data(json_data)
    
    check_unique_id(receipts_fact_df, '_id.$oid')
        
    rewards_receipt_items_df = create_integration_id(rewards_receipt_items_df)

    check_unique_id(rewards_receipt_items_df, 'integration_id')
    
    receipts_fact_df = convert_dates(receipts_fact_df)
    
    load_data_to_db(receipts_fact_df, rewards_receipt_items_df, db_config)

if __name__ == "__main__":
    main()
