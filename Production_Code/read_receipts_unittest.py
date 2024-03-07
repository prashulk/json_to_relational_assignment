import unittest
import pandas as pd
import json

class TestProcessReceiptsIntegration(unittest.TestCase):
    
    def setUp(self):
        with open('receipts.json', 'r') as file:
            json_data = file.readlines()

        self.parsed_data = [json.loads(line) for line in json_data]

        self.df = pd.json_normalize(self.parsed_data)

    def test_columns_presence(self):
        expected_columns = ['bonusPointsEarned', 'bonusPointsEarnedReason', 'pointsEarned', 'purchasedItemCount', 'rewardsReceiptItemList', 'rewardsReceiptStatus', 'totalSpent', 'userId', '_id.$oid', 'createDate.$date', 'dateScanned.$date', 'finishedDate.$date', 'modifyDate.$date', 'pointsAwardedDate.$date', 'purchaseDate.$date']
        self.assertListEqual(list(self.df.columns), expected_columns)

    def test_data_types(self):
        expected_data_types = {
            'bonusPointsEarned': float,
            'bonusPointsEarnedReason': object,
            'pointsEarned': object,
            'purchasedItemCount': float,
            'rewardsReceiptItemList': object,
            'rewardsReceiptStatus': object,
            'totalSpent': object,
            'userId': object,
            '_id.$oid': object,
            'createDate.$date': int,
            'dateScanned.$date': int,
            'finishedDate.$date': float,
            'modifyDate.$date': int,
            'pointsAwardedDate.$date': float,
            'purchaseDate.$date': float
        }
        
        for column, expected_type in expected_data_types.items():
            self.assertEqual(self.df[column].dtype, expected_type)

    def test_non_empty_dataframe(self):
        self.assertFalse(self.df.empty)

    def test_unique_id_oid(self):
        unique_ids = self.df['_id.$oid'].nunique()
        self.assertEqual(len(self.df), unique_ids)

if __name__ == '__main__':
    unittest.main()
