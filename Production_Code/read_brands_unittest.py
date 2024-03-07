import unittest
import pandas as pd
import json

class TestReadBrandsIntegration(unittest.TestCase):
    def setUp(self):
        with open('brands.json', 'r') as file:
            json_data = file.readlines()

        self.parsed_data = [json.loads(line) for line in json_data]

        self.df = pd.json_normalize(self.parsed_data)

    def test_columns_presence(self):
        expected_columns = ['barcode','category','categoryCode','name',	'topBrand',	'_id.$oid',	'cpg.$id.$oid',	'cpg.$ref',	'brandCode']
        self.assertListEqual(list(self.df.columns), expected_columns)

    def test_data_types(self):
        self.assertEqual(self.df['_id.$oid'].dtype, object)
        self.assertEqual(self.df['barcode'].dtype, object)
        self.assertEqual(self.df['brandCode'].dtype, object)

    def test_non_empty_dataframe(self):
        self.assertFalse(self.df.empty)

    def test_unique_id_oid(self):
        unique_ids = self.df['_id.$oid'].nunique()
        self.assertEqual(len(self.df), unique_ids)

if __name__ == '__main__':
    unittest.main()
