import unittest
import pandas as pd
import json

class TestReadUsersIntegration(unittest.TestCase):
    def setUp(self):
        with open('users.json', 'r') as file:
            json_data = file.readlines()

        self.parsed_data = [json.loads(line) for line in json_data]

        self.df = pd.json_normalize(self.parsed_data)

    def test_columns_presence(self):
        expected_columns = ['active', 'role', 'signUpSource', 'state', '_id.$oid', 'createdDate.$date', 'lastLogin.$date']
        self.assertListEqual(list(self.df.columns), expected_columns)

    def test_data_types(self):
        self.assertEqual(self.df['active'].dtype, bool)
        self.assertEqual(self.df['role'].dtype, object)

    def test_non_empty_dataframe(self):
        self.assertFalse(self.df.empty)

if __name__ == '__main__':
    unittest.main()
