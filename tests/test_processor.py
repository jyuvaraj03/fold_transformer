import unittest
import os
import shutil
import pandas as pd
from datetime import datetime
import pytz
from src.processor import process_file

class TestProcessor(unittest.TestCase):
    def setUp(self):
        self.test_dir = 'tests/test_data'
        self.output_dir = 'tests/test_output'
        os.makedirs(self.test_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Create a sample CSV file
        self.input_file = os.path.join(self.test_dir, 'sample_txns.csv')
        data = {
            'account_number': ['1234', '1234', '5678'],
            'card_number': ['', '', '9999'],
            'card_name': ['', '', 'MyCard'],
            'bank_name': ['BankA', 'BankA', ''],
            'txn_timestamp': ['2023-10-26T10:00:00Z', '2023-10-27T10:00:00Z', '2023-10-28T10:00:00Z'],
            'amount': [-100.0, 50.0, -20.0],
            'type': ['DEBIT', 'CREDIT', 'DEBIT'],
            'narration': ['Expense 1', 'Refund', 'Expense 2'],
            'reference': ['Ref1', 'Ref2', 'Ref3'],
            'merchant': ['MerchantA', '', 'MerchantB'],
            'category': ['Food', 'Refund', 'Travel'],
            'tracking_method': ['SMS', 'SMS', 'EMAIL']
        }
        df = pd.DataFrame(data)
        df.to_csv(self.input_file, index=False)

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        shutil.rmtree(self.output_dir)

    def test_process_file_creates_output(self):
        process_file(self.input_file, self.output_dir)
        
        # Expecting two files: BankA_1234.csv and MyCard_9999.csv
        files = os.listdir(self.output_dir)
        self.assertIn('BankA_1234.csv', files)
        self.assertIn('MyCard_9999.csv', files)

    def test_content_verification(self):
        process_file(self.input_file, self.output_dir)
        
        bank_file = os.path.join(self.output_dir, 'BankA_1234.csv')
        df = pd.read_csv(bank_file)
        
        # Verify columns
        expected_cols = ['date', 'payee', 'notes', 'debit_amount', 'credit_amount']
        self.assertEqual(list(df.columns), expected_cols)
        
        # Verify rows count
        self.assertEqual(len(df), 2)
        
        # Verify specific values (Timezone conversion 10:00 UTC -> 15:30 IST)
        self.assertEqual(df.iloc[0]['date'], '2023-10-26')
        self.assertEqual(df.iloc[0]['debit_amount'], 100.0)
        self.assertEqual(df.iloc[0]['credit_amount'], 0.0)
        
        self.assertEqual(df.iloc[1]['credit_amount'], 50.0)

    def test_since_date_filtering(self):
        # Filter transactions since 2023-10-27
        process_file(self.input_file, self.output_dir, since_date='2023-10-27')
        
        bank_file = os.path.join(self.output_dir, 'BankA_1234.csv')
        df = pd.read_csv(bank_file)
        
        # Should only have the transaction from 27th (since >= 27th)
        # Note: 2023-10-27T10:00:00Z is 2023-10-27 15:30:00+05:30.
        # process_file logic: if ts < since_dt: continue
        # if since_date is 2023-10-27, it becomes 2023-10-27 00:00:00+05:30
        # The txn is at 15:30:00+05:30, so it should be included.
        # The txn from 26th should be excluded.
        
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['date'], '2023-10-27')

if __name__ == '__main__':
    unittest.main()
