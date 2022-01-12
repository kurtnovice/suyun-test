import unittest
import os
import sys

  
# setting path
sys.path.append('../test_suyun')


from transaction import TransactionSourceAdapter, Transaction
from test_settings import SOURCE_FOLDER, DEFAULT_SOURCE_FILE_NAME, TEST_OWNER_ADDRESS, TEST_SOURCE_FILE_NAME

class TestTransAdapter(unittest.TestCase):

    def setUp(self):
        #create a pickle file that from dataframe 
        pass

    def test_upon_initialize_if_no_file_create_file(self):
        trans_adp = TransactionSourceAdapter(SOURCE_FOLDER,DEFAULT_SOURCE_FILE_NAME)
        file_path = f"{SOURCE_FOLDER}{DEFAULT_SOURCE_FILE_NAME}"
        file_created = os.path.exists(file_path)
        self.assertTrue(file_created)
        if file_created:
            # clear up
            os.remove(file_path)
            print(f"removing file {file_path}")
    def test_upon_initialize_successfully_load_file(self):
        trans_adp = TransactionSourceAdapter(SOURCE_FOLDER,TEST_SOURCE_FILE_NAME)
        self.assertEqual(len(trans_adp.data), 2)
        transactions = []
        for row in trans_adp.data.to_dict('records'):
            transactions.append(Transaction(
                owner_address = TEST_OWNER_ADDRESS,
                id=row['id'],
                sender=row['sender'],
                to=row['to'],
                type=row['type'],
                token=row['token'],
                record_timestamp=row['record_timestamp'], 
                amount=row['amount'],
                blockchain_timestamp=row['blockchain_timestamp'],
                notif_sent_timestamp=row['notif_sent_timestamp'],
                notif_confirm_code=row['notif_confirm_code'],
                status=row['status']
            ))
        self.assertEqual(transactions[0].status, "CONFIRMED")
        self.assertEqual(transactions[1].status, "UNCONFIRMED")
        
            
    def tearDown(self):
        pass

if __name__ == "__main__":
    unittest.main()