import datetime
import unittest
import sys
import os

# setting path
sys.path.append('../test_suyun')
from transaction_manager import TotalManager
from transaction import TransactionSourceAdapter
from sender import TelegramSender
from test_settings import SERVER, TEST_TOKEN, \
    SOURCE_FOLDER, TEST_TOTAL_FILE_NAME, DEFAULT_TOTAL_FILE_NAME, DEFAULT_SOURCE_FILE_NAME

class TestTotalManagerFileLoad(unittest.TestCase):

    def setUp(self):
        dt = datetime.datetime.strptime("2022-01-12", "%Y-%m-%d")
        self.total_mgr = TotalManager(SOURCE_FOLDER,
                                      DEFAULT_TOTAL_FILE_NAME,
                                      dt,
                                      TelegramSender)

    def test_calculate_total(self):
        try:
            adapter = TransactionSourceAdapter(SOURCE_FOLDER,
                                               DEFAULT_SOURCE_FILE_NAME)
            self.assertTrue(len(adapter.data)>0)

            self.total_mgr.total_up(adapter.data)
            os.remove(f"{SOURCE_FOLDER}{DEFAULT_SOURCE_FILE_NAME}")

        except RuntimeError as err:
            print(err)

        try:
            adapter = TransactionSourceAdapter(SOURCE_FOLDER,
                                               DEFAULT_SOURCE_FILE_NAME)
            self.assertTrue(len(adapter.data) > 0)

            self.total_mgr.total_up(adapter.data)
            os.remove(f"{SOURCE_FOLDER}{DEFAULT_SOURCE_FILE_NAME}")

        except RuntimeError as err:
            print(err)



if __name__ == "__main__":
    unittest.main()