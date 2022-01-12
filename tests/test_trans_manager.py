import datetime
import unittest
import os
import sys
import math
  
# setting path
sys.path.append('../test_suyun')
from transaction import Transaction
from transaction_manager import TransactionManager
from test_settings import SERVER, TEST_TOKEN, \
    SOURCE_FOLDER, TEST_SOURCE_FILE_NAME, DEFAULT_SOURCE_FILE_NAME, \
        TEST_OWNER_REAL_ADDRESS,TEST_OWNER_ADDRESS, TEST_TOKEN_SYMBOL


class TestTransactionManager(unittest.TestCase):

    def setUp(self):
        self.trans_mgr = TransactionManager(
            SERVER,TEST_OWNER_ADDRESS, TEST_TOKEN,SOURCE_FOLDER,TEST_SOURCE_FILE_NAME, TEST_TOKEN_SYMBOL
        )

    @unittest.skip("Obsolete")
    def test_data_load_unconfirmed_correctly(self):
        # latest record timestamp should be as of the record time of the latest record
        self.assertEqual(self.trans_mgr.latest_record_timestamp,1641727001,"Latest record timestamp is wrong")
        unconf = self.trans_mgr._data.iloc[1]
        unconfirmed = Transaction(
                        owner_address=TEST_OWNER_ADDRESS,
                        id=unconf["id"],
                        sender=unconf["sender"],
                        to=unconf["to"],
                        amount=unconf["amount"],
                        token=TEST_TOKEN,
                        record_timestamp=unconf["record_timestamp"],
                        type="in" if unconf['sender'] != TEST_OWNER_REAL_ADDRESS else "out",
                        blockchain_timestamp=unconf["blockchain_timestamp"],
                        notif_sent_timestamp=None,
                        notif_confirm_code=None
                    )
        self.assertEqual(str(self.trans_mgr.unconfirmed_transactions[0]),str(unconfirmed))


class TestTransactionManagerEmptyFile(unittest.TestCase):
    def setUp(self):
        self.trans_mgr = TransactionManager(
            SERVER,TEST_OWNER_REAL_ADDRESS, TEST_TOKEN,SOURCE_FOLDER,DEFAULT_SOURCE_FILE_NAME, TEST_TOKEN_SYMBOL
        )
    @unittest.skip("done")
    def test_latest_record_timestamp_is_last_ten_seconds(self):
        # because this will create an empty file, the length of the data will be 0
        now = datetime.datetime.now()
        ten_seconds_ago = now - datetime.timedelta(seconds=10)
        self.assertGreaterEqual(self.trans_mgr.latest_record_timestamp, ten_seconds_ago.timestamp())
        self.assertLess(self.trans_mgr.latest_record_timestamp, now.timestamp())

    @unittest.skip("done")
    def test_successfully_append_new_entries_to_data(self):
        temp1 = Transaction(
            owner_address=TEST_OWNER_REAL_ADDRESS,
            id="abc",
            sender="bcg",
            to=TEST_OWNER_REAL_ADDRESS,
            amount=1000000,
            token=TEST_TOKEN,
            record_timestamp=1641841761,
            type="in",
            blockchain_timestamp=1641840000,
            notif_confirm_code=None,
            notif_sent_timestamp=None,
            status="UNCONFIRMED"
            
        )
        temp2 = Transaction(
            owner_address=TEST_OWNER_REAL_ADDRESS,
            id="abc",
            sender=TEST_OWNER_REAL_ADDRESS,
            to="bcg",
            amount=1000000,
            token=TEST_TOKEN,
            record_timestamp=1641841761,
            type="out",
            blockchain_timestamp=1641840000,
            notif_confirm_code="93249r2i43rfn2ru9",
            notif_sent_timestamp=1641850000,
            status="CONFIRMED"
            
        )
        self.trans_mgr.insert_data([temp1,temp2])
        # print(self.trans_mgr._data)
        self.assertEqual(len(self.trans_mgr._data), 2)

class TestTransactionManagerFunctional(unittest.TestCase):
    def setUp(self):
        self.trans_mgr = TransactionManager(
            SERVER,TEST_OWNER_REAL_ADDRESS, TEST_TOKEN,SOURCE_FOLDER,DEFAULT_SOURCE_FILE_NAME, TEST_TOKEN_SYMBOL
        )
        #expect the original dataframe to be 0 length

    def test_get_latest_transaction(self):
        if len(self.trans_mgr._data) > 0:
            # because we are loading data from file
            # if we have see if all of them are unconfirmed only
            len_data = len(self.trans_mgr._data)
            unconfirmed = self.trans_mgr._data[self.trans_mgr._data["status"] == "UNCONFIRMED"]
            self.assertEqual(len_data, len(unconfirmed))

        # self.assertEqual(len(self.trans_mgr._data), 0)
        ten_minutes_ago = datetime.datetime.now() - datetime.timedelta(minutes=10)
        transactions = self.trans_mgr.request_params_builder(min=ten_minutes_ago.timestamp()*1000)\
                .get_latest_transactions()
        print(transactions)
        total_trans = len(transactions)
        # self.assertNotEqual(total_trans,0)
        # now insert data
        self.trans_mgr.insert_data(transactions)
        print(f"size is {self.trans_mgr._data.shape}")

        #assert that all timestamp are above the ten_minutes_ago
        all_timestamps_are_above_ten_minutes_ago = True
        for trans in transactions:
            print(f"{trans._blockchain_timestamp} ... {ten_minutes_ago.timestamp()*1000}")
            if trans._blockchain_timestamp >= ten_minutes_ago.timestamp()*1000:
                all_timestamps_are_above_ten_minutes_ago = False
                print(f"{trans._blockchain_timestamp} is greater than {ten_minutes_ago.timestamp()*1000}")
        # self.assertTrue(all_timestamps_are_above_ten_minutes_ago)
        
        
        # assert all latest transactions are appended to the 
        # self.assertEqual(len(self.trans_mgr._data), total_trans)
        
        #assert all transactions are unconfrimed in dataframe
        # self.assertEqual(len(self.trans_mgr._data[self.trans_mgr._data["status"]=="UNCONFIRMED"]),total_trans)

       
        # print(self.trans_mgr.latest_record_timestamp)
        last_capture = datetime.datetime.fromtimestamp(self.trans_mgr.latest_record_timestamp/1000)
        # print(f'Last capture ={last_capture.strftime("%Y-%m-%d %H:%M:%S")} - {self.trans_mgr.latest_record_timestamp}')
        now = datetime.datetime.now()
        # print(f'Now = {now.strftime("%Y-%m-%d %H:%M:%S")} - {now.timestamp()*1000}')
        
        # assert the latest record timestamp is in the past
        # assert the latest record timestamp is after ten minutes ago
        self.assertTrue(self.trans_mgr.latest_record_timestamp > ten_minutes_ago.timestamp()*1000)
        self.assertTrue(self.trans_mgr.latest_record_timestamp < now.timestamp() * 1000)

        # updating transactions status, only first 5
        confirmed_ids = set()
        for trans in transactions[:5]:
            trans.status = "CONFIRMED"
            print(str(trans))
            confirmed_ids.add(trans.id)
            self.trans_mgr.update_data(trans)
        # check if this is reflected in the dataframe as well
        confirmed = self.trans_mgr._data[self.trans_mgr._data["status"]=="CONFIRMED"]
        df_confirmed_id_set = set(confirmed["id"])
        print(f"ids in dataframe of confirmed transactions = {df_confirmed_id_set}")
        self.assertEqual(len(df_confirmed_id_set.difference(confirmed_ids)), 0)

        self.trans_mgr.save_data()
        # test to 
        num_unconfirmed = len(transactions) - 5
        # self.assertEqual(len(self.trans_mgr.unconfirmed_transactions), num_unconfirmed)
        # self.assertEqual(len(self.trans_mgr._data), num_unconfirmed)

        # now try to get all unconfirmed again
        unconfirmed_trans = self.trans_mgr.request_params_builder(only_unconfirmed=True)\
            .get_latest_transactions()
        if len(unconfirmed_trans) == 0:
            # all transactions have been cleared
            for trans in self.trans_mgr.unconfirmed_transactions:
                trans.status = "CONFIRMED"
                self.trans_mgr.update_data(trans)
        else:
            # find which ids are not yet confirmed
            recently_retrieved_unconfirmed_trans_id_set = set([trans.id for trans in unconfirmed_trans])
            existing_retrieved_unconfirmed_trans_id_set = set([trans.id for trans in self.trans_mgr.\
                                                              unconfirmed_transactions])
            confirmed_trans_id_set = existing_retrieved_unconfirmed_trans_id_set.\
                difference(recently_retrieved_unconfirmed_trans_id_set)
            # first sort the unconfirmed_transactions
            sorted_confirmed_trans_id_set = sorted(confirmed_trans_id_set)
            self.trans_mgr.sort_unconfirmed_transactions()
            for trans_id in sorted_confirmed_trans_id_set:
                # find the trans
                for trans in self.trans_mgr.unconfirmed_transactions:
                    if trans_id == trans.id:
                        trans.status = "CONFIRMED"
                        self.trans_mgr.update_data(trans)
            print(f"Number of unconfirmed is {len(self.trans_mgr.unconfirmed_transactions)}")
        self.trans_mgr.save_data()

    

if __name__ == "__main__":
    unittest.main()