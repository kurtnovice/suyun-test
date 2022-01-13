import datetime

from transaction_manager import TransactionManager, TotalManager
from transaction import TransactionSourceAdapter
import time
from settings import SERVER, TOKEN, TOKEN_SYMBOL,\
    DEFAULT_SOURCE_FILE_NAME, DEFAULT_TOTAL_FILE_NAME, \
    OWNER_REAL_ADDRESS, SOURCE_FOLDER, TOTAL_TRIGGER_TIME
from sender import TelegramSender

def process_latest_trans(trans_mgr:TransactionManager):
    """
    This is the main process, 
    1. initialize the manager
    2. Get unconfirmed transactions
    3. Set up parameters to get transactions
    4. Get transactions
    5. Insert data into dataframe
    """
    transactions = trans_mgr.request_params_builder(min_timestamp=trans_mgr.latest_record_timestamp,limit=50) \
        .get_latest_transactions()
    trans_mgr.insert_data(transactions)
    trans_mgr.save_data()

def process_unconfirmed(trans_mgr:TransactionManager):
    """
    This is the main process,
    1. initialize the manager
    2. Get unconfirmed transactions
    3. Set up parameters to get transactions
    4. Get transactions
    5. Insert data into dataframe
    """
    unconfirmed_trans = trans_mgr.request_params_builder(only_unconfirmed=True, limit=50) \
        .get_latest_transactions()
    if len(unconfirmed_trans) == 0:
        # all transactions have been cleared
        for trans in trans_mgr.unconfirmed_transactions:
            trans.status = "CONFIRMED"
            trans_mgr.update_data(trans)
    else:
        # find which ids are not yet confirmed
        recently_retrieved_unconfirmed_trans_id_set = set([trans.id for trans in unconfirmed_trans])
        existing_retrieved_unconfirmed_trans_id_set = set([trans.id for trans in trans_mgr. \
                                                          unconfirmed_transactions])
        confirmed_trans_id_set = existing_retrieved_unconfirmed_trans_id_set. \
            difference(recently_retrieved_unconfirmed_trans_id_set)
        # first sort the unconfirmed_transactions
        sorted_confirmed_trans_id_set = sorted(confirmed_trans_id_set)
        trans_mgr.sort_unconfirmed_transactions()
        for trans_id in sorted_confirmed_trans_id_set:
            # find the trans
            for trans in trans_mgr.unconfirmed_transactions:
                if trans_id == trans.id:
                    trans.status = "CONFIRMED"
                    trans_mgr.update_data(trans)
        print(f"Number of unconfirmed is {len(trans_mgr.unconfirmed_transactions)}")
    trans_mgr.save_data()


if __name__ == "__main__":
    # initialize the trans manager
    trans_mgr = TransactionManager(SERVER, OWNER_REAL_ADDRESS,
                                   TOKEN, SOURCE_FOLDER,
                                   DEFAULT_SOURCE_FILE_NAME, TOKEN_SYMBOL)
    while True:

        now = datetime.datetime.now()
        process_latest_trans(trans_mgr)
        process_unconfirmed(trans_mgr)

        time.sleep(10)
        if now.strftime("%H:%M") == TOTAL_TRIGGER_TIME:
            # load existing data
            trans_adp = TransactionSourceAdapter(SOURCE_FOLDER, DEFAULT_SOURCE_FILE_NAME)
            ttl_mgr = TotalManager(SOURCE_FOLDER, DEFAULT_TOTAL_FILE_NAME, now, TelegramSender)
            ttl_mgr.total_up(trans_adp.data)

