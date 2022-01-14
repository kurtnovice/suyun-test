import pandas as pd
from transaction import TransactionSourceAdapter, Transaction
import requests
import json
import datetime
from typing import List
from observers import UpdateDataFrameObserver, SendNotificationObserver
from sender import TelegramSender, Sender
import math
import numpy as np

class TransactionManager:
    def __init__(self, server, owner_address, token, directory, file_name, token_symbol):
        # initialize adapter
        # self._latest_record_timestamp = None
        self._trans_adp = TransactionSourceAdapter(directory, file_name)
        self._data = self._trans_adp.data
        # if previous data is longer than len 0, filter out confirmed because we ignore the previous unconfirmd
        if len(self._data) > 0:
            self._data = self._data[self._data["status"] == "UNCONFIRMED"]
        self._params = dict()
        self._server = server
        self._token = token
        self._owner_address = owner_address
        self._unconfirmed_transactions = None
        self._token_symbol = token_symbol
        self.load_unconfirmed_only()
        self._directory = directory
        self._file_name = file_name

    """
    To set up singleton
    """

    # _instance = None
    # def __new__(cls, *args, **kwargs):
    #     if not cls._instance:
    #         cls._instance = super(TransactionManager, cls)\
    #             .__new__(cls, *args, **kwargs)
    #     return cls._instance

    def load_unconfirmed_only(self):
        """
        Everytime the data has been loaded from the adapter
        we load only the ones unconfirmed because we want to compare with the 
        unconfirmed request, if the transaction id does not exist in the 
        unconfirmed request, change the status to confirmed
        """
        self._unconfirmed_transactions = []
        # make it empty because the state of the unconfirmed may have been changed in the 
        # print(self._data)
        if len(self._data):
            unconfirmed = self._data[self._data['status'] == "UNCONFIRMED"]
            # print("unconfirmed is ")
            if len(unconfirmed) == 0:
                # meaning no unconfirmed
                return
            # print(type(unconfirmed))
            for index in range(0, len(unconfirmed)):
                # create transactions
                trans = unconfirmed.iloc[index]
                unconfirmed_trans = Transaction(
                        owner_address=self._owner_address,
                        id=trans["id"],
                        sender=trans["sender"],
                        to=trans["to"],
                        amount=trans["amount"],
                        token=self._token,
                        record_timestamp=self.latest_record_timestamp,
                        type="in" if trans['sender'] != self._owner_address else "out",
                        blockchain_timestamp=trans["blockchain_timestamp"],
                        notif_sent_timestamp=None,
                        notif_confirm_code=None
                    )
                unconfirmed_trans.attach(UpdateDataFrameObserver(unconfirmed_trans, self))
                unconfirmed_trans.attach(SendNotificationObserver(unconfirmed_trans, TelegramSender))
                self._unconfirmed_transactions.append(unconfirmed_trans)

    def request_params_builder(self, **kwargs):
        self._params = kwargs
        return self

    @property
    def latest_record_timestamp(self):
        """
        This method accesses the latest timestamp that the transaction record was taken
        It will be used as the min
        """
        try:
            return self._latest_record_timestamp
        except AttributeError as err:
            if len(self._data):
                # if there is data, take the last 
                temp_data = self._data.sort_values(by="record_timestamp", ascending=False)
                last_row = temp_data.iloc[0]
                self._latest_record_timestamp = int(last_row["record_timestamp"])
                return self._latest_record_timestamp
            # there is none yet, so we return now, the last 10 seconds
            now = datetime.datetime.now()
            last_ten_sec = now - datetime.timedelta(seconds=10)

            self._latest_record_timestamp = int(datetime.datetime.timestamp(last_ten_sec)*1000)
            return self._latest_record_timestamp

    @property
    def unconfirmed_transactions(self):
        return self._unconfirmed_transactions

    def get_latest_transactions(self):
        print("retrieving from API")
        url = f"{self._server}/v1/accounts/{self._owner_address}/transactions/{self._token}"
        headers = {
            "Accept": "application/json"
        }
        # print(f"Token = {self._token}")
        """
        Params may include 
        1. min when we are trying to get transaction for the next cycle, this 
            corresponds with self._latest_record_timestamp
            a. We assume every time the latest_record_timestamp is included in the 
                params, the transactions returned on the response 
                would be consecutive after the timestamp provided
                so there will not be overlap
            b. We only do this request to all transaction, no confirmed or unconfirmed
                filter
        2. unconfirmed only to see the status of every transaction
        """
        # print(url)
        # print(f"query params = {json.dumps(self._params)}")
        if "min_timestamp" in self._params.keys():
            # print(self._params["min"])
            min = int(self._params["min_timestamp"])
            # print(min)
            min_dt = datetime.datetime.fromtimestamp(min)
            self._params["min_timestamp"] = min_dt.strftime("%Y-%m-%d %H:%M:%S")
        print(f"params: {json.dumps(self._params)}")
        req = requests.request("GET", url, params=self._params, headers=headers)
        # print(f"status code = {req.status_code}")
        if req.status_code >= 400:
            # bad request
            print("BAD REQUEST")
            print(req.text)
            pass
        elif req.status_code == 200:
            # ok
            response = json.loads(req.text)
            # we are only concerned with id, sender, to, amount, token, 
            # blockchain_timestamp, status
            self._latest_record_timestamp = int(response["meta"]["at"])
            trans_list = response["data"]
            transactions = []
            for trans in trans_list:
                # only those with matching token symbol and above the given min
                # timestamp are taken, to avoid duplicates
                if trans["token_info"]["symbol"] == self._token_symbol:
                    temp_trans = Transaction(
                        owner_address=self._owner_address,
                        id=trans["transaction_id"],
                        sender=trans["from"],
                        to=trans["to"],
                        amount=trans["value"],
                        token=self._token,
                        record_timestamp=self._latest_record_timestamp,
                        type="in" if trans['from'] != self._owner_address else "out",
                        blockchain_timestamp=trans["block_timestamp"],
                        notif_sent_timestamp=None,
                        notif_confirm_code=None
                    )
                    temp_trans.attach(UpdateDataFrameObserver(temp_trans, self))
                    temp_trans.attach(SendNotificationObserver(temp_trans, TelegramSender))
                    transactions.append(temp_trans)
            return transactions

    def insert_data(self, transactions: List[Transaction]):
        # this is to inser latest transactions after being 
        # print(transactions)
        ids = set(self._data['id']) if len(self._data) else set()  # index is the id
        # print(self._data.columns)
        # print(ids)
        trans_df = [self._data]
        new_df = pd.DataFrame()
        for index, trans in enumerate(transactions):
            if trans.id not in ids:
                dic = trans.get_dict()
                new_df = new_df.append(dic, ignore_index=True)
                # df = pd.DataFrame(data=dic, index=[index])
                # trans_df.append(df)
        self._data = pd.concat([self._data, new_df], ignore_index=True)
        self._data.drop_duplicates(subset="id", keep="last", inplace=True)
        if self._data.columns[-1] == "blockchain_timestamp":
            self._data.drop(columns=[self._data.columns[len(self._data.columns) - 1]], inplace=True)
        # self._data = self._data.append(new_df)
        # print(f"num of columns after append = {len(self._data.columns)}, size is {self._data.shape}")
        # print(self._data.columns)
        self.load_unconfirmed_only()

    def update_data(self, transaction: Transaction):
        # to do update status
        id = transaction.id
        # print(f"updating {str(transaction)} in dataframe")
        try:
            index = None
            for idx, row in enumerate(self._data.iterrows()):
                if row[1]["id"] == id:
                    index = idx
            if index == None:
                raise KeyError("id doesn't exist")
            self._data.iloc[index] = transaction.get_dict()
            # print(self._data.iloc[index])
            # reload unconfirmed only to update the unconfirmed
            self.load_unconfirmed_only()
        except KeyError as err:
            print("CANT FIND ID IN TRANSACTIIONS")
            # this transaction doesn't exist yet
            self.insert_data([transaction])

    def sort_unconfirmed_transactions(self):
        # algorithmically reduce search time
        if self._unconfirmed_transactions and len(self._unconfirmed_transactions) > 1:
            self._unconfirmed_transactions = sorted(self._unconfirmed_transactions, key=lambda x: x.id)

    def save_data(self):
        # this is to save data back to the csv
        # drop duplicates 
        # append everything, including unconfirmed
        df = pd.read_excel(f"{self._directory}{self._file_name}", index_col=None)
        # print(df)
        if "Unnamed: 0" in df.columns:
            # print("Unnamed: 0 is found in columns")
            # print(df.columns)
            try:
                df.drop("Unnamed: 0", inplace=True, axis=1)
            except KeyError as err:
                pass
        if len(df) > 0 and "id" not in df.columns:
            new_columns = df.iloc[0]
            df = df.iloc[1:]
            df.columns = new_columns

        # first append
        final = pd.concat([df, self._data], ignore_index=True)
        final.drop_duplicates(subset="id", keep="last", inplace=True)
        final.to_excel(f"{self._directory}{self._file_name}")
        # in self._data, remove all 
        # self._data = self._data[self._data["status"] == "UNCONFIRMED"]
        self.load_unconfirmed_only()


class TotalManager:

    def __init__(self, directory: str, file_name: str, date: datetime.datetime, SenderClass):
        if not issubclass(SenderClass, Sender):
            raise RuntimeError("Not implementing the right sender class")
        self._sender_class = SenderClass
        self._directory = directory
        self._file_name = file_name
        self._date = date
        self._start = None
        self._end = None
        try:
            df = pd.read_excel(f"{self._directory}{self._file_name}", index_col=None)
            if "Unnamed: 0" in df.columns:
                # print("Unnamed: 0 is found in columns")
                # print(df.columns)
                try:
                    df.drop("Unnamed: 0", inplace=True, axis=1)
                except KeyError as err:
                    pass
            if len(df) > 0 and "date" not in df.columns:
                new_columns = df.iloc[0]
                df = df.iloc[1:]
                df.columns = new_columns
            self._data = df
        except FileNotFoundError as err:
            self._data = pd.DataFrame(columns=["date", "in", "out","nett"], data=None, index=[])
        self._load_latest_sent_date()


    def _load_latest_sent_date(self):
        dt = None

        if self._date is not None:
            # self._date is datetime
            # check if the date exists in the data frame already
            if len(self._data) and self._date.strftime("%d-%m-%Y") in self._data["date"].unique():
                raise RuntimeError("This date's total has been sent before")
            # because we are forced to process this date
            dt = self._date
        else:
            # if no date provided, open the dataframe and get the last date
            if len(self._data):
                temp = self._data.sort_values(by="date", ascending=False)
                last_dt = temp.iloc[0]["date"]
                last_dt = datetime.datetime.strptime(last_dt, "%d-%m-%Y")
                # we are assigning the next day so increment
                dt = last_dt + datetime.timedelta(days=1)
                # first assign as from the data loaded
            else:
                # if no data and no date provided, go with today
                dt = datetime.datetime.now()

        self._start = dt.replace(minute=0, hour=0, second=0)
        self._end = self._start + datetime.timedelta(days=1)

    @property
    def start(self):
        return self._start.timestamp() * 1000

    @property
    def end(self):
        return self._end.timestamp() * 1000

    def total_up(self, data: pd.DataFrame):
        # data["blockchain_timestamp"] = data["blockchain_timestamp"].astype("int32")
        # data["date"] = pd.to_datetime(data["blockchain_timestamp"]/1000, unit="s")
        temp = data[(data["blockchain_timestamp"] > self.start) & (data["blockchain_timestamp"] <= self.end) & (data["status"] == "CONFIRMED")]
        if len(temp) == 0:
            raise RuntimeError(f"No total for {self._date.strftime('%d-%m-%Y')}")
        # drop everything else except amount and type
        temp = temp.loc[:, ["type", "amount"]]
        groups = temp.groupby(["type"])

        total_info = dict.fromkeys(["date","in","out","nett"])
        try:
            total_info["in"] = groups.get_group("in")["amount"].sum()
        except KeyError as err:
            total_info["in"] = 0
        try:
            total_info["out"] = groups.get_group("out")["amount"].sum()
        except KeyError as err:
            total_info["out"] = 0
        total_info["nett"] = total_info["in"] - total_info["out"]
        total_info["date"] = self._start.strftime("%d-%m-%Y")
        sender = self._sender_class()
        val = sender.send_total(total_info)
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._store_total_data(total_info, val, now)

    def _store_total_data(self, total_info, response_code, now):
        total_info["sent_at"] = now
        total_info["response_code"] = response_code
        new_df = pd.DataFrame()
        new_df = new_df.append(total_info, ignore_index=True)
        self._data = pd.concat([self._data, new_df], ignore_index=True)
        self._data.to_excel(f"{self._directory}{self._file_name}")
