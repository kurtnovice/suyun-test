import os
import pandas as pd
from settings import DEFAULT_SOURCE_FILE_NAME
from observers import UpdateDataFrameObserver
import datetime
# import xlsxwriter

class Transaction:
    def __init__(self, owner_address, id, sender, to, amount, token, record_timestamp, type, blockchain_timestamp, notif_sent_timestamp, notif_confirm_code, status="UNCONFIRMED"):
        self._owner_address = owner_address
        self._id = id
        self._sender = sender
        self._to = to
        self._amount = amount
        self._token = token
        self._type = type
        self._record_timestamp = record_timestamp
        self._blockchain_timestamp = blockchain_timestamp
        self._status = status
        self.observers = []
        self._notif_confirm_code = notif_confirm_code
        self._notif_sent_timestamp = notif_sent_timestamp

    @property
    def id(self):
        return self._id

    @property
    def status(self):
        return self._status

    @property
    def date(self):
        return datetime.datetime.fromtimestamp(self._blockchain_timestamp/1000).strftime("%d-%m-%Y %H:%M:%S")
        

    def notif_builder(self, time, code):
        self._notif_sent_timestamp = time
        self._notif_confirm_code = code
        # here we call the UpdateDataframe observer
        # self._update_observers(only=UpdateDataFrameObserver)
        return self

    def attach(self, observer):
        self.observers.append(observer)

    @status.setter
    def status(self, value:str):
        if value is "CONFIRMED":
            self._status = "CONFIRMED"
            self._update_observers()

    def _update_observers(self, only=None):
        for observer in self.observers:
            if only and isinstance(observer,only):
                print(f"calling {type(observer)}")
                observer()
            else:
                observer()

    def get_dict(self):
        # return the dictionary format for insertion
        return {
            "id":self._id,
            "sender":self._sender,
            "to":self._to,
            "type":self._type,
            "amount":self._amount,
            "token":self._token,
            "blockchain_timestamp":self._blockchain_timestamp,
            "record_timestamp":self._record_timestamp,
            "status":self._status,
            "notif_sent_timestamp":self._notif_sent_timestamp,
            "notif_confirm_code":self._notif_confirm_code
        }

    def __str__(self):
        return f"{self._id}-{self._status}"


class TransactionSourceAdapter:
    """
    This is the adapter to access the file
    if the file doesn't exist create the file in init
    This must be singleton
    """
    def __init__(self, source_dir:str, file_name:str=None):
        self.source_dir = source_dir
        self.file_name = file_name if file_name else DEFAULT_SOURCE_FILE_NAME
        if os.path.exists(self.source_dir) is False:
            os.mkdir(self.source_dir)
        # read the csv
        try:
            if file_name:
                df = pd.read_excel(f"{self.source_dir}{file_name}", header=0, engine="openpyxl")
                # print(df)
                # if "Unnamed: 0" in df.columns:
                #     # print("Unnamed: 0 is found in columns")
                #     # print(df.columns)
                #     try:
                #         df.drop("Unnamed:0",inplace=True, axis=1)
                #     except KeyError as err:
                #         pass
                if len(df)>0 and "example" in df.columns:
                    new_columns = df.iloc[0]
                    df = df.iloc[1:]
                    df.columns = new_columns
                for col in df.columns:
                    if "Unnamed:" in col:
                        df.drop(columns=['Unnamed: 0'], inplace=True)
                self.data = df
                # print(self.data)
            else:
                self.data = pd.read_excel(f"{self.source_dir}{DEFAULT_SOURCE_FILE_NAME}")

        except FileNotFoundError as err:
            # here create an empty dataframe and save it
            # self.data = pd.DataFrame(
            #     data={
            #         "id":[],
            #         "sender":[],
            #         "to":[],
            #         "type":[],
            #         "amount":[],
            #         "blockchain_timestamp":[],
            #         "record_timestamp":[],
            #         "status":[],
            #         "notif_sent_timestamp":[],
            #         "notif_confirm_code":[],
            #         "token":[]
            #     },
            #     index=[],
            #     columns=["id","sender",\
            #         "to","type","amount","blockchain_timestamp",\
            #         "record_timestamp","status",\
            #         "notif_sent_timestamp","notif_confirm_code",\
            #         "token"])
            self.data = pd.DataFrame()
            # print(f"{self.source_dir}{self.file_name}")
            self.data.to_excel(f"{self.source_dir}{self.file_name}", index=False)

    # def get_unconfirmed_entries(self):
    #     unconfirmed = self.data[self.data['status']=="UNCONFIRMED"]
    #     unconfirmed.to_dict(orient="id")

        


