import abc
from settings import TELEGRAM_TOKEN, CHAT_ID, TELEGRAM_URL
import requests


class Sender(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def send(self, transaction):
        pass

    @abc.abstractmethod
    def send_total(self, total_info):
        pass

    @classmethod
    def __subclasshook__(cls, C):
        if cls is Sender:
            attrs = set(dir(C))
            if set(cls.__abstractmethods__) <= attrs:
                return True
        
        return NotImplemented

class TelegramSender(Sender):
    _instance = None

    def __init__(self):
        # setup configuration here
        # self._tb = telebot.TeleBot(TELEGRAM_TOKEN)
        # tb.send_message(chatid, message)
        pass


    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(TelegramSender, cls)\
                .__new__(cls, *args, **kwargs)

        return cls._instance

    def send(self, transaction):
        # only if the transaction status is confirmed
        val = transaction.get_dict()
        if transaction.status == "CONFIRMED":
            # simulate send by telegram API
            message = f"Transaction {val['id']} type "\
                f"{val['type'].upper()} has been {transaction.status} on "\
                f"{transaction.date}"
            print(message)

            response_code = self._send(message, CHAT_ID)
            return response_code
        else:
            return None

    def send_total(self, total_info):
        message = f"Date: {total_info['date']}, Total In: {total_info['in']},"\
            f"Total Out: {total_info['out']}, Nett:{total_info['nett']}"
        print(message)
        response_code = self._send(message, CHAT_ID)
        return response_code

    def _send(self, message, chat_id):
        body = {
            "chat_id": chat_id,
            "text": message
        }
        header = {
            "Content-Type": "application/json"
        }
        url = f"{TELEGRAM_URL}bot{TELEGRAM_TOKEN}/sendMessage"
        req = requests.request("POST", url=url, data=body)
        return req.status_code

    
