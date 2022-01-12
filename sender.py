import abc

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
        # setup configuration here, more than likely this will be a singleton too
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
            print(f"Transaction {val['id']} type "\
                f"{val['type'].upper()} has been {transaction.status} on "\
                f"{transaction.date}")
            return "12341345"
        else:
            return None

    def send_total(self, total_info):
        print(f"Date: {total_info['date']}, Total In: {total_info['in']},"
            f"Total Out: {total_info['out']}, Nett:{total_info['nett']}")
        return "1232453245"


    
