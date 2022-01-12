import datetime
from sender import Sender


class UpdateDataFrameObserver:
    def __init__(self, trans, trans_mgr):
        self.trans = trans
        self.trans_manager = trans_mgr
        

    def __call__(self):
        # in the source adapter, update the index of trans to confirmed
        self.trans_manager.update_data(self.trans)
        pass

class SendNotificationObserver:
    def __init__(self, trans, SenderClass):
        # sender class must implement SenderAbstract
        if not issubclass(SenderClass, Sender):
            raise RuntimeError("Not implementing the right sender class")
        self.trans = trans
        self.sender = SenderClass()
    def __call__(self):
        val = self.sender.send(self.trans)
        now = datetime.datetime.now()
        self.trans.notif_builder(now.timestamp()*1000, val)

