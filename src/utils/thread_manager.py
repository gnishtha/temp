from threading import Timer
from . import helpers
import threading
import sys
class Repeat(object):
    def __init__(self, interval, function, **kwargs):
        self._timer     = None
        self.interval   = interval
        self.function   = function
        self.args       = None
        self.kwargs     = kwargs
        self.is_running = False
        #self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def setargs(self,*args):
        self.args=args

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.daemon = True
            self._timer.start()
            self.is_running = True

    def stop(self):
        print('stopping')
        self._timer.cancel()
        self.is_running = False

class ThreadManagerMain():
    def continous_thread(self,target,args):
        self.load_thread = threading.Thread(target=target, args=args)
        self.load_thread.start()

    def splitter_thread(self, split_timestamp, split_every, namef1, namef2, split_length):
            if split_timestamp:
                rt = Repeat(split_every, helpers.timestamp_split)
                rt.setargs([namef1, namef2, split_length, rt])
                rt.start()  # it auto-starts, no need of rt.start()
                self.st=rt
            else:
                rt = Repeat(split_every, helpers.records_split)
                rt.setargs([namef1, namef2, split_length, rt])
                rt.start()  # it auto-starts, no need of rt.start()
                self.st = rt
    def detach_thread(self,settings):
        self.st.stop()
        self.load_thread.shutdown = True
        settings.directorywatch = False
        settings.directorywatch = False
        self.load_thread.join()
        sys.exit()




