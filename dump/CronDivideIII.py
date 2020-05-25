from threading import Timer, Lock
from argparse import ArgumentParser
import os
import pandas as pd
from time import sleep
from apscheduler.schedulers.blocking import BlockingScheduler



global count
count = 0


class Repeat(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer     = None
        self.interval   = interval
        self.function   = function
        self.args       = args
        self.kwargs     = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False


class AsyncWrite(threading.Thread):

    def __init__(self, text, out):
        # calling superclass init
        threading.Thread.__init__(self)
        self.text = text
        self.out = out

    def run(self):
        f = open(self.out, "a")
        f.write(self.text + '\n')
        f.close()

        # waiting for 2 seconds after writing
        # the file
        time.sleep(2)
        print("Finished background file write to",
              self.out)

def timestamp(name):
    global count
    csv=pd.read_csv(name,header=None)
    csv=csv.iloc[(count%10)*1000:((count+1)%10)*1000, :]
    string=csv.to_csv(name[:-4]+str(count)+'.csv',header=False)
    print("File created: "+name[:-4]+str(count)+'.csv')
    background.join()
    count+=1




def run():
    parser = ArgumentParser()
    parser.add_argument("-f", "--file", dest="filename",
                        help="write report to FILE", metavar="FILE",default='../data/S1_XX_F1_10000_DDMMYYYY.csv')
    args = parser.parse_args()
    name=args.filename
    print("starting...")
    while True:
        timestamp(name)
        sleep(10)
if __name__ == '__main__':

    run()