from threading import Timer, Lock
from argparse import ArgumentParser
import os
import pandas as pd
from time import sleep
from datetime import datetime
global count
count = 0
global inittimestamp
global endtimestamp
global flag
flag=False
import json
inittimestamp=None
endtimestamp=None
import math


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

def records_split(name):
    global count
    global flag
    namef1=name[0]
    namef2=name[1]
    split_record=name[2]
    csv_f1=pd.read_csv(namef1,header=None,index_col=False,dtype=str)
    csv_f2=pd.read_csv(namef2, header=None,index_col=False,dtype=str)
    csv=pd.concat([csv_f1, csv_f2], axis=1,ignore_index=True)
    print(len(csv)//split_record)
    if count==math.ceil(len(csv)/split_record):
        flag=True
        return None
    csv=csv[count*split_record:(count+1)*split_record]
    yy, mm, dd = datetime.today().strftime('%Y-%m-%d').split('-')
    csv.to_csv('../data/interim/S1_XX_F1F2_'+str(count*split_record)+'_'+str(dd)+str(mm)+str(yy)+'.csv', header=False,index=None)
    print("File created: "+'../data/interim/S1_XX_F1F2_'+str(count*split_record)+'_'+str(dd)+str(mm)+str(yy)+'.csv')
    count+=1

def timestamp_split(name):
    global inittimestamp
    global endtimestamp
    global count
    global flag
    namef1=name[0]
    namef2=name[1]
    split_duration=name[2]
    csv_f1=pd.read_csv(namef1,header=None,index_col=False,dtype=str)
    csv_f2=pd.read_csv(namef2, header=None,index_col=False,dtype=str)
    csv=pd.concat([csv_f1, csv_f2], axis=1,ignore_index=True)
    if inittimestamp == None:
        inittimestamp=float(csv_f1[0].iloc[0])
        endtimestamp=float(inittimestamp+split_duration)
    elif inittimestamp > float(csv[0].iloc[-1]):
        flag = True
        return None
    csv=csv[(csv[0].astype(float) >= inittimestamp) & (csv[0].astype(float) < endtimestamp)]
    yy, mm, dd = datetime.today().strftime('%Y-%m-%d').split('-')
    csv.to_csv('../data/interim/S1_XX_F1F2_'+str(count*split_duration)+'_'+str(dd)+str(mm)+str(yy)+'.csv', header=False,index=None)
    print("File created: "+'../data/interim/S1_XX_F1F2_'+str(count*split_duration)+'_'+str(dd)+str(mm)+str(yy)+'.csv')
    inittimestamp = endtimestamp
    endtimestamp = inittimestamp + split_duration
    count+=1




def run(split_every,split_duration,split_record):
    parser = ArgumentParser()
    parser.add_argument("-f1", "--file1", dest="filenamef1",
                        help="write report to FILE 1", metavar="FILE1",default='../data/raw/S1_XX_F1_10000_DDMMYYYY.csv')
    parser.add_argument("-f2", "--file2", dest="filenamef2",
                        help="write report to FILE 2", metavar="FILE2", default='../data/raw/S1_XX_F2_10000_DDMMYYYY.csv')
    args = parser.parse_args()
    namef1=args.filenamef1
    namef2=args.filenamef2
    #print(os.mkdir())
    print("starting...")
    with open('testconfig.json') as json_file:
        data = json.load(json_file)
    if data['timestamp']:
        rt = Repeat(split_every, timestamp_split, [namef1,namef2,split_duration])  # it auto-starts, no need of rt.start()
    else:
        rt = Repeat(split_every, records_split, [namef1, namef2,split_record])
    try:
        while True:
            if flag:
                break  # your long-running job goes here...
    finally:
        rt.stop()  # better in a try/finally block to make sure the program ends!

if __name__ == '__main__':
    split_every=2 #split the file every 10 second
    split_duration=1000 #duration of time inside file to split
    split_record=5000 #split every 1000 record
    run(split_every,split_duration,split_record)