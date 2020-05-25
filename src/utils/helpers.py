import threading
from os import listdir
from os.path import isfile, join
from time import sleep
import os
import pandas as pd
from datetime import datetime
global count
from . import thread_manager
count = 0
global inittimestamp
global endtimestamp
global flag
flag=False
inittimestamp=None
endtimestamp=None
import math
global rt

#settings.init()
def directory_watch(filesystem,settings):
    while settings.directorywatch:
        #List of files updated in the directory
        settings.updatedfiles=[f for f in listdir(filesystem) if isfile(join(filesystem, f))]
        if sorted(settings.updatedfiles) != sorted(settings.initfiles):
            for file in settings.updatedfiles:
                if file not in settings.initfiles:
                    settings.initfiles=settings.initfiles+[file]
                    settings.beamprocessingstack.append(file)
                    print(file+" Processed")
        else:
             pass
        sleep(1)

def file_watch(filesystem, settings):
    while settings.filewatch:
        # Timestamp of file modification
        settings.updatedfile = os.stat(filesystem).st_mtime
        if settings.updatedfile!=settings.initfile:
            settings.initfile=settings.updatedfile
            settings.recordqueue+=1
            print(settings.recordqueue)
        else:
            pass
        sleep(1)



def watch_filesystem(filesystem,settings,tmm):
    if settings.directorywatch:
        settings.initfiles = [f for f in listdir(filesystem) if isfile(join(filesystem, f))]
        detectchange=directory_watch
    elif settings.filewatch:
        settings.initfile = os.stat(filesystem).st_mtime
        detectchange=file_watch
    tmm.continous_thread(target=detectchange, args=[filesystem,settings])





def records_split(name):
    global count
    rt=name[3]
    namef1=name[0]
    namef2=name[1]
    split_record=name[2]
    csv_f1=pd.read_csv(namef1,header=None,index_col=False,dtype=str)
    csv_f2=pd.read_csv(namef2, header=None,index_col=False,dtype=str)
    csv=pd.concat([csv_f1, csv_f2], axis=1,ignore_index=True)
    print(len(csv)//split_record)
    if count==math.ceil(len(csv)/split_record):
        rt.stop()
        return None
    csv=csv[count*split_record:(count+1)*split_record]
    yy, mm, dd = datetime.today().strftime('%Y-%m-%d').split('-')
    csv.to_csv('../data/interim/S1_XX_F1F2_'+str(count*split_record)+'_'+str(dd)+str(mm)+str(yy)+'.csv', header=False,index=None)
    print("File created: "+'../data/interim/S1_XX_F1F2_'+str(count*split_record)+'_'+str(dd)+str(mm)+str(yy)+'.csv')
    count+=1
    return None

def timestamp_split(name):
    global inittimestamp
    global endtimestamp
    global count
    namef1=name[0]
    namef2=name[1]
    split_duration=name[2]
    rt = name[3]
    csv_f1=pd.read_csv(namef1,header=None,index_col=False,dtype=str)
    csv_f2=pd.read_csv(namef2, header=None,index_col=False,dtype=str)
    csv=pd.concat([csv_f1, csv_f2], axis=1,ignore_index=True)
    if inittimestamp == None:
        inittimestamp=float(csv_f1[0].iloc[0])
        endtimestamp=float(inittimestamp+split_duration)
    elif inittimestamp > float(csv[0].iloc[-1]):
        rt.stop()
        return None
    csv=csv[(csv[0].astype(float) >= inittimestamp) & (csv[0].astype(float) < endtimestamp)]
    yy, mm, dd = datetime.today().strftime('%Y-%m-%d').split('-')
    csv.to_csv('../data/interim/S1_XX_F1F2_'+str(count*split_duration)+'_'+str(dd)+str(mm)+str(yy)+'.csv', header=False,index=None)
    print("File created: "+'../data/interim/S1_XX_F1F2_'+str(count*split_duration)+'_'+str(dd)+str(mm)+str(yy)+'.csv')
    inittimestamp = endtimestamp
    endtimestamp = inittimestamp + split_duration
    count+=1
    return None

