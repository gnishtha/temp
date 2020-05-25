from __future__ import absolute_import
import argparse
import json
from config.filesystem_watch import filesystem_data as fsd
from utils.helpers import watch_filesystem
from utils.thread_manager import ThreadManagerMain
from data.data_ingestion import MyBeamClass
import os
import sys

class init_params:
    def __init__(self,input_dir,output_dir,mode,output_split,header,delimiter,output_schema_timestamp,output_schema_label,output_schema_first_set,output_schema_second_set):
        self.input_dir=input_dir
        self.output_dir=output_dir
        self.header=header
        self.delimiter=delimiter
        self.pipeline_args=None
        self.output_schema_timestamp = output_schema_timestamp
        self.output_schema_first_set = output_schema_first_set
        self.output_schema_second_set = output_schema_second_set
        self.output_schema_label=output_schema_label
        self.output_split=output_split
        self.mode=mode
        self.keep_running=True


    def set_terminal(self,argv=None):
        """The main function which creates the pipeline and runs it."""
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--input',
            dest='input',
            required=False,
            help='Input file to read. This can be a local file or '
                 'a file in a Google Storage Bucket.',
            default=os.path.normpath(os.getcwd() + os.sep + os.pardir).replace('\\','/')+'/data/interim/' )
        parser.add_argument(
            '--header',
            dest='header',
            required=False,
            help='Data Definition',
            default=[A, B, C, D, E, F,G,H,I,J] )

        parser.add_argument('--output',
                                dest='output',
                                required=False,
                                help='Output directory',
                                default=os.path.normpath(os.getcwd() + os.sep + os.pardir).replace('\\','/')+'/data/processed/')

        parser.add_argument('--delim',
                    dest='delim',
                    required=False,
                    help='Delimiter',
                    default=",")
        # Parse arguments from the command line.
        known_args, self.pipeline_args = parser.parse_known_args(argv)
        self.input_dir=known_args.input
        self.output_dir=known_args.output
        header=known_args.header
        self.header = map(str, header.strip('[]').split(','))
        self.delimiter=known_args.delim

    def run(self,settings):
        myObj=MyBeamClass(self)
        myObj.ingestion()
        print(settings.initfiles)
        while(self.keep_running):
            if self.mode == "on-demand" and self.output_split:
                if len(settings.initfiles)!=0:
                    current_file=settings.initfiles.pop(0)
                    myObj.extract(current_file)
                else:
                    self.keep_running=False
            elif self.mode=="on-demand" and not self.output_split:
                current_file='*'
                myObj.extract(current_file)
                self.keep_running = False
            if self.mode == "event-driven" and self.output_split:
                if len(settings.beamprocessingstack)!=0:
                    current_file=settings.beamprocessingstack.pop(0)
                    myObj.extract(current_file)
            elif self.mode == "event-driven" and not self.output_split:
                if len(settings.beamprocessingstack) != 0:
                    current_file = settings.beamprocessingstack.pop(0)
                    myObj.extract(current_file)


            #sleep(2)



if __name__ == '__main__':
    input_dir = os.path.normpath(os.getcwd() + os.sep+os.pardir).replace('\\', '/') + '/data/interim/'
    output_dir = os.path.normpath(os.getcwd() + os.sep+os.pardir ).replace('\\', '/') + '/data/processed/'
    header = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"]
    delimiter = ","
    pipeline_args = None
    output_schema_timestamp="A"
    output_schema_first_set=["D", "E"]
    output_schema_second_set=["B", "C","D", "E", "F", "G", "H", "I", "J"]
    output_schema_label="temp"
    output_split=False
    mode="simulation" #on-demand,simulation,event-driven

########----to be removed----###########
    ## Control of tester ##
    if mode=="simulation":
        namef1=os.path.normpath(os.getcwd() + os.sep+os.pardir).replace('\\', '/') + '/data/raw/S1_XX_F1_10000_DDMMYYYY.csv'
        namef2=os.path.normpath(os.getcwd() + os.sep+os.pardir).replace('\\', '/') + '/data/raw/S1_XX_F2_10000_DDMMYYYY.csv'
        split_timestamp=False
        split_every=10 #split the file every 10 second
        split_length=1000 #duration of time inside file to split or split every N records
        mode="event-driven"
        tmm=ThreadManagerMain()
        tmm.splitter_thread(split_timestamp, split_every, namef1, namef2, split_length)


########----to be removed----###########
    if mode=="event-driven":
        settings=fsd()
        watch_filesystem(input_dir,settings,tmm)

    obj = init_params(input_dir, output_dir,mode, output_split, header, delimiter, output_schema_timestamp,
                      output_schema_label, output_schema_first_set, output_schema_second_set)
    try:
        obj.run(settings)
    except (KeyboardInterrupt, SystemExit):
        tmm.detach_thread(settings)



