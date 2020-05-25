from .libs.extract import DataExtract
from .libs.ingestion import DataIngestion
class MyBeamClass(DataExtract):
    def __init__(self,parent):
        self.pipeline_args=parent.pipeline_args
        self.inn=parent.input_dir
        self.out=parent.output_dir
        self.delim=parent.delimiter
        self.header=parent.header
        self.output_schema_timestamp = parent.output_schema_timestamp
        self.output_schema_first_set = parent.output_schema_first_set
        self.output_schema_second_set = parent.output_schema_second_set
        self.output_schema_label = parent.output_schema_label
        self.output_split=parent.output_split
        self.mode=parent.mode
        self.obj=parent

    def ingestion(self):
        # decides how to read the data from source
        self.ingestion=DataIngestion(self)

    def extract(self,currentfile):
        # decides which method of DataExtract class to use
        if self.mode=="on-demand":
            if self.output_split:
                self.beam_simplerun(currentfile)
            else:
                self.beam_ondemand(currentfile)
        else:
            if self.output_split:
                self.beam_simplerun(currentfile)
            else:
                self.beam_appendrun(currentfile)


