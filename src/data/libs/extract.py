import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os
class WriteToSeparateFiles(beam.DoFn):
    def __init__(self, outdir):
        self.outdir = outdir
    def process(self, element):
        if os.path.exists(self.outdir):
            append_write = 'a'  # append if already exists
        else:
            append_write = 'w'
        file_object = open(self.outdir, append_write)
        file_object.write(element+"\n")
        file_object.close()



class DataExtract():
    def beam_simplerun(self,current_file):
        pipeline_args=self.pipeline_args
        data_ingestion=self.ingestion
        inn=self.inn
        out=self.out
        p = beam.Pipeline(options=PipelineOptions(pipeline_args))
        (p
         # Read the file. This is the source of the pipeline. All further
         # processing starts with lines read from the file. We use the input
         # argument from the command line. We also skip the first line which is a
         # header row.
         | 'Read from a File' >> beam.io.ReadFromText(inn + current_file,
                                                      min_bundle_size=1000)
         # This stage of the pipeline translates from a CSV file single row
         # input as a string, to a dictionary object consumable by BigQuery.
         # It refers to a function we have written. This function will
         # be run in parallel on different workers using input from the
         # previous stage of the pipeline.
         | 'String To key value Row' >>
         beam.Map(lambda s: data_ingestion.parse_method(s))
         | beam.io.WriteToText(out + current_file))
        p.run()

    def beam_ondemand(self,current_file):
        pipeline_args=self.pipeline_args
        data_ingestion=self.ingestion
        inn=self.inn
        out=self.out
        p = beam.Pipeline(options=PipelineOptions(pipeline_args))
        (p
         # Read the file. This is the source of the pipeline. All further
         # processing starts with lines read from the file. We use the input
         # argument from the command line. We also skip the first line which is a
         # header row.
         | 'Read from a File' >> beam.io.ReadFromText(inn + current_file,
                                                      min_bundle_size=1000)
         # This stage of the pipeline translates from a CSV file single row
         # input as a string, to a dictionary object consumable by BigQuery.
         # It refers to a function we have written. This function will
         # be run in parallel on different workers using input from the
         # previous stage of the pipeline.
         | 'String To key value Row' >>
         beam.Map(lambda s: data_ingestion.parse_method(s))
         | beam.io.WriteToText(out + '/results.txt'))
        p.run()

    def beam_appendrun(self,current_file):
        pipeline_args=self.pipeline_args
        data_ingestion=self.ingestion
        inn=self.inn
        out=self.out
        p = beam.Pipeline(options=PipelineOptions(pipeline_args))
        (p
         # Read the file. This is the source of the pipeline. All further
         # processing starts with lines read from the file. We use the input
         # argument from the command line. We also skip the first line which is a
         # header row.
         | 'Read from a File' >> beam.io.ReadFromText(inn + current_file,
                                                      min_bundle_size=1000)
         # This stage of the pipeline translates from a CSV file single row
         # input as a string, to a dictionary object consumable by BigQuery.
         # It refers to a function we have written. This function will
         # be run in parallel on different workers using input from the
         # previous stage of the pipeline.
         | 'String To key value Row' >>
         beam.Map(lambda s: data_ingestion.parse_method(s))
         | beam.ParDo(WriteToSeparateFiles(out + "/results.txt"))
         )
        p.run()