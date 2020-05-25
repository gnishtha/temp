import re
import time
from datetime import datetime
import os
def time_convert(timestamp):
    sec,nanosec=timestamp.split('.')
    epoch=time.mktime(tuple(map(int,list(datetime.today().strftime('%Y-%m-%d').split('-')+ ['00', '00', '00', '00', '00', '00']))))
    epoch=str(int(epoch)+int(sec))+nanosec[:]
    return epoch

class DataIngestion:
    """A helper class which contains the logic to translate the file into
    a format dictionary based databases will accept."""
    def __init__(self,param):
        self.delim=param.delim
        self.header=param.header
        self.output_schema_timestamp = param.output_schema_timestamp
        self.output_schema_first_set = param.output_schema_first_set
        self.output_schema_second_set = param.output_schema_second_set
        self.output_schema_label = param.output_schema_label
    def parse_method(self, string_input):
        # Strip out carriage return, newline and quote characters.
        values = re.split(self.delim,
                          re.sub('\r\n', '', re.sub(u'"', '', string_input)))
        firstset=self.output_schema_label+","
        secondset=""
        for i in range(len(values)):
            if self.header[i] in self.output_schema_first_set:
                firstset += self.header[i] + "=" + str(values[i]) + ","
            elif self.header[i] in self.output_schema_second_set:
                secondset += self.header[i] + "=" + str(values[i]) + ","
            elif self.header[i]==self.output_schema_timestamp:
                timestamp=time_convert(str(values[i]))
            else:
                raise ValueError("Output Schema not specified")
            row=firstset[:-1]+" "+secondset[:-1]+" "+timestamp
        return row

