#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
import uuid

class MRGenerateData(MRJob):

    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, _, line):
        if line[0:11] == "business_id":
            return
        
        yield _, str(uuid.uuid4()) + line[22:]

if __name__ == '__main__':
    MRGenerateData.run()
