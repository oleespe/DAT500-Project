#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class MRStructureData(MRJob):

    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, _, line):
        if line[0:11] == "business_id":
            return
        segment_indices = [0] # List that contains the index of every new segment of the csv.
        
        # Determine whether we are inside a set of qutaion marks, so as to ignore any comma-characters within.
        within_quotes = False
        for i in range(len(line)-1): 
            # Add the -1 here to avoid the scenario where a comma-character is the final character in a line, 
            # which could result in an index out of bounds error.
            if line[i] == '"':
                within_quotes = not within_quotes
            elif line[i] == "," and not within_quotes:
                segment_indices.append(i+1)

        # Column indices to retrieve
        indices = [0, 6, 7, 8]

        result = ""
        for i in range(len(indices)):
            result += line[segment_indices[indices[i]]:segment_indices[indices[i]+1]-1]
            if i != len(indices)-1:
                result += ","
        yield _, result

    # def combiner(self, key, values):
    #    yield key, next(values)

    #def reducer(self, key, values):
    #    yield key, next(values)


if __name__ == '__main__':
    MRStructureData.run()
