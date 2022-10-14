#!python3

from mrjob.job import MRJob
from mrjob.step import MRStep

import csv
import json

cols = 'transaction_id,customer_id,customer_name,customer_birthdate,customer_gender,customer_country,transaction_date,transaction_product,transaction_amount'.split(',')

def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row

class MapReduce(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.sort)
        ]

    def mapper(self, _, line):
        # Convert each line into a dictionary
        row = dict(zip(cols, csv_readline(line)))

        #skip first row as header
        if row['transaction_id'] != 'transaction_id':
            # Yield the order_date
            yield row['transaction_date'][0:4], int(row['transaction_amount'])

    def reducer(self, key, values):
        yield None, (key,sum(values))
    
    def sort(self, key, values):
        data = []
        for transaction_date, transaction_total in values:
            data.append((transaction_date, transaction_total))
            data.sort()

        for transaction_date, transaction_total in data:
           yield transaction_date, transaction_total

if __name__ == '__main__':
    MapReduce.run()