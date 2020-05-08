import apache_beam as beam
from apache_beam.io import iobase
from elasticsearch import Elasticsearch
from elasticsearch import helpers


class ElasticSearchWriter(iobase.Writer):

    def __init__(self, client, batch_size=None):
        self.client = client
        self.batch_size = batch_size

    def write(self, value):
        index_name = value["_index"]
        del value["_index"]
        self.client.index(index=index_name, body=value)

    def close(self):
        pass
