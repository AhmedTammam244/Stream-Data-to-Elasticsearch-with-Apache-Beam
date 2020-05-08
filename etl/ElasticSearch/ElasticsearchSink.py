from apache_beam.io import iobase
from elasticsearch import Elasticsearch
from etl.ElasticSearch.ElasticsearchWriter import ElasticSearchWriter


class ElasticSearchSink(iobase.Sink):

    def __init__(self, args):
        self.args = args

    def initialize_write(self):
        es = Elasticsearch([{'host': self.args["host"], 'port': self.args["port"]}],
                           http_auth=self.args["http_auth"], scheme=self.args["scheme"])
        return es

    def open_writer(self, init_result, uid):
        batch_size = self.args['batch_size'] if 'batch_size' in self.args else None
        return ElasticSearchWriter(init_result, batch_size)

    def pre_finalize(self, init_result, writer_results):
        pass

    def finalize_write(self, init_result, writer_results, pre_finalize_result):
        init_result.transport.close()
