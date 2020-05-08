from apache_beam.options.pipeline_options import PipelineOptions
from elasticsearch import Elasticsearch
from etl.ElasticSearch.ElasticsearchSink import ElasticSearchSink
from etl.writers.PluggableWritableInterface import PluggableWritableInterface
import apache_beam as beam
import json

def generate_actions(datapoint):
    return {
        "_index": "test_events",
        "doc": datapoint
    }


class ElasticSearchWriter(PluggableWritableInterface):
    def __int__(self, pcoll, kv_dict):
        super(ElasticSearchWriter, self).__init__(pcoll, kv_dict)

    def parse(self):
        self.transformed_p = self.pcoll | "Transforming Records" >> beam.Map(generate_actions)

    def write(self):
        self.transformed_p = self.transformed_p | "printing Records" >> beam.Map(printing)
        self.logger.info("writing records to Elastic Search ")
        self.transformed_p = (self.transformed_p
                              | 'Writing [Records] To Elastic Search'
                              >> beam.io.Write(ElasticSearchSink(self.kv_dict))
                              )
