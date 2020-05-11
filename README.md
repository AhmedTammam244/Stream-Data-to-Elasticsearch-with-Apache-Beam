
<img src="https://github.com/AhmedTammam244/Stream-Data-to-Elasticsearch-with-Apache-Beam/blob/master/PubsubToElasticsearch.png"
 alt="PubsubToElasticsearch logo" title="PubsubToElasticsearch" />

# Stream Data to Elasticsearch with Apache Beam
# problem : if you want your pipeline to read data from (or write data to) a storage system for which the Beam SDK for Python does not provide native support.
May 11 ,2020    
Author : Ahmed Tammam             

In this post I walk through the process of handling unbounded streaming data using Apache Beam, and pushing it to Elasticsearch  as a data warehouse.

install beam  https://beam.apache.org/get-started/quickstart-py/ 

1. Read messages from a Pub/Sub topic :    
  it is a streaming pipeline that reads Pub/Sub messages from a Pub/Sub topic.
  
            def run(self):
            with beam.Pipeline(options=self.pipeline_args) as pcoll:
                pcoll = pcoll | beam.io.ReadFromPubSub(self.input_topic)

2. Creating a New Sink :
     You should create a new sink if you’d like to use the advanced features that the Sink API provides, such as global            initialization and finalization that allow the write operation to appear “atomic” (i.e. either all data is written or        none is).

    A sink represents a resource that can be written to using the Write transform. A parallel write to a sink consists of         three phases:

    A sequential initialization phase. For example, creating a temporary output directory.
    A parallel write phase where workers write bundles of records.
    A sequential finalization phase. For example, merging output files.
    For example, if you’d like to write to a new table in a database, you should use the Sink API. In this case, the             initializer will create a temporary table, the writer will write rows to it, and the finalizer will rename the table to     a final location.

    To create a new data sink for your pipeline, you’ll need to provide the format-specific logic that tells the sink how to     write bounded data from your pipeline’s PCollections to an output sink. The sink writes bundles of data in parallel           using multiple workers.

    You supply the writing logic by creating the following classes:

    A subclass of Sink, which you can find in the iobase.py module. Sink describes the location or resource to write to.        Depending on the type of sink, your Sink subclass may contain fields such as the path to an output directory on a            filesystem or a database table name. Sink provides three methods for performing a write operation to the sink it              describes. Your subclass of Sink must implement these three methods: initialize_write(), open_writer(), and                  finalize_write().

   A subclass of Writer, which you can find in the iobase.py module. Writer writes a bundle of elements from an input            PCollection to your designated data sink. Writer defines two methods: write(), which writes a single record from the          bundle, and close(), which is called once at the end of writing a bundle.

3. Implementing the Sink Subclass :
    Your Sink subclass describes the location or resource to which your pipeline writes its output. This might include a         file system location, the name of a database table or dataset, etc.

    To implement a Sink, your subclass must override the following methods:

    initialize_write: This method performs any necessary initialization before writing to the output location. Services call     this method before writing begins. For example, you can use initialize_write to create a temporary output directory.

    open_writer: This method enables writing a bundle of elements to the sink.

    finalize_write:This method finalizes the sink after all data is written to it. Given the result of initialization and an     iterable of results from bundle writes, finalize_write performs finalization after writing and closes the sink. This         method is called after all bundle write operations are complete.

    Caution: initialize_write and finalize_write are conceptually called once: at the beginning and end of a Write               transform.     However, when you implement these methods, you must ensure that they are idempotent, as they may be           called multiple times     on different machines in the case of failure, retry, or for redundancy.

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

4. Implementing the Writer Subclass :
    Your Writer subclass implements the logic for writing a bundle of elements from a PCollection to output location defined     in your Sink. Services may instantiate multiple instances of your Writer in different threads on the same worker, so         access to any static members or methods must be thread-safe.

    To implement a Writer, your subclass must override the following abstract methods:

    write: This method writes a value to your Sink using the current writer.
    
    close: This method closes the current writer.

         def __init__(self, client, batch_size=None):
              self.client = client
              self.batch_size = batch_size

          def write(self, value):
              index_name = value["_index"]
              del value["_index"]
              self.client.index(index=index_name, body=value)

          def close(self):
              pass

5. Writing to a New Sink :
  The following code demonstrates how to write to the sink using the Write transform
  
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

# Test ^_^  
     $   #!/usr/bin/env bash     
     $  /home/{machine_name}/miniconda3/envs/rec-3.5/bin/python3  /home/{machine_name}/etl/pubsubToSink.py    \
              --runner DirectRunner     \
              --temp_location gs://bucket_name/temp  \
              --max_num_workers 6    \
              --input projects/{project_name}/topics/good   \
              --region us-central1          \
              --streaming flag     \
              --project {project_name}       \
              --sink_opts "{'host':'localhost','port':9200,'scheme':'http','http_auth':('username', 'password')}"

Great Thanks to Ali Mohamed ( https://www.linkedin.com/in/ali-mohamed-332193152/ ) for helping me  to publish this essay 
reference : http://beam.apachecn.org/documentation/sdks/python-custom-io/
