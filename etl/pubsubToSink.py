from apache_beam.options.pipeline_options import PipelineOptions
from gutil.GApplication import GApplication
from Parser.Parser import Parser
import apache_beam as beam


class AppOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Input Information
        parser.add_argument('--input_topic', type=str, required=True)
        parser.add_argument('--parser_type', type=str, required=True)
        parser.add_argument('--sink', type=str, required=True)
        parser.add_argument('--sink_opts', type=str, required=True)

def printing(x):
    print(x)
    print(type(x))
    return x
class StreamPubSubToElasticsearch(GApplication):
    def __init__(self, args):
        GApplication.__init__(self)
        self.logger.info("this is the arguments {}".format(args))
        self.pipeline_args = args
        self.sink_opts = eval(args.sink_opts)
        self.parser_type = args.parser_type
        self.input_topic = args.input_topic

    def run(self):
        with beam.Pipeline(options=self.pipeline_args) as pcoll:
            pcoll = pcoll | beam.io.ReadFromPubSub(self.input_topic)
            pcoll = pcoll | beam.ParDo(Parser[self.parser_type]())
            pcoll = pcoll | beam.Map(printing)
	    sink = ElasticSearchWriter(pcoll, self.sink_opts)
            sink.parse()
            sink.write()


if __name__ == '__main__':
    pipeline_args = PipelineOptions(flags=None)
    args = pipeline_args.view_as(AppOptions)
    stream_app = StreamPubSubToElasticsearch(args=args)
    stream_app.start()
