import apache_beam as beam

class Parser(beam.DoFn):
    def __init__(self):
        super(SnowPlowParser, self).__init__()

    def process(self, element, *args, **kwargs):
        lines = element.decode("utf-8")
        lines = lines.split("\n")
        lines = eval(lines)
        yield lines

