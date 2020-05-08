import abc
from gutil.LoggingBase import LoggingBase

class PluggableWritableInterface(LoggingBase):
    __metaclass__ = abc.ABCMeta

    def __init__(self, pcoll, kv_dict):
        """
        :param pcoll: p-collection
        :param kv_dict: dictionary containing key: value fields that are specific to data sink (ex. connection details)
        """
        LoggingBase.__init__(self)
        self.pcoll = pcoll
        self.kv_dict = kv_dict
        self.transformed_p = None

    @abc.abstractmethod
    def parse(self):
        """
        [SINK SPECIFIC]
        Transforms the input PCollection stored in self.pcoll to a p_collection that holds suitable schema for the
        desired SINK.

        The output MUST BE stored in self.transformed_p to allow unified analytics (if we will)
        on the transformed_p later
        :return:
        """
        pass

    @abc.abstractmethod
    def write(self):
        """
        [SINK SPECIFIC]
        writes self.transformed_p into the desired SINK
        :return:
        """
        pass
