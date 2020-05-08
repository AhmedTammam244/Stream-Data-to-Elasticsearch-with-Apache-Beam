from gutil.LoggingBase import LoggingBase
import timeit
import abc

"""
Any Application that runs in the system have 
    - Logging 
    - Statistics Object: capturing different application stats
and runs through the start method
###############################################################
Applications extending GApplication implements the run method
"""


class GApplication(LoggingBase):

    def __init__(self):
        LoggingBase.__init__(self)  # initialize logging
        self.stats = dict()  # initializing statistics dictionary to log job statistics

    @abc.abstractmethod
    def run(self):
        pass

    def start(self):

        self.stats['start_time'] = timeit.default_timer()  # capturing start time
        self.run()
        self.stats['stop_time'] = timeit.default_timer()  # capturing end time
        self.stats['runtime'] = self.stats['stop_time'] - self.stats['start_time']
        #
        self.logger.info("Application Statistics: {}".format(self.stats))
        self.logger.info('RunTime in seconds: {}'.format(self.stats['runtime']))
