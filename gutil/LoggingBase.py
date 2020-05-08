import logging

"""
Any application that runs through the system extends LoggingBase class
"""


class LoggingBase(object):

    def __init__(self):
        logging.basicConfig()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
