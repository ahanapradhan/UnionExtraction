
from ...core.elapsed_time import create_zero_time_profile
from ...util.constants import BACKEND_BUSY
from ...util.queue import Queue


class GenericPipeLine:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(GenericPipeLine, cls).__new__(cls)
        return cls._instance

    def __init__(self, connectionHelper, name):
        self.connectionHelper = connectionHelper
        self.pipeline_name = name
        self.time_profile = create_zero_time_profile()

    def doJob(self, args):
        return self.extract(args)

    def extract(self, query):
        pass
