import time
from typing import Any


class Base:
    _instance = None
    method_call_count = 0
    connectionHelper = None
    local_start_time = None
    local_end_time = None
    local_elapsed_time = None
    extractor_name = "Base"

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Base, cls).__new__(cls)
        return cls._instance

    def __init__(self, connectionHelper, name):
        self.connectionHelper = connectionHelper
        self.extractor_name = name

    def doJob(self, param: Any):
        self.local_start_time = time.time()
        self.doActualJob(param)
        self.local_end_time = time.time()

    def doActualJob(self, param: Any):
        pass

    def print_elapsed_time(self):
        self.local_elapsed_time = self.local_end_time - self.local_start_time
        print(str(self.extractor_name) + ".:Elapsed time: ... " + str(self.local_elapsed_time))
