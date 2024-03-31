import time


from ...src.pipeline.abstract.TpchSanitizer import TpchSanitizer
from ...src.util.ConnectionHelper import ConnectionHelper
from ...src.util.Log import Log


class Base(TpchSanitizer):
    _instance = None
    method_call_count = 0

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Base, cls).__new__(cls)
        return cls._instance

    def __init__(self, connectionHelper: ConnectionHelper, name: str):
        super().__init__(connectionHelper)
        self.connectionHelper = connectionHelper
        self.extractor_name = name
        self.local_start_time = None
        self.local_end_time = None
        self.local_elapsed_time = None
        self.done = False
        self.result = None
        self.logger = Log(name, connectionHelper.config.log_level)

    def doJob(self, *args):
        self.local_start_time = time.time()
        try:
            self.result = self.doAppCountJob(args)
        except Exception as e:
            raise e
        else:
            self.done = True
            return self.result
        finally:
            self.local_end_time = time.time()
            self.local_elapsed_time = self.local_end_time - self.local_start_time
            self.method_call_count += 1

    def doActualJob(self, args):
        pass

    def doAppCountJob(self, args):
        return self.doActualJob(args)

    def extract_params_from_args(self, args):
        pass

    def print_elapsed_time(self):
        print(str(self.extractor_name) + ".:Elapsed time: ... " + str(self.local_elapsed_time))
