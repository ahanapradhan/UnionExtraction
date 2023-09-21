import time

from mysite.unmasque.src.pipeline.abstract.TpchSanitizer import TpchSanitizer


class Base(TpchSanitizer):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Base, cls).__new__(cls)
        return cls._instance

    def __init__(self, connectionHelper, name):
        super().__init__(connectionHelper)
        self.connectionHelper = connectionHelper
        self.extractor_name = name
        self.method_call_count = 0
        self.local_start_time = None
        self.local_end_time = None
        self.local_elapsed_time = None
        self.done = False
        self.result = None

    def doJob(self, *args):
        self.local_start_time = time.time()
        self.result = self.doActualJob(args)
        self.local_end_time = time.time()
        self.local_elapsed_time = self.local_end_time - self.local_start_time
        self.done = True
        self.method_call_count += 1
        return self.result

    def doActualJob(self, args):
        pass

    def sanitize(self):
        super().doJob()

    def extract_params_from_args(self, args):
        pass

    def print_elapsed_time(self):
        print(str(self.extractor_name) + ".:Elapsed time: ... " + str(self.local_elapsed_time))
