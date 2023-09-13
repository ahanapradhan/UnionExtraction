from ...core.elapsed_time import create_zero_time_profile


class RateLimitedPipeLine:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(RateLimitedPipeLine, cls).__new__(cls)
        return cls._instance

    def __init__(self, connectionHelper, name):
        self.connectionHelper = connectionHelper
        self.pipeline_name = name
        self.time_profile = create_zero_time_profile()

    def extract(self, query):
        print(query)
