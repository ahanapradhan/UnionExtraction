
from ...core.elapsed_time import create_zero_time_profile
from ...util.constants import BACKEND_BUSY
from ...util.queue import Queue


class RateLimitedPipeLine:
    _instance = None
    q = Queue()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(RateLimitedPipeLine, cls).__new__(cls)
        return cls._instance

    def __init__(self, connectionHelper, name):
        self.connectionHelper = connectionHelper
        self.pipeline_name = name
        self.time_profile = create_zero_time_profile()

    def doJob(self, args):
        query = args
        self.q.enqueue(query)
        '''
        To-Do: remove this busy loop
        '''
        front = self.q.peek()
        while front != query:
            print(BACKEND_BUSY)
            pass
        self.extract(query)
        self.q.dequeue()

    def extract(self, query):
        print(query)
