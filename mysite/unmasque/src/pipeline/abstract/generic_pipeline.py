import functools
import threading

from ...core.elapsed_time import create_zero_time_profile
from ...util.constants import WAITING


def synchronized(wrapped):
    lock = threading.Lock()
    print(lock, id(lock))

    @functools.wraps(wrapped)
    def _wrap(*args, **kwargs):
        with lock:
            '''
            print("Calling '%s' with Lock %s from thread %s [%s]"
                  % (wrapped.__name__, id(lock),
                     threading.current_thread().name, time.time()))
            '''
            result = wrapped(*args, **kwargs)
            '''
            print("Done '%s' with Lock %s from thread %s [%s]"
                  % (wrapped.__name__, id(lock),
                     threading.current_thread().name, time.time()))
            '''
            return result

    return _wrap


class PipeLineState(object):
    state = None

    @synchronized
    def set(self, state):
        self.state = state


class GenericPipeLine:
    _instance = None
    state = PipeLineState()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(GenericPipeLine, cls).__new__(cls)
        return cls._instance

    def __init__(self, connectionHelper, name):
        self.connectionHelper = connectionHelper
        self.pipeline_name = name
        self.time_profile = create_zero_time_profile()
        self.token = None

    def doJob(self, args):
        self.update_state(WAITING)
        return self.extract(args)

    def extract(self, query):
        pass

    def update_state(self, state):
        self.state.set(state)

    def get_state(self):
        return self.state.state
