import functools
import threading

from ...core.elapsed_time import create_zero_time_profile
from ...util.Log import Log
from ...util.constants import WAITING, DONE, WRONG, RESULT_COMPARE, START, RUNNING
from ....refactored.result_comparator import ResultComparator


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
        self.logger = Log(name, connectionHelper.config.log_level)
        self.correct = False
        self.all_relations = []
        self.validate_extraction = True

    def doJob(self, query):
        self.update_state(WAITING)
        result = self.extract(query)
        if self.validate_extraction:
            self.verify_correctness(query, result)
        return result

    def verify_correctness(self, query, result):
        self.update_state(RESULT_COMPARE + START)
        self.connectionHelper.connectUsingParams()
        rc = ResultComparator(self.connectionHelper, False)
        rc.set_all_relations(self.all_relations)
        self.update_state(RESULT_COMPARE + RUNNING)
        matched = rc.doJob(query, result)
        self.connectionHelper.closeConnection()

        self.time_profile.update_for_result_comparator(rc.local_elapsed_time)
        if matched:
            self.logger.info("Extracted Query is Correct.")
            self.correct = True
            self.update_state(DONE)
        else:
            self.logger.info("Extracted Query seems different!.")
            self.correct = False
            self.update_state(WRONG)

    def extract(self, query):
        pass

    def update_state(self, state):
        self.state.set(state)

    def get_state(self):
        return self.state.state
