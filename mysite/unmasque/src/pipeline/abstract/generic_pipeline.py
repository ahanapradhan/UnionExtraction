import functools
import threading
import time

from ...core.elapsed_time import create_zero_time_profile
from ...util.Log import Log
from ...util.constants import WAITING, DONE, WRONG, RESULT_COMPARE, START, RUNNING
from ....refactored.executable import Executable
from ....refactored.result_comparator import ResultComparator


def synchronized(wrapped):
    lock = threading.Lock()
    print(lock, id(lock))

    @functools.wraps(wrapped)
    def _wrap(*args, **kwargs):
        with lock:
            result = wrapped(*args, **kwargs)
            return result

    return _wrap


class PipeLineState(object):
    state = None
    info = {}

    def set(self, state):
        self.state = state


class GenericPipeLine:
    _instance = None
    state = WAITING

    # def __new__(cls, *args, **kwargs):
    #     if cls._instance is None:
    #         cls._instance = super(GenericPipeLine, cls).__new__(cls)
    #     return cls._instance

    def __init__(self, connectionHelper, name):
        self.update_state(WAITING)
        self.info = {}
        self.connectionHelper = connectionHelper
        self.pipeline_name = name
        self.time_profile = create_zero_time_profile()
        self.token = None
        self.logger = Log(name, connectionHelper.config.log_level)
        self.correct = False
        self.all_relations = []
        self.error = None

    def process(self, query: str):
        result = None
        try:
            self.update_state(WAITING)
            app = Executable(self.connectionHelper)
            app.method_call_count = 0
            result = self.extract(query)
            self.verify_correctness(query, result)
        except Exception as e:
            print("Some problem while Execution!")
            print(e)
            return result
        else:
            print("Valid Execution")
            return result
        finally:
            print("Ended Execution")

    def doJob(self, query, qe=None):
        local_start_time = time.time()
        if qe is None:
            qe = []
        result = self.process(query)
        qe.clear()
        qe.append(result)
        local_end_time = time.time()
        self.time_profile.update_for_total_time(local_end_time - local_start_time)
        return result

    def verify_correctness(self, query, result):
        self.update_state(RESULT_COMPARE + START)
        self.connectionHelper.connectUsingParams()
        rc = ResultComparator(self.connectionHelper, True)
        rc.set_all_relations(self.all_relations)
        self.update_state(RESULT_COMPARE + RUNNING)
        matched = rc.doJob(query, result)
        self.info[RESULT_COMPARE] = matched
        self.connectionHelper.closeConnection()

        self.time_profile.update_for_result_comparator(rc.local_elapsed_time, rc.app_calls)
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
        self.state = state

    def get_state(self):
        return self.state
