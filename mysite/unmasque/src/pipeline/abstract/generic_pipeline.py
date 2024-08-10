import functools
import threading
import time
from abc import abstractmethod, ABC

from ...core.elapsed_time import create_zero_time_profile
from ...core.factory.ExecutableFactory import ExecutableFactory
from ...util.Log import Log
from ...util.constants import WAITING, DONE, WRONG, RESULT_COMPARE, START, RUNNING, ERROR
from ....src.core.result_comparator import ResultComparator


def synchronized(wrapped):
    lock = threading.Lock()

    # print(lock, id(lock))

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


class GenericPipeLine(ABC):
    _instance = None

    def __init__(self, connectionHelper, name):
        self.state = [WAITING]
        self.done_states = []
        self.update_state(WAITING)
        self.info = {}
        self.IO = {}
        self.connectionHelper = connectionHelper
        self.pipeline_name = name
        self.time_profile = create_zero_time_profile()
        self.token = None
        self.logger = Log(name, connectionHelper.config.log_level)
        self.correct = False
        self.all_sizes = {}
        self.error = ""
        self.core_relations = None

    def process(self, query, speed=1.0):
        try:
            self.update_state(WAITING)
            exe_factory = ExecutableFactory()
            app = exe_factory.create_exe(self.connectionHelper)
            """
            try:
                self.connectionHelper.connectUsingParams()
                check_result = app.doJob(query)
                self.connectionHelper.closeConnection()
                if isinstance(check_result, str):
                    return check_result
            except Exception as e:
                self.connectionHelper.closeConnection()
                return str(e)
            """
            app.method_call_count = 0
            result = self.extract(query, speed)
            if result is None:
                result = self.error
                self.update_state(ERROR)
                return result
            self.verify_correctness(query, result)
            self.time_profile.update_for_app(app.method_call_count)
            return result
        except Exception as e:
            self.error += str(e)
            self.logger.error(self.error)
            self.update_state(ERROR)
            return self.error
        finally:
            self.update_state(DONE)

    def doJob(self, query, qe=None, speed=0.5):
        local_start_time = time.time()
        if qe is None:
            qe = []
        result = self.process(query, speed)
        qe.clear()
        qe.append(result)
        local_end_time = time.time()
        self.time_profile.update_for_total_time(local_end_time - local_start_time)
        return result

    def verify_correctness(self, query, result):
        self.update_state(RESULT_COMPARE + START)
        self.connectionHelper.connectUsingParams(True)
        rc = ResultComparator(self.connectionHelper, True, self.core_relations)
        self.update_state(RESULT_COMPARE + RUNNING)
        matched = rc.doJob(query, result)
        if not matched:
            self.logger.debug("Hash comparator failed. Going for comparison!..")
            rc = ResultComparator(self.connectionHelper, False, self.core_relations)
            self.update_state(RESULT_COMPARE + RUNNING)
            matched = rc.doJob(query, result)
        self.info[RESULT_COMPARE] = matched
        self.connectionHelper.closeConnection()

        self.time_profile.update_for_result_comparator(rc.local_elapsed_time, rc.app_calls)
        if matched:
            self.logger.info("Extracted Query is Correct.")
            self.correct = True
        else:
            self.logger.info("Extracted Query seems different!.")
            self.correct = False
            self.update_state(WRONG)
        self.update_state(RESULT_COMPARE + DONE)

    @abstractmethod
    def extract(self, query, speed):
        pass

    def update_state(self, state):
        self.state.append(state)
        if DONE in state:
            self.done_states.append(state)

    def get_state(self):
        return self.state[-1]

    def get_list(self):
        return self.state

    def get_done_list(self):
        return self.done_states
