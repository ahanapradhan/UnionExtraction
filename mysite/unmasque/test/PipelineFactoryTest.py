import threading
import unittest
from time import sleep

from mysite.unmasque import views
from mysite.unmasque.src.core.factory.PipeLineFactory import PipeLineFactory
from mysite.unmasque.src.pipeline.abstract.generic_pipeline import GenericPipeLine
from mysite.unmasque.src.util.PostgresConnectionHelper import PostgresConnectionHelper
from mysite.unmasque.src.util.configParser import Config
from mysite.unmasque.src.util.constants import START, WAITING, RUNNING, DONE, WRONG, DB_MINIMIZATION
from mysite.unmasque.test.util import queries

EXTRACTION_TIME = 10
NUM_THREADS = 5


class MockPipeLine(GenericPipeLine):
    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Mock PipeLine")
        self.time_profile = 0
        self.state_sequence = [WAITING, WAITING, WAITING,
                               START,
                               RUNNING, RUNNING, RUNNING, RUNNING, RUNNING,
                               DONE]

    def extract(self, args):
        query = "test_query_" + str(args[0])
        bucket = args[1]
        print("Extraction Starts:...")
        bucket.append(query)
        for i in range(EXTRACTION_TIME):
            self.update_state(self.state_sequence[i])
            sleep(1)
        print("... done!")
        bucket.append(query)
        print("Extracted Query: ", query)
        return query


class MockPipeLineFactory(PipeLineFactory):

    def create_pipeline(self, connectionHelper):
        self.pipeline = MockPipeLine(connectionHelper)


def thread_func(factory, name, bucket):
    print(name)
    factory.doJob([name, bucket], "test")
    print("bye")


def thread_func_union(union_pip, query):
    print(query)
    u_Q = union_pip.doJob(query)
    print(u_Q)
    print("bye")


def monitor_func(factory, timeout, observed):
    for i in range(timeout):
        print("Monitor: ", factory.get_pipeline_state())
        observed.append(factory.get_pipeline_state())
        sleep(1)


def monitor_func_union(factory, token, observed):
    while True:
        state = factory.get_pipeline_state(token)
        print("Monitor: ", state)
        if state not in observed:
            observed.append(state)
        sleep(1)
        if state in [DONE, WRONG]:
            break


class MockRequest:
    session = {
        'hq': "select o_orderkey as key from customer, orders where c_custkey = o_custkey "
              "and o_totalprice <= 890 limit 3;",
        'token': '',
        'partials': ''}


class MyTestCase(unittest.TestCase):
    timeout = EXTRACTION_TIME * NUM_THREADS + 1
    connHelper = PostgresConnectionHelper(Config())

    def test_union_pipeline_state(self):
        self.connHelper.config.detect_union = True
        factory = PipeLineFactory()
        key = 'Q3'
        query = queries.queries_dict[key]
        '''
        query = "(SELECT p_partkey, p_name FROM part, partsupp where p_partkey = ps_partkey and ps_availqty > 100) " \
                "UNION ALL (SELECT s_suppkey, s_name FROM supplier, partsupp where s_suppkey = ps_suppkey " \
                "and ps_availqty > 200);"
        '''
        token = factory.doJobAsync(query, self.connHelper)
        observed = []
        monitor_func_union(factory, token, observed)
        self.assertTrue(len(observed))
        print(observed)
        self.assertTrue(DB_MINIMIZATION + RUNNING in observed)

    def test_state_changes(self):
        req = MockRequest()
        token = views.func_start(self.connHelper, MockRequest.session['hq'], req)
        done = (req.session[str(token) + 'partials'] == DONE or req.session[str(token) + 'partials'] == WRONG)
        while not done:
            sleep(1)
            views.func_check_progress(req, token)
            done = (req.session[str(token) + 'partials'] == DONE or req.session[str(token) + 'partials'] == WRONG)
        self.assertTrue(True)

    def rate_limited_to_1(self):
        factory = MockPipeLineFactory()

        bucket = []
        for i in range(NUM_THREADS):
            t = threading.Thread(target=thread_func, args=(factory, i, bucket,))
            t.start()

        observed = []
        m = threading.Thread(target=monitor_func, args=(factory, self.timeout - 1, observed,))
        m.start()

        sleep(self.timeout)
        self.assertEqual(len(bucket), EXTRACTION_TIME)
        for i in range(0, len(bucket), 2):
            if bucket[i] != bucket[i + 1]:
                self.assertTrue(False)
            else:
                print(bucket[i])
        self.assertEqual(len(observed), NUM_THREADS * len(factory.pipeline.state_sequence))

        print(observed)

        for i in range(len(observed)):
            # print(i, i % NUM_THREADS)
            self.assertEqual(observed[i], factory.pipeline.state_sequence[i % len(factory.pipeline.state_sequence)])


if __name__ == '__main__':
    unittest.main()
