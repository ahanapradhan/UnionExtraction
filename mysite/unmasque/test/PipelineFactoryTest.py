import threading
import unittest
from time import sleep

from mysite.unmasque import views
from mysite.unmasque.src.pipeline.PipeLineFactory import PipeLineFactory
from mysite.unmasque.src.pipeline.abstract.generic_pipeline import GenericPipeLine
from mysite.unmasque.src.util.ConnectionHelper import ConnectionHelper
from mysite.unmasque.src.util.constants import START, WAITING, RUNNING, DONE

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


def monitor_func(factory, timeout, observed):
    for i in range(timeout):
        print("Monitor: ", factory.get_pipeline_state())
        observed.append(factory.get_pipeline_state())
        sleep(1)


class MockRequest:
    session = {
        'hq': "select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890 limit 3;",
        'token': '',
        'partials': ''}


class MyTestCase(unittest.TestCase):
    timeout = EXTRACTION_TIME * NUM_THREADS + 1
    connHelper = ConnectionHelper()

    def test_state_changes(self):
        req = MockRequest()
        views.func_start(self.connHelper, MockRequest.session['hq'], req)
        done = (req.session['partials'][2] != 'NA')
        while not done:
            sleep(70/1000)
            views.func_check_progress(req)
            done = (req.session['partials'][2] != 'NA')
        self.assertTrue(True)

    def test_rate_limited_to_1(self):
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
