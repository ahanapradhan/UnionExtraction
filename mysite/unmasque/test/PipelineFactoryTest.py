import threading
import unittest
from time import sleep

from mysite.unmasque.src.pipeline.PipeLineFactory import PipeLineFactory
from mysite.unmasque.src.pipeline.abstract.generic_pipeline import GenericPipeLine


class MockPipeLine(GenericPipeLine):
    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Mock PipeLine")
        self.time_profile = 0

    def extract(self, query):
        print("Extraction Starts:...")
        for i in range(10):
            print(i)
            sleep(1)
        print("... done!")
        print("Extracted Query: ", query)


class MockPipeLineFactory(PipeLineFactory):

    def get_element(self, connectionHelper):
        return MockPipeLine(connectionHelper)


def thread_func(name):
    print(name)
    factory = MockPipeLineFactory()
    factory.doJob("test_query_" + str(name), "test")
    print("bye")


class MyTestCase(unittest.TestCase):

    def test_rate_limited_to_1(self):
        for i in range(5):
            t = threading.Thread(target=thread_func, args=(i,))
            t.start()
        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()
