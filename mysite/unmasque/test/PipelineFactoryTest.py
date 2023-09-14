import threading
import unittest
from time import sleep

from mysite.unmasque.src.pipeline.PipeLineFactory import PipeLineFactory
from mysite.unmasque.src.pipeline.abstract.generic_pipeline import GenericPipeLine

EXTRACTION_TIME = 10
NUM_THREADS = 5


class MockPipeLine(GenericPipeLine):
    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Mock PipeLine")
        self.time_profile = 0

    def extract(self, args):
        query = "test_query_" + str(args[0])
        bucket = args[1]
        print("Extraction Starts:...")
        bucket.append(query)
        for i in range(EXTRACTION_TIME):
            print(i)
            sleep(1)
        print("... done!")
        bucket.append(query)
        print("Extracted Query: ", query)


class MockPipeLineFactory(PipeLineFactory):

    def get_element(self, connectionHelper):
        return MockPipeLine(connectionHelper)


def thread_func(name, bucket):
    print(name)
    factory = MockPipeLineFactory()
    factory.doJob([name, bucket], "test")
    print("bye")


class MyTestCase(unittest.TestCase):

    def test_rate_limited_to_1(self):
        bucket = []
        for i in range(NUM_THREADS):
            t = threading.Thread(target=thread_func, args=(i, bucket,))
            t.start()

        sleep(EXTRACTION_TIME * NUM_THREADS + 1)
        self.assertEqual(len(bucket), EXTRACTION_TIME)
        for i in range(0, len(bucket), 2):
            if bucket[i] != bucket[i + 1]:
                self.assertTrue(False)
            else:
                print(bucket[i])
        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()
