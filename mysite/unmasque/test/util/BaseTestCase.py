import signal
import sys
import unittest

from ...src.util.ConnectionFactory import ConnectionHelperFactory
from ...src.util.configParser import Config
from ...src.pipeline.abstract.TpchSanitizer import TpchSanitizer
from ...src.util.PostgresConnectionHelper import PostgresConnectionHelper


def signal_handler(signum, frame):
    print('You pressed Ctrl+C!')
    sigconn = PostgresConnectionHelper(Config())
    sigconn.connectUsingParams()
    sanitizer = TpchSanitizer(sigconn)
    sanitizer.sanitize()
    sigconn.closeConnection()
    print("database restored!")
    sys.exit(0)


class BaseTestCase(unittest.TestCase):
    conn = ConnectionHelperFactory().createConnectionHelper()
    sigconn = ConnectionHelperFactory().createConnectionHelper()
    sanitizer = TpchSanitizer(sigconn)

    def setUp(self):
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        self.sanitize_db()

    def sanitize_db(self):
        self.sigconn.connectUsingParams()
        self.sanitizer.sanitize()
        self.sigconn.closeConnection()

    def tearDown(self):
        self.sanitize_db()


if __name__ == '__main__':
    unittest.main()
