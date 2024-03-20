import signal
import sys
import unittest

from ...src.pipeline.abstract.TpchSanitizer import TpchSanitizer
from ...src.util.ConnectionHelper import ConnectionHelper


def signal_handler(signum, frame):
    print('You pressed Ctrl+C!')
    sigconn = ConnectionHelper()
    sigconn.connectUsingParams()
    sanitizer = TpchSanitizer(sigconn)
    sanitizer.sanitize()
    sigconn.closeConnection()
    print("database restored!")
    sys.exit(0)


class BaseTestCase(unittest.TestCase):
    conn = ConnectionHelper()
    sigconn = ConnectionHelper()
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
