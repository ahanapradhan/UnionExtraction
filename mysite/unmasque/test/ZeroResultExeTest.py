import unittest

from mysite.unmasque.src.core.executables.ZeroResultExecutable import ZeroResultExecutable
from mysite.unmasque.src.util.ConnectionFactory import ConnectionHelperFactory


class MyTestCase(unittest.TestCase):
    def test_something(self):
        conn = ConnectionHelperFactory().createConnectionHelper()
        exe = ZeroResultExecutable(conn)
        result = [('col1', 'col2', 'col3'), ('0', '0', '0'), ('None', '0', '0')]
        self.assertTrue(exe.isQ_result_empty(result))
        result1 = [('col1', 'col2', 'col3'), ('0', '0', '0'), ('None', '0', '1'),
                   ('0', '0', '0'), ('0', '0', '0'), ('0', '0', '0'), ('0', '0', '0'),
                   ('0', '0', '0'), ('0', '0', '0'), ('0', '0', '0'), ('0', '0', '0'),
                   ('0', '0', '0'), ('0', '0', '0'), ('0', '0', '0'), ('0', '0', '0'),
                   ('0', '0', '0'), ('0', '0', '0'), ('0', '0', '0'), ('0', '0', '0'),
                   ('0', '0', '0'), ('0', '0', '0'), ('0', '0', '0'), ('0', '0', '0'),
                   ('0', '0', '0'), ('0', '0', '0'),
                   ('0', '0', '0'), ('0', '0', '0'), ('0', '0', '0'),
                   ('0', '0', '0'), ('0', '0', '0'), ]
        self.assertFalse(exe.isQ_result_empty(result1))


if __name__ == '__main__':
    unittest.main()
