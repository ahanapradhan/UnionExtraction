import unittest

from mysite.unmasque.src.util.configParser import Config


class MyTestCase(unittest.TestCase):
    def test_default(self):
        config = Config()
        self.assertEqual(config.dbname, "tpch")  # add assertion here
        self.assertEqual(config.user, "postgres")  # add assertion here
        self.assertEqual(config.password, "postgres")  # add assertion here
        self.assertEqual(config.host, "localhost")  # add assertion here
        self.assertEqual(config.port, "5432")  # add assertion here
        self.assertEqual(config.schema, "public")  # add assertion here
        self.assertEqual(config.log_level, "INFO")

        config.parse_config()
        self.assertEqual(config.dbname, "tpch")  # add assertion here
        self.assertEqual(config.user, "postgres")  # add assertion here
        self.assertEqual(config.password, "postgres")  # add assertion here
        self.assertEqual(config.host, "localhost")  # add assertion here
        self.assertEqual(config.port, "5432")  # add assertion here
        self.assertEqual(config.schema, "public")  # add assertion here
        self.assertEqual(config.log_level, "DEBUG")


if __name__ == '__main__':
    unittest.main()
