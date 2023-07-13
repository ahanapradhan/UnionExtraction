import unittest

from db_actions import DbParser


class MyTestCase(unittest.TestCase):
    def test_for2_single(self):
        parser = DbParser()
        query = "(select * from part) union all (select * from customer)"
        parser.parse(query)
        parser.print()
        self.assertEqual(parser.fromtab_QH, {'part', 'customer'})  # add assertion here
        self.assertEqual(parser.comtab_QH, set())  # add assertion here
        self.assertEqual(parser.parttab_QH, {'part', 'customer'})  # add assertion here
        self.assertEqual(parser.fromtabs_qi, parser.parttabs_qi)  # add assertion here

    def test_for2_joins(self):
        parser = DbParser()
        query = "(select * from part, orders ) union all (select * from customer, orders )"
        parser.parse(query)
        parser.print()
        self.assertEqual(parser.fromtab_QH, {'part', 'customer', 'orders'})  # add assertion here
        self.assertEqual(parser.comtab_QH, {'orders'})  # add assertion here
        self.assertEqual(parser.parttab_QH, {'part', 'customer'})  # add assertion here
        self.assertEqual(parser.fromtabs_qi[0], {'part', 'orders'})  # add assertion here
        self.assertEqual(parser.fromtabs_qi[1], {'customer', 'orders'})  # add assertion here
        self.assertEqual(parser.parttabs_qi[0], {'part'})  # add assertion here
        self.assertEqual(parser.parttabs_qi[1], {'customer'})  # add assertion here


if __name__ == '__main__':
    unittest.main()
