import unittest

from src.mocks.db_actions import DbParser


class MyTestCase(unittest.TestCase):

    def test_one(self):
        parser = DbParser()
        query = "select * from lineitem"
        parser.parse(query)
        parser.print()
        self.assertEqual(parser.fromtab_QH, {'lineitem'})  # add assertion here
        self.assertEqual(parser.comtab_QH, set())
        self.assertEqual(parser.parttab_QH, set())
        self.assertEqual(parser.fromtabs_qi, [])
        self.assertEqual(parser.parttabs_qi, [])

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
        query = "(select * from part,orders) union all (select * from customer,orders)"
        parser.parse(query)
        parser.print()
        self.assertEqual(parser.fromtab_QH, {'part', 'customer', 'orders'})  # add assertion here
        self.assertEqual(parser.comtab_QH, {'orders'})  # add assertion here
        self.assertEqual(parser.parttab_QH, {'part', 'customer'})  # add assertion here
        self.assertEqual(parser.fromtabs_qi[0], {'part', 'orders'})  # add assertion here
        self.assertEqual(parser.fromtabs_qi[1], {'customer', 'orders'})  # add assertion here
        self.assertEqual(parser.parttabs_qi[0], {'part'})  # add assertion here
        self.assertEqual(parser.parttabs_qi[1], {'customer'})  # add assertion here

    def test_for3_joins(self):
        parser = DbParser()
        query = "(select * from part,orders,nation) union all (select * from customer,orders,region) union all (" \
                "select * from nation,region)"
        parser.parse(query)
        parser.print()
        self.assertEqual(parser.fromtab_QH, {'part', 'customer', 'orders', 'nation', 'region'})  # add assertion here
        self.assertEqual(parser.comtab_QH, set())  # add assertion here
        self.assertEqual(parser.parttab_QH, {'part', 'customer', 'orders', 'nation', 'region'})  # add assertion here
        self.assertEqual(parser.parttab_QH, parser.fromtab_QH)  # add assertion here
        self.assertEqual(parser.fromtabs_qi[0], {'part', 'orders', 'nation'})  # add assertion here
        self.assertEqual(parser.fromtabs_qi[1], {'customer', 'orders', 'region'})  # add assertion here
        self.assertEqual(parser.fromtabs_qi[2], {'nation', 'region'})  # add assertion here
        self.assertEqual(parser.fromtabs_qi[0], parser.parttabs_qi[0])  # add assertion here
        self.assertEqual(parser.fromtabs_qi[1], parser.parttabs_qi[1])  # add assertion here
        self.assertEqual(parser.fromtabs_qi[2], parser.parttabs_qi[2])  # add assertion here


if __name__ == '__main__':
    unittest.main()
