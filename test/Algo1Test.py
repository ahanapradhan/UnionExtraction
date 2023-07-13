import unittest

import algorithm1
from database import TPCH


class MyTestCase(unittest.TestCase):
    def test_init(self):
        query = "(select * from part) union all (select * from customer)"
        db = TPCH()
        Partial_QH, MaxNonNulls, NonNulls, Nulls, Partials, S = algorithm1.init(db, query)
        self.assertEqual(Partial_QH, {'part', 'customer'})  # add assertion here
        self.assertEqual(MaxNonNulls, set())
        self.assertEqual(NonNulls, set())
        self.assertEqual(Nulls, set())
        self.assertEqual(Partials, set())
        self.assertEqual(S, {frozenset({'part'}), frozenset({'customer'})})

    def test_init1(self):
        query = "(select * from part,orders) union all (select * from customer)"
        db = TPCH()
        Partial_QH, MaxNonNulls, NonNulls, Nulls, Partials, S = algorithm1.init(db, query)
        self.assertEqual(Partial_QH, {'part', 'customer', 'orders'})  # add assertion here
        self.assertEqual(MaxNonNulls, set())
        self.assertEqual(NonNulls, set())
        self.assertEqual(Nulls, set())
        self.assertEqual(Partials, set())
        self.assertEqual(S, {frozenset({'part'}), frozenset({'customer'}), frozenset({'orders'}),
                             frozenset({'part', 'orders'}), frozenset({'part', 'customer'}),
                             frozenset({'customer', 'orders'})})


if __name__ == '__main__':
    unittest.main()
