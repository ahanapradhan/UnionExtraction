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

    def test_nullify_and_runQuery(self):
        query = "(select * from part,orders) union all (select * from customer)"
        db = TPCH()
        check = algorithm1.nullify_and_runQuery(query, db, {'part', 'orders'})
        self.assertEqual(check, True)

    def test_nullify_and_runQuery2(self):
        query = "(select * from part,orders) union all (select * from customer)"
        db = TPCH()
        check = algorithm1.nullify_and_runQuery(query, db, {'part'})
        self.assertEqual(check, True)

    def test_nullify_and_runQuery3(self):
        query = "(select * from part,orders) union all (select * from customer)"
        db = TPCH()
        check = algorithm1.nullify_and_runQuery(query, db, {'part', 'orders', 'customer'})
        self.assertEqual(check, False)

    def test_nullify_and_runQuery1(self):
        query = "(select * from part,orders) union all (select * from customer)"
        db = TPCH()
        check = algorithm1.nullify_and_runQuery(query, db, {'customer'})
        self.assertEqual(check, True)

    def test_construct_nulls_NonNulls(self):
        query = "(select * from part,orders) union all (select * from orders,customer) union all (select * from " \
                "customer,part)"
        db = TPCH()
        Partial_QH, MaxNonNulls, NonNulls, Nulls, Partials, S = algorithm1.init(db, query)
        self.assertEqual(S, {frozenset({'part'}), frozenset({'customer'}), frozenset({'orders'}),
                             frozenset({'part', 'orders'}), frozenset({'part', 'customer'}),
                             frozenset({'customer', 'orders'})})
        NonNulls = algorithm1.construct_nulls_nonNulls(NonNulls, Nulls, query, S, db)
        self.assertEqual(NonNulls, {frozenset({'part'}), frozenset({'customer'}), frozenset({'orders'})})

    def test_construct_nulls_NonNulls1(self):
        query = "(select * from part,orders,nation) union all (select * from customer,orders,nation) union all (" \
                "select * from " \
                "lineitem,orders,nation)"
        db = TPCH()
        Partial_QH, MaxNonNulls, NonNulls, Nulls, Partials, S = algorithm1.init(db, query)
        self.assertEqual(S, {frozenset({'part'}), frozenset({'customer'}), frozenset({'lineitem'}),
                             frozenset({'part', 'customer'}), frozenset({'part', 'lineitem'}),
                             frozenset({'customer', 'lineitem'})})
        NonNulls = algorithm1.construct_nulls_nonNulls(NonNulls, Nulls, query, S, db)
        self.assertEqual(NonNulls, S)

    def test_construct_nulls_NonNulls2(self):
        query = "(select * from part,orders) union all (select * from orders,customer,nation)"
        db = TPCH()
        Partial_QH, MaxNonNulls, NonNulls, Nulls, Partials, S = algorithm1.init(db, query)
        NonNulls = algorithm1.construct_nulls_nonNulls(NonNulls, Nulls, query, S, db)
        self.assertEqual(NonNulls, {frozenset({'part'}), frozenset({'customer'}), frozenset({'nation'}),
                                    frozenset({'customer', 'nation'})})


if __name__ == '__main__':
    unittest.main()
