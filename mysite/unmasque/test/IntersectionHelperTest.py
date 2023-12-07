import unittest

from mysite.unmasque.src.core.factories.projection_factory import find_common_items2
from mysite.unmasque.src.core.n_minimizer import get_combinations


class MyTestCase(unittest.TestCase):
    def test_something(self):
        li = [[('a', 'b', '=', 6, 6), ('c', 'd', '=', 6, 6)],
              [('a', 'x', '=', 5, 5), ('c', 'd', '=', 6, 6)],
              [('c', 'd', '=', 5, 5), ('a', 'b', '=', 4, 4), ('c', 'd', '=', 6, 6)]]
        result_dict = find_common_items2(li)
        print(result_dict)
        val_6 = result_dict[6]
        self.assertEqual(len(val_6), 3)
        self.assertEqual(val_6[0], [('a', 'b', '=', 6, 6), ('c', 'd', '=', 6, 6)])
        self.assertEqual(val_6[1], [('c', 'd', '=', 6, 6)])
        self.assertEqual(val_6[2], [('c', 'd', '=', 6, 6)])
        self.assertEqual(len(result_dict), 1)

    def test_something1(self):
        li = [[('a', 'b', '=', 6, 6), ('c', 'd', '=', 6, 6), ('a', 'c', '=', 5, 5)],
              [('a', 'x', '=', 5, 5), ('c', 'd', '=', 6, 6)],
              [('c', 'd', '=', 5, 5), ('a', 'b', '=', 4, 4), ('c', 'd', '=', 6, 6)]]
        result_dict = find_common_items2(li)
        print(result_dict)
        self.assertEqual(len(result_dict), 2)
        val_6 = result_dict[6]
        self.assertEqual(len(val_6), 3)
        self.assertEqual(val_6[0], [('a', 'b', '=', 6, 6), ('c', 'd', '=', 6, 6)])
        self.assertEqual(val_6[1], [('c', 'd', '=', 6, 6)])
        self.assertEqual(val_6[2], [('c', 'd', '=', 6, 6)])
        val_5 = result_dict[5]
        self.assertEqual(len(val_5), 3)
        self.assertEqual(val_5[0], [('a', 'c', '=', 5, 5)])
        self.assertEqual(val_5[1], [('a', 'x', '=', 5, 5)])
        self.assertEqual(val_5[2], [('c', 'd', '=', 5, 5)])

    def test_from_query(self):
        li = [[('customer', 'c_mktsegment', 'equal', 'AUTOMOBILE', 'AUTOMOBILE'),
               ('nation', 'n_name', 'equal', 'ARGENTINA', 'ARGENTINA')],
              [('customer', 'c_mktsegment', 'equal', 'AUTOMOBILE', 'AUTOMOBILE'),
               ('nation', 'n_name', 'equal', 'BRAZIL', 'BRAZIL')]]
        result_dict = find_common_items2(li)
        print(result_dict)
        self.assertEqual(len(result_dict), 1)
        val_AUTOMOBILE = result_dict['AUTOMOBILE']
        self.assertEqual(len(val_AUTOMOBILE), 2)

    def test_from_query1(self):
        li = [[('customer', 'c_mktsegment', 'equal', 'AUTOMOBILE', 'AUTOMOBILE'),
               ('nation', 'n_name', 'equal', 'BRAZIL', 'BRAZIL')],
              [('customer', 'c_mktsegment', 'equal', 'AUTOMOBILE', 'AUTOMOBILE'),
               ('nation', 'n_name', 'equal', 'ARGENTINA', 'ARGENTINA')]]
        result_dict = find_common_items2(li)
        print(result_dict)
        self.assertEqual(len(result_dict), 1)
        val_AUTOMOBILE = result_dict['AUTOMOBILE']
        self.assertEqual(len(val_AUTOMOBILE), 2)

    def test_combinations(self):
        two_l = [0, 1, 2]
        res_two_l = get_combinations(two_l, 2)
        print(res_two_l)
        self.assertEqual(len(res_two_l), 3)
        self.assertTrue([0, 1] in res_two_l)
        self.assertTrue([0, 2] in res_two_l)
        self.assertTrue([1, 2] in res_two_l)

        three_l = [1, 2, 3, 4]
        res_three_l = get_combinations(three_l, 3)
        print(res_three_l)
        self.assertEqual(len(res_three_l), 4)
        self.assertTrue([1, 2, 3] in res_three_l)
        self.assertTrue([1, 2, 4] in res_three_l)
        self.assertTrue([2, 3, 4] in res_three_l)
        self.assertTrue([1, 3, 4] in res_three_l)

    def test_combination1(self):
        one_l = [0, 1]
        res_one_l = get_combinations(one_l, 1)
        print(one_l)
        self.assertEqual(len(res_one_l), 2)
        self.assertEqual([[0], [1]], res_one_l)


if __name__ == '__main__':
    unittest.main()
