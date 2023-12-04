import unittest

from mysite.unmasque.src.core.factories.projection_factory import find_common_items2


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


if __name__ == '__main__':
    unittest.main()
