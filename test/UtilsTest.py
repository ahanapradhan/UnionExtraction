import unittest

import utils


class MyTestCase(unittest.TestCase):
    def test_pairing(self):
        elems = {1, 2, 3}
        res = utils.get_pairs_from_set(elems)
        self.assertEqual(res, {frozenset({1, 3}), frozenset({2, 3}), frozenset({1, 2})})

    def test_pairing_for_maxNonNull_construction(self):
        one = frozenset({'A', 'B'})
        two = frozenset({'B', 'C', 'E'})
        three = frozenset({'C', 'D'})
        nonNulls = {one, two, three}
        res = utils.get_pairs_from_set(nonNulls)
        self.assertEqual(res, {frozenset({one, three}), frozenset({two, three}), frozenset({one, two})})

    def test_construct_maxNonNulls(self):
        s = frozenset({'S'})
        w = frozenset({'W'})
        c = frozenset({'C'})
        sw = frozenset({'S', 'W'})
        wc = frozenset({'W', 'C'})
        sc = frozenset({'S', 'C'})
        nonNulls = {s, w, c, sw, wc, sc}
        maxnonNulls = set()
        maxnonNulls = utils.construct_maxNonNulls(maxnonNulls, nonNulls)
        self.assertEqual(maxnonNulls, {sw, wc, sc})

    def test_construct_maxNonNulls1(self):
        a = frozenset({'A'})
        d = frozenset({'D'})
        c = frozenset({'C'})
        cd = frozenset({'C', 'D'})
        nonNulls = {a, c, d, cd}
        maxnonNulls = set()
        maxnonNulls = utils.construct_maxNonNulls(maxnonNulls, nonNulls)
        self.assertEqual(maxnonNulls, {a, cd})

    def test_construct_maxNonNulls2(self):
        a = frozenset({'A'})
        b = frozenset({'B'})
        c = frozenset({'C'})
        d = frozenset({'D'})
        ab = frozenset({'A', 'B'})
        cd = frozenset({'C', 'D'})
        ad = frozenset({'A', 'D'})

        nonNulls = {a, b, c, d, ab, cd, ad}
        maxnonNulls = set()
        maxnonNulls = utils.construct_maxNonNulls(maxnonNulls, nonNulls)
        self.assertEqual(maxnonNulls, {ab, ad, cd})


if __name__ == '__main__':
    unittest.main()
