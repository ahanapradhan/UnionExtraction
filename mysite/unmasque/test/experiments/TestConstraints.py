import unittest

import pytest
from constraint import *
from numpy import mean


def get_for_bounded_min(bound, num, satisfy):
    vals = []
    if satisfy:  # min < bound
        vals.append(bound - 1)
    else:  # min > bound
        vals.append(bound)
    for _ in range(num - 1):
        vals.append(bound)
    return vals


def get_for_bounded_max(bound, num, satisfy):
    vals = []
    if satisfy:  # max < bound
        vals.append(bound - 1)
    else:  # min > bound
        vals.append(bound)
    for _ in range(num - 1):
        vals.append(bound - 2)
    return vals


def get_for_bounded_sum(bound, num, satisfy):
    if satisfy:  # sum < bound
        unit = (bound - 1) / num
    else:  # sum > bound
        unit = (bound + 1) / num
    return [unit for _ in range(num)]


def get_for_bounded_avg(bound, num, satisfy):
    if satisfy:  # avg < bound
        unit = bound - 1
    else:  # avg > bound
        unit = bound + 1
    return [unit for _ in range(num)]


def check_against_funcs(bound, vals):
    _sum = sum(vals)
    _max = max(vals)
    _avg = mean(vals)
    _min = min(vals)
    funcs = [_sum, _max, _avg, _min]
    func_gt_bound = [(f > bound) for f in funcs]
    func_lt_bound = [(f < bound) for f in funcs]
    print(f"If func > {bound}:")
    print(func_gt_bound)
    print(f"If func < {bound}:")
    print(func_lt_bound)
    print("\n\n")


class MyTestCase(unittest.TestCase):
    def test_one(self):
        bound = 120000 - 1
        no_of_rows = 2

        funcs = [get_for_bounded_sum,
                 get_for_bounded_max,
                 get_for_bounded_avg,
                 get_for_bounded_min]

        for i in range(len(funcs)):
            print(funcs[i])

            _f_t_val = funcs[i](bound, no_of_rows, True)
            self.assertEqual(len(_f_t_val), no_of_rows)
            print(_f_t_val)
            check_against_funcs(bound, _f_t_val)

            _f_f_val = funcs[i](bound, no_of_rows, False)
            self.assertEqual(len(_f_f_val), no_of_rows)
            print(_f_f_val)
            check_against_funcs(bound, _f_f_val)

    @pytest.mark.skip
    def test_something(self):
        # Create a problem
        problem = Problem()

        # Add the variable X with a domain from 0 to 100
        problem.addVariable('x', range(11999))
        problem.addVariable('y', range(11999))

        # Add any other constraints you need
        # For example, let's add a constraint that X must be even
        problem.addConstraint(lambda x, y: x + y / 2 > 12000, ['x', 'y'])
        problem.addConstraint(lambda x, y: x + y > 12000, ['x', 'y'])
        problem.addConstraint(lambda x, y: max(x, y) > 12000, ['x', 'y'])
        problem.addConstraint(lambda x, y: min(x, y) < 12000, ['x', 'y'])

        # Solve the problem
        solutions = problem.getSolutions()

        # Print the solutions
        for solution in solutions:
            print(solution)
        self.assertTrue(len(solutions))


if __name__ == '__main__':
    unittest.main()
