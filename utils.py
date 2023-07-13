from itertools import combinations


def get_combs(elems):
    powerset = lambda s: [set(combo) for r in range(len(s) + 1) for combo in combinations(s, r)]
    pw_elems = powerset(elems)
    pw_elems.remove(set())
    pw_elems.remove(elems)
    print(pw_elems)
    return pw_elems
