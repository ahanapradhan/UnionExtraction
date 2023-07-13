from itertools import combinations


def get_combs(elems):
    powerset = lambda s: [set(combo) for r in range(len(s) + 1) for combo in combinations(s, r)]
    pw_elems = powerset(elems)
    pw_elems.remove(set())
    pw_elems.remove(elems)
    combs = set()
    for e in pw_elems:
        combs.add(frozenset(e))
    #combs = set(pw_elems)
    print(combs)
    return combs
