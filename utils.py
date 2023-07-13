from itertools import combinations


def get_combs(elems):
    powerset = lambda s: [set(combo) for r in range(len(s) + 1) for combo in combinations(s, r)]
    pw_elems = powerset(elems)
    pwset_elems = set(tuple(elem) for elem in pw_elems)
    pwset_elems.discard(())
    pwset_elems.discard(tuple(elems))
    print(pwset_elems)
    return pwset_elems
