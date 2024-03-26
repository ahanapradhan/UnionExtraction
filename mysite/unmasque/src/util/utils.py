from itertools import combinations


def get_header_from_cursour_desc(desc):
    colnames = [d[0] for d in desc]
    new_result = [tuple(colnames)]
    t = new_result[0]
    header = '(' + ', '.join(t) + ')'
    return header

def get_combs(elems):
    powerset = lambda s: [set(combo) for r in range(len(s) + 1) for combo in combinations(s, r)]
    pw_elems = powerset(elems)
    pw_elems.remove(set())
    pw_elems.remove(elems)
    combs = set()
    for e in pw_elems:
        combs.add(frozenset(e))
    # print(combs)
    return combs


def get_pairs_from_set(elems):
    org_nl = list(elems)
    res = set()
    for i in range(len(org_nl)):
        one_rotate_nl = org_nl[i:] + org_nl[:i]
        zipped = zip(org_nl, one_rotate_nl)
        for z in zipped:
            if not (z[0] == z[1]):
                res.add(frozenset(z))
    return res


def construct_maxNonNulls(MaxNonNulls, NonNulls):
    pairs = get_pairs_from_set(NonNulls)

    to_add = set()
    to_del = set()
    dont_add = set()

    for pair in pairs:
        one, other = pair

        if one < other:
            to_add.add(other)
        if other < one:
            to_add.add(one)
        if (not one < other) and (not other < one):
            to_add.add(other)
            to_add.add(one)

        for m in MaxNonNulls:
            for ta in to_add:
                if m < ta:
                    to_del.add(m)
                if ta < m:
                    dont_add.add(ta)

        for da in dont_add:
            to_add.discard(da)
        for td in to_del:
            MaxNonNulls.discard(td)
        for ta in to_add:
            MaxNonNulls.add(ta)

    return MaxNonNulls
