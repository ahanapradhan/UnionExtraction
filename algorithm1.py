from database import TPCH
from utils import get_combs


def algo(QH, partial_QH):
    db = TPCH()
    S = get_combs(partial_QH)
    Nulls = set()
    NonNulls = set()
    MaxNonNulls = set()
    Partials = set()

    for s in S:
        print(s)
        s_set = set(s)
        for onenull in Nulls:
            subset_check = onenull.issubset(s_set)
            if subset_check:
                Nulls = Nulls.union(s_set)
                continue

        db.nullify_except(s_set)
        Res = db.run_query(QH)

        if not Res:
            Nulls = Nulls.union(s_set)
        else:
            NonNulls = NonNulls.union(s_set)

    print("Nulls: ")
    print(Nulls)
    print("NonNulls: ")
    print(NonNulls)

    for one in NonNulls:
        is_subset = True
        for other in NonNulls:
            if one != other:
                is_subset = is_subset and (not one.issubset(other))
        if not is_subset:
            MaxNonNulls = MaxNonNulls.union(one)

    for c in MaxNonNulls:
        cc = partial_QH.difference(c)
        Partials = Partials.union(cc)

    return Partials

