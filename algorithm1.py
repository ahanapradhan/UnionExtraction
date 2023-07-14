from utils import get_combs, construct_maxNonNulls


def algo(db, QH):
    Partial_QH, MaxNonNulls, NonNulls, Nulls, Partials, S = init(db, QH)

    NonNulls = construct_nulls_nonNulls(NonNulls, Nulls, QH, S, db)

    MaxNonNulls = construct_maxNonNulls(MaxNonNulls, NonNulls)

    for c in MaxNonNulls:
        cc = Partial_QH.difference(c)
        Partials.add(frozenset(cc))

    return Partials


def construct_nulls_nonNulls(NonNulls, Nulls, QH, S, db):
    for s in S:

        to_nulls = False
        for onenull in Nulls:
            subset_check = onenull.issubset(s)
            if subset_check:
                to_nulls = True
                break
        if to_nulls:
            continue

        Res = nullify_and_runQuery(QH, db, s)
        if not Res:
            Nulls.add(s)
        else:
            NonNulls.add(s)
    return NonNulls


def nullify_and_runQuery(QH, db, s):
    db.nullify_except(s)
    Res = db.run_query(QH)
    return Res


def init(db, QH):
    partial_QH = db.get_partial_QH(QH)
    S = get_combs(partial_QH)
    Nulls = set()
    NonNulls = set()
    MaxNonNulls = set()
    Partials = set()
    return partial_QH, MaxNonNulls, NonNulls, Nulls, Partials, S
