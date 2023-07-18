from src.util.utils import get_combs, construct_maxNonNulls


def algo(db, QH):
    Partial_QH, MaxNonNulls, NonNulls, Nulls, Partials, S = init(db, QH)

    NonNulls = construct_nulls_nonNulls(NonNulls, Nulls, QH, S, db)

    MaxNonNulls = construct_maxNonNulls(MaxNonNulls, NonNulls)

    for c in MaxNonNulls:
        cc = Partial_QH.difference(c)
        Partials.add(frozenset(cc))

    prettY_str = construct_pretty_print_string(Partials)

    return Partials, prettY_str


def construct_pretty_print_string(Partials):
    i = 1
    prettY_str = ""
    for p in Partials:
        tabs = ""
        for t in p:
            tabs += str(t) + ", "
        tabs = tabs[:-2]
        prettY_str += "FROM(q" + str(i) + ") = { " + tabs + " }, "
        i = i + 1
    return prettY_str[:-2]


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
