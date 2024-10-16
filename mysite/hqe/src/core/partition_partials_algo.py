import sys
import time

sys.path.append(".")
from ..util.utils import get_combs, construct_maxNonNulls


def algo(db, QH):
    Partial_QH, MaxNonNulls, NonNulls, Nulls, Partials, S = init(db, QH)

    if len(Partial_QH) == 0:
        return {frozenset(db.fromtabs)}, construct_pretty_print_string({frozenset(db.fromtabs)}), (0, db.app_calls)

    local_start_time = time.time()
    local_start_capp_calls = db.app_calls

    NonNulls = construct_nulls_nonNulls(NonNulls, Nulls, QH, S, db)

    MaxNonNulls = construct_maxNonNulls(MaxNonNulls, NonNulls)

    comtabs = db.comtabs

    for c in MaxNonNulls:
        cc = Partial_QH.difference(c)
        for ct in comtabs:
            cc.add(ct)
        Partials.add(frozenset(cc))

    prettY_str = construct_pretty_print_string(Partials)

    local_end_time = time.time()
    local_end_capp_calls = db.app_calls

    return Partials, prettY_str, (local_end_time - local_start_time, local_end_capp_calls - local_start_capp_calls)


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
        if db.isEmpty(Res):
            Nulls.add(s)
        else:
            NonNulls.add(s)
    return NonNulls


def nullify_and_runQuery(QH, db, s):
    db.nullify_except(s)
    Res = db.run_query(QH)
    db.revert_nullify()
    return Res


def init(db, QH):
    partial_QH = db.get_partial_QH(QH)
    S = set()
    if len(partial_QH) > 0:
        S = get_combs(partial_QH)
    Nulls = set()
    NonNulls = set()
    MaxNonNulls = set()
    Partials = set()
    return partial_QH, MaxNonNulls, NonNulls, Nulls, Partials, S
