import copy
import datetime
import math
from decimal import Decimal, localcontext, ROUND_DOWN
from itertools import combinations

from dateutil.relativedelta import relativedelta

from .constants import NUMERIC_TYPES, INT_TYPES, TEXT_TYPES
from ...src.util import constants
from ...src.util.constants import dummy_int, dummy_date, dummy_char, NUMBER_TYPES

from ...src.util.error_handling import UnmasqueError
from ...src.util.error_codes import ERROR_003, ERROR_004


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


def count_empty_lists_in(l):
    return sum(x.count([]) for x in l)


def find_diff_idx(list1, list2):
    diffs = []
    for sub_list1, sub_list2 in zip(list1, list2):
        if 'None' in sub_list1 or 'None' in sub_list2:
            continue
        for i, (item1, item2) in enumerate(zip(sub_list1, sub_list2)):
            if item1 != item2 and i not in diffs:
                diffs.append(i)
    return diffs


def generateCombos(val):
    res = [[0]]
    for i in range(1, val):
        new_add = []
        for elt in res:
            temp = copy.deepcopy(elt)
            temp.append(i)
            new_add.append(temp)
        for elt in new_add:
            if len(elt) < val:
                res.append(copy.deepcopy(elt))
    return copy.deepcopy(res)


def get_all_combo_lists(max_list_len):
    result = {0: [], 1: []}
    for i in range(2, max_list_len + 1):
        result[i] = generateCombos(i)
    return result


def is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def is_date(s):
    return isinstance(s, datetime.date)


def get_datatype_of_val(val):
    if is_date(val):
        return 'date'
    elif is_int(val):
        return 'int'
    elif is_number(val):
        return 'numeric'
    else:
        raise UnmasqueError(ERROR_003, "utils", f"The value is {val}")


def get_unused_dummy_val(datatype, value_used):
    if datatype in NUMBER_TYPES:
        dint = constants.dummy_int
    elif datatype == 'date':
        dint = constants.dummy_date
    elif datatype in TEXT_TYPES:
        if constants.dummy_char == 91:
            constants.dummy_char = 65
        dint = get_char(constants.dummy_char)
    else:
        raise UnmasqueError(ERROR_004, "utils", f"Problem in generating database for datatype :-{datatype}")

    while dint in value_used:
        dint = get_val_plus_delta(datatype, dint, 1)

    if datatype in NUMBER_TYPES:
        constants.dummy_int = dint
    elif datatype == 'date':
        constants.dummy_date = dint
    elif datatype in TEXT_TYPES:
        dint = get_char(dint)
        constants.dummy_char = get_int(dint)
    else:
        raise UnmasqueError(ERROR_004, "utils", f"Problem in generating database for datatype :-{datatype}")
    return dint


def get_datatype_from_typesList(list_type):
    if 'date' in list_type:
        datatype = 'date'
    elif 'int' in list_type or 'integer' in list_type:
        datatype = 'int'
    elif 'numeric' in list_type or 'decimal' in list_type or 'Decimal' in list_type or 'real' in list_type:
        datatype = 'numeric'
    else:
        datatype = 'text'
    return datatype


def get_dummy_val_for(datatype):
    if datatype in NUMBER_TYPES:
        return dummy_int
    elif datatype == 'date':
        return dummy_date
    else:
        return dummy_char


def get_val_plus_delta(datatype, min_val, delta):
    if min_val is None:
        return None
    plus_delta = min_val
    try:
        if datatype == 'date':
            plus_delta = min_val + datetime.timedelta(days=delta)
        elif datatype == 'int':
            plus_delta = min_val + delta
        elif datatype == 'numeric':
            plus_delta = float(Decimal(min_val) + Decimal(delta))
        elif datatype == 'char':
            plus_delta = get_int(min_val) + delta
        return plus_delta
    except OverflowError:
        return min_val


def get_min_and_max_val(datatype):
    if datatype == 'date':
        return constants.min_date_val, constants.max_date_val
    elif datatype in INT_TYPES:
        return constants.min_int_val, constants.max_int_val
    elif datatype in NUMERIC_TYPES:
        return constants.min_numeric_val, constants.max_numeric_val
    else:
        return constants.min_int_val, constants.max_int_val


def is_left_less_than_right_by_cutoff(datatype, left, right, cutoff):
    if datatype == 'date':
        yes = int((right - left).days) >= cutoff
    else:
        yes = (right - left) >= cutoff
    return yes


def get_format(datatype, val):
    if datatype in ['date'] + TEXT_TYPES:
        return f'\'{str(val)}\''
    elif datatype in NUMERIC_TYPES:
        val = float(val)
        return str(round(val, 3))
    return str(val)

def get_format_for_agg(datatype, val):
    if datatype in TEXT_TYPES:
        return f'\'{str(val)}\''
    elif datatype in NUMERIC_TYPES:
        val = float(val)
        return str(round(val, 3))
    return str(val)



def add_two(one, two, datatype):
    if datatype == 'date':
        year = two.year
        month = two.month
        day = two.day
        one_ = one + datetime.timedelta(days=day)
        one__ = one_ + relativedelta(months=month)
        one___ = one__ + relativedelta(years=year)
        return one___
    else:
        return one + two


def truncate(num, places):
    if not isinstance(places, int):
        return num

    with localcontext() as context:
        context.rounding = ROUND_DOWN
        exponent = Decimal(str(10 ** - places))
        value = float(Decimal(str(num)).quantize(exponent).to_eng_string())
        return value


def get_mid_val(datatype, high, low, div=2):
    if datatype == 'date':
        mid_val = low + datetime.timedelta(days=int(math.floor((high - low).days / div)))
    elif datatype == 'int':
        mid_val = low + int((high - low) / div)
    else:  # numeric
        result = Decimal(str(high)) + Decimal(str(low))
        mid_val = result / div
        mid_val = float(truncate(mid_val, 2))
    return mid_val


def get_cast_value(datatype, val):
    if datatype in INT_TYPES:
        return int(val)
    elif datatype in NUMERIC_TYPES:
        return float(val)
    else:  # date
        return val


def get_char(dchar):
    try:
        chr(dchar)
    except TypeError:
        return dchar
    return chr(dchar)


def get_int(dchar):
    try:
        ord(dchar)
    except TypeError:
        return dchar
    return ord(dchar)


def find_indices(list_to_check, item_to_find):
    return [idx for idx, value in enumerate(list_to_check) if value == item_to_find]


def get_escape_string(attrib_list_inner):
    esc_string = '(' + '%s'
    for k in range(1, len(attrib_list_inner)):
        esc_string = esc_string + ", " + '%s'
    esc_string = esc_string + ")"
    return esc_string


def get_datatype(my_list, input_tuple):
    for tpl in my_list:
        if tpl[:2] == input_tuple:
            return tpl[2]
    return 'int'  # Return None if no match is found