import copy
import datetime
import itertools
import math

from dateutil.relativedelta import relativedelta

from ...src.util import constants
from ...src.util.constants import dummy_int, dummy_date, dummy_char


def count_empty_lists_in(l):
    return sum(x.count([]) for x in l)


def find_diff_idx(list1, list2):
    diffs = []
    if len(list1) == len(list2):
        for i in range(len(list1)):
            if list1[i] != list2[i]:
                diffs.append(i)
    return diffs


def isQ_result_empty(Res):
    if len(Res) <= 1:
        return True
    return False


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
        raise ValueError


def get_unused_dummy_val(datatype, value_used):
    if datatype in ['int', 'integer', 'numeric', 'float']:
        dint = constants.dummy_int
    elif datatype == 'date':
        dint = constants.dummy_date
    elif datatype in ['char', 'str']:
        if constants.dummy_char == 91:
            constants.dummy_char = 65
        dint = get_char(constants.dummy_char)
    else:
        raise ValueError

    while dint in value_used:
        dint = get_val_plus_delta(datatype, dint, 1)

    if datatype in ['int', 'integer', 'numeric', 'float']:
        constants.dummy_int = dint
    elif datatype == 'date':
        constants.dummy_date = dint
    elif datatype in ['char', 'str']:
        dint = get_char(dint)
        constants.dummy_char = get_int(dint)
    else:
        raise ValueError
    return dint


def get_datatype_from_typesList(list_type):
    if 'date' in list_type:
        datatype = 'date'
    elif 'int' in list_type:
        datatype = 'int'
    elif 'numeric' in list_type:
        datatype = 'numeric'
    else:
        datatype = 'text'
    return datatype


def get_dummy_val_for(datatype):
    if datatype == 'int' or datatype == 'numeric':
        return dummy_int
    elif datatype == 'date':
        return dummy_date
    else:
        return dummy_char


def get_val_plus_delta(datatype, min_val, delta):
    plus_delta = min_val
    try:
        if datatype == 'date':
            plus_delta = min_val + datetime.timedelta(days=delta)
        elif datatype == 'int' or datatype == 'numeric':  # INT, NUMERIC
            plus_delta = min_val + delta
        elif datatype == 'char':
            plus_delta = get_int(min_val) + delta
            # if get_char(plus_delta) >= '\\':
            #    plus_delta = get_dummy_val_for(datatype)
        return plus_delta
    except OverflowError:
        return min_val


def get_min_and_max_val(datatype):
    if datatype == 'date':
        return constants.min_date_val, constants.max_date_val
    elif datatype == 'int':
        return constants.min_int_val, constants.max_int_val
    elif datatype == 'numeric':
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
    if datatype == 'date' or datatype == 'char' \
            or datatype == 'character' \
            or datatype == 'character varying' or datatype == 'str':
        return f"\'{str(val)}\'"
    elif datatype == 'float' or datatype == 'numeric':
        return str(round(val, 12))
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


def get_mid_val(datatype, high, low, div=2):
    if datatype == 'date':
        mid_val = low + datetime.timedelta(days=int(math.floor((high - low).days / div)))
    elif datatype == 'int':
        mid_val = low + int((high - low) / div)
    else:  # numeric
        mid_val = (high + low) / div
        mid_val = round(mid_val, 3)
    return mid_val


def get_cast_value(datatype, val):
    if datatype == 'int':
        return int(val)
    elif datatype == 'float' or datatype == 'numeric':
        return float(val)
    else:  # date
        return val


def get_test_value_for(datatype, val, precision):
    if datatype == 'float' or datatype == 'numeric':
        return round(val, precision)
    elif datatype == 'int':
        return int(val)


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


def get_2_elems_sublists(l):
    comb = list(itertools.combinations(l, 2))
    comb1 = list(set(tup) for tup in comb)
    return comb1


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
