import copy
import datetime
import itertools
import math

from mysite.unmasque import constants
from mysite.unmasque.constants import dummy_int, dummy_date, dummy_char


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


def get_unused_dummy_val(datatype, value_used):
    if datatype == 'int':
        dint = constants.dummy_int
    elif datatype == 'date':
        dint = constants.dummy_date
    elif datatype == 'char':
        if constants.dummy_char == 91:
            constants.dummy_char = 65
        dint = get_char(constants.dummy_char)

    while dint in value_used:
        dint = get_val_plus_delta(datatype, dint, 1)

    if datatype == 'int':
        constants.dummy_int = dint
    elif datatype == 'date':
        constants.dummy_date = dint
    elif datatype == 'char':
        constants.dummy_char = get_int(dint)

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
    if datatype == 'date':
        plus_delta = min_val + datetime.timedelta(days=delta)
    elif datatype == 'int' or datatype == 'numeric':  # INT, NUMERIC
        plus_delta = min_val + delta
    elif datatype == 'char':
        plus_delta = get_int(min_val) + delta
    return plus_delta


def get_min_and_max_val(datatype):
    if datatype == 'date':
        return constants.min_date_val, constants.max_date_val
    elif datatype == 'int':
        return constants.min_int_val, constants.max_int_val
    elif datatype == 'numeric':
        return constants.min_numeric_val, constants.max_numeric_val


def is_left_less_than_right_by_cutoff(datatype, left, right, cutoff):
    if datatype == 'date':
        yes = int((right - left).days) > cutoff
    else:
        yes = int((right - left)) > cutoff
    return yes


def get_format(datatype, val):
    if datatype == 'date':
        return "'" + str(val) + "'"
    elif datatype == 'float' or datatype == 'numeric':
        return str(round(val, 12))
    return str(val)


def get_mid_val(datatype, high, low):
    if datatype == 'date':
        mid_val = low + datetime.timedelta(days=int(math.ceil((high - low).days / 2)))
    elif datatype == 'int':
        mid_val = int((high + low) / 2)
    else:  # numeric
        mid_val = (high + low) / 2
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


def get_escape_string(att_order, attrib_list_inner):
    esc_string = '(' + '%s'
    for k in range(1, len(attrib_list_inner)):
        esc_string = esc_string + ", " + '%s'
    esc_string = esc_string + ")"
    att_order = att_order[:-1]
    att_order += ')'
    return att_order, esc_string
