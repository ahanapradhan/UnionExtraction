import copy
import datetime

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
        plus_delta = chr(min_val + delta)
    return plus_delta


def get_min_and_max_val(datatype):
    if datatype == 'date':
        return constants.min_date_val, constants.max_date_val
    elif datatype == 'int':
        return constants.min_int_val, constants.max_int_val
    elif datatype == 'numeric':
        return constants.min_numeric_val, constants.max_numeric_val
