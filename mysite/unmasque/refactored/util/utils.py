import copy


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


def get_min_max_vals(datatype):
    if datatype == "numeric":
        return -214748364888, 214748364788
    elif datatype == "int":
        return -2147483648, 2147483647


