import re

from mysite.unmasque.test.src.parser import parse_sql_query, extract_equality_predicates


def validate_gb(q_h, q_e):
    if q_h is None or q_e is None:
        print("Something is Wrong")
        return False
    gb_h, _ = parse_sql_query(q_h)
    gb_h_lower = [element.lower() for element in gb_h]

    gb_e, _ = parse_sql_query(q_e)
    gb_e_lower = [element.lower() for element in gb_e]

    q_h_eq = extract_equality_predicates(q_h)

    if len(gb_h_lower) != len(gb_e_lower):
        print("Grouping attributes do not match in count!")
        return False
    for gb_attrib in gb_h_lower:
        if gb_attrib not in gb_e_lower:
            if gb_attrib not in q_h_eq.keys() and gb_attrib not in q_h_eq.values():
                return False

    return True


same_hidden_ob = "same_hidden_ob"
Ob_suffix = "Ob_suffix"
count_ob = "count_ob"


def validate_ob(q_h, q_e):
    attrib_e_o = ""
    attrib_h_o = ""
    sort_order_e = ""
    sort_order_h = ""
    index_error = False

    ob_dict = {same_hidden_ob: True, Ob_suffix: False, count_ob: True}

    if q_h is None or q_e is None:
        print("Something is Wrong")
        return {same_hidden_ob: None, Ob_suffix: None, count_ob: None}

    _, ob_h = parse_sql_query(q_h)
    _, ob_e = parse_sql_query(q_e)

    missing_ob = []

    if len(ob_h) > len(ob_e):
        ob_attrib_e_0, sort_order_e_0 = format_ob_attrib(0, ob_e)
        for i in range(len(ob_h)):
            ob_attrib_h_i, sort_order_h_i = format_ob_attrib(i, ob_h)
            if ob_attrib_e_0 == ob_attrib_h_i:
                rem_h = ob_h[i:]
                ob_h = rem_h
                break
            else:
                missing_ob.append(ob_attrib_h_i)

    elif len(ob_h) < len(ob_e):
        ob_dict[Ob_suffix] = True
    for i in range(len(ob_h)):
        try:
            attrib_e_o, attrib_h_o, sort_order_h, sort_order_e = format_ob_attribs(i, ob_e, ob_h)
        except IndexError:
            index_error = True
        if attrib_h_o != attrib_e_o or index_error:
            ob_dict[same_hidden_ob] = False
        else:
            if sort_order_h and sort_order_e and sort_order_h != sort_order_e:
                ob_dict[same_hidden_ob] = False
            if not sort_order_h and sort_order_e == 'desc':
                ob_dict[same_hidden_ob] = False
            if not sort_order_e and sort_order_h == 'desc':
                ob_dict[same_hidden_ob] = False
    if not missing_ob:
        ob_dict[count_ob] = False
    for missing in missing_ob:
        pattern = fr"count\([^)]*\) as {re.escape(missing)}"
        matches = re.findall(pattern, q_h)
        if matches:
            ob_dict[count_ob] = True and ob_dict[count_ob]
    return ob_dict


def format_ob_attribs(i, ob_e, ob_h):
    print(i)
    ob_attrib_h, sort_order_h = format_ob_attrib(i, ob_h)
    ob_attrib_e, sort_order_e = format_ob_attrib(i, ob_e)
    return ob_attrib_e, ob_attrib_h, sort_order_h, sort_order_e


def format_ob_attrib(i, ob_h):
    attrib_h = ob_h[i]
    attrib_h_o = attrib_h.split(" ")
    ob_attrib_h = attrib_h_o[0]
    ob_attrib_h = ob_attrib_h.replace(';', '')
    ob_attrib_h = ob_attrib_h.lower()
    sort_order_h = extract_sort_order(attrib_h_o)
    return ob_attrib_h, sort_order_h


def extract_sort_order(attrib_h_o):
    if len(attrib_h_o) > 1:
        sort_order_h = attrib_h_o[1]
        sort_order_h = sort_order_h.replace(';', '')
        sort_order_h = sort_order_h.lower()
    else:
        sort_order_h = False
    return sort_order_h


def pretty_print(q_id, gb_correct):
    GB_CORRECT = "Gb Correct?"
    space_len = len(f"Qno                {GB_CORRECT}")
    gb_correct_len = len(GB_CORRECT)
    rem_len = space_len - gb_correct_len
    rem_len = rem_len - len(q_id)
    hspace = ""
    for i in range(rem_len):
        hspace += " "
    if gb_correct:
        correct_flag = f"{gb_correct} "
    else:
        correct_flag = f"{gb_correct}"
    return f"{q_id}{hspace}{correct_flag}"
