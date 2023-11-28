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


def validate_ob(q_h, q_e):
    remark = "-"
    if q_h is None or q_e is None:
        print("Something is Wrong")
        return False, "None query"

    _, ob_h = parse_sql_query(q_h)
    _, ob_e = parse_sql_query(q_e)

    if len(ob_h) > len(ob_e):
        return False, "Some ordering attribute is missing!"
    elif len(ob_h) < len(ob_e):
        return False, "Extra ordering attributes!"
    for i in range(len(ob_h)):
        attrib_e_o, attrib_h_o, sort_order_h, sort_order_e = format_ob_attribs(i, ob_e, ob_h)
        if attrib_h_o != attrib_e_o:
            remark = f"Mismatched ordering attribute {attrib_e_o} against {attrib_h_o}"
        else:
            if sort_order_h and sort_order_e and sort_order_h != sort_order_e:
                return False, "Mismatched sort order"
            if not sort_order_h and sort_order_e == 'desc':
                return False, "Mismatched sort order"
            if not sort_order_e and sort_order_h == 'desc':
                return False, "Mismatched sort order"
    return True, remark


def format_ob_attribs(i, ob_e, ob_h):
    attrib_h = ob_h[i]
    attrib_h_o = attrib_h.split(" ")

    attrib_e = ob_e[i]
    attrib_e_o = attrib_e.split(" ")

    ob_attrib_h = attrib_h_o[0]
    ob_attrib_h = ob_attrib_h.replace(';', '')
    ob_attrib_h = ob_attrib_h.lower()

    ob_attrib_e = attrib_e_o[0]
    ob_attrib_e = ob_attrib_e.replace(';', '')
    ob_attrib_e = ob_attrib_e.lower()

    sort_order_h = extract_sort_order(attrib_h_o)
    sort_order_e = extract_sort_order(attrib_e_o)

    return ob_attrib_e, ob_attrib_h, sort_order_h, sort_order_e


def extract_sort_order(attrib_h_o):
    if len(attrib_h_o) > 1:
        sort_order_h = attrib_h_o[1]
        sort_order_h = sort_order_h.replace(';', '')
        sort_order_h = sort_order_h.lower()
    else:
        sort_order_h = False
    return sort_order_h
