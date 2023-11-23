from mysite.unmasque.test.src.parser import parse_sql_query


def validate_gb(q_h, q_e):
    if q_h is None or q_e is None:
        print("Something is Wrong")
        return False
    gb_h, _ = parse_sql_query(q_h)
    gb_e, _ = parse_sql_query(q_e)
    if frozenset(gb_h) != frozenset(gb_e):
        print("Grouping attributes do not match!")
        return False
    return True


def validate_ob(q_h, q_e):
    if q_h is None or q_e is None:
        print("Something is Wrong")
        return False

    _, ob_h = parse_sql_query(q_h)
    _, ob_e = parse_sql_query(q_e)

    if len(ob_h) > len(ob_e):
        print("Some ordering attribute is missing!")
        return False
    elif len(ob_h) < len(ob_e):
        print("Extra ordering attributes!")
        return False
    for i in range(len(ob_h)):
        attrib_h = ob_h[i]
        attrib_h_o = attrib_h.split(" ")
        attrib_e = ob_e[i]
        attrib_e_o = attrib_e.split(" ")
        if attrib_h_o[0] != attrib_e_o[0]:
            print(f"Mismatched ordering attribute {ob_e} against {ob_h}")
        elif attrib_h_o[0] == attrib_e_o[0]:
            len_h = len(attrib_h_o)
            len_e = len(attrib_e_o)
            if len_h > 1 and attrib_h_o[1] != attrib_e_o[1]:
                print("Mismatched sort order")
                return False
    return True
