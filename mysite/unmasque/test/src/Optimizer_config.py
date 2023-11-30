def set_optimizer_params(is_on):
    if is_on:
        option = "on"
    else:
        option = "off"

    mergejoin_option = f"SET enable_mergejoin = {option};"
    indexscan_option = f"SET enable_indexscan = {option};"
    sort_option = f"SET enable_sort = {option};"

    return [mergejoin_option, indexscan_option, sort_option]
