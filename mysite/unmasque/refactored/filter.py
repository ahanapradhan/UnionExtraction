import copy
import math

from .abstract.MutationPipeLineBase import MutationPipeLineBase
from .util.common_queries import get_tabname_4, update_sql_query_tab_attribs, form_update_query_with_value, \
    insert_into_tab_select_star_fromtab, truncate_table, update_tab_attrib_with_quoted_value, \
    select_attribs_from_relation, get_column_details_for_table
from .util.utils import isQ_result_empty, get_val_plus_delta, get_cast_value, \
    get_min_and_max_val, get_format, get_mid_val, is_left_less_than_right_by_cutoff, is_int
from ..src.util.ConnectionHelper import ConnectionHelper


def parse_for_int(val):
    try:
        v_int = int(val)
        v_int = str(val)
    except ValueError:
        v_int = f"\'{str(val)}\'"
    except TypeError:
        v_int = f"\'{str(val)}\'"
    return v_int


def round_ceil(num, places):
    adder = 5 / (10 ** (places + 1))
    return round(num + adder, places)


def round_floor(num, places):
    adder = 5 / (10 ** (places + 1))
    return round(num - adder, places)


class Filter(MutationPipeLineBase):

    def __init__(self, connectionHelper: ConnectionHelper,
                 core_relations: list[str],
                 global_min_instance_dict: dict):
        super().__init__(connectionHelper, core_relations, global_min_instance_dict, "Filter")
        # init data
        self.global_attrib_types = []
        self.global_all_attribs = []
        self.global_d_plus_value = {}  # this is the tuple from D_min
        self.global_attrib_max_length = {}

        self.global_attrib_types_dict = {}
        self.global_attrib_dict = {}

        self.filter_predicates = None

        self.mutate_dmin_with_val = None # method to be passed by aoa

    def do_init(self):
        for tabname in self.core_relations:

            res, desc = self.connectionHelper.execute_sql_fetchall(
                get_column_details_for_table(self.connectionHelper.config.schema, tabname))

            tab_attribs = []
            tab_attribs.extend(row[0] for row in res)
            self.global_all_attribs.append(copy.deepcopy(tab_attribs))

            self.global_attrib_types.extend((tabname, row[0], row[1]) for row in res)

            self.global_attrib_max_length.update(
                {(tabname, row[0]): int(str(row[2])) for row in res if is_int(str(row[2]))})

            res, desc = self.connectionHelper.execute_sql_fetchall(
                select_attribs_from_relation(tab_attribs, tabname))
            for row in res:
                for attrib, value in zip(tab_attribs, row):
                    self.global_d_plus_value[attrib] = value

    def get_datatype(self, tab_attrib: tuple[str, str]) -> str:
        if any(x in self.global_attrib_types_dict[tab_attrib] for x in ['int', 'integer']):
            return 'int'
        elif 'date' in self.global_attrib_types_dict[tab_attrib]:
            return 'date'
        elif any(x in self.global_attrib_types_dict[tab_attrib] for x in ['text', 'char', 'varbit']):
            return 'str'
        elif any(x in self.global_attrib_types_dict[tab_attrib] for x in ['numeric', 'float']):
            return 'numeric'
        else:
            raise ValueError

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        self.do_init()
        self.filter_predicates = self.get_filter_predicates(query)
        # self.logger.debug(self.filter_predicates)
        return self.filter_predicates

    def prepare_attrib_set_for_bulk_mutation(self, attrib_list):
        d_plus_value = copy.deepcopy(self.global_d_plus_value)
        attrib_max_length = copy.deepcopy(self.global_attrib_max_length)
        prepared_attrib_list = []
        for tab_attrib in attrib_list:
            tab, attrib = tab_attrib[0], tab_attrib[1]
            one_attrib = (tab, attrib, attrib_max_length, d_plus_value)
            prepared_attrib_list.append(one_attrib)
        return prepared_attrib_list

    def get_filter_predicates(self, query: str) -> list:
        filter_attribs = []
        total_attribs = 0
        d_plus_value = copy.deepcopy(self.global_d_plus_value)
        attrib_max_length = copy.deepcopy(self.global_attrib_max_length)

        for entry in self.global_attrib_types:
            # aoa change
            self.global_attrib_types_dict[(entry[0], entry[1])] = entry[2]

        for i in range(len(self.core_relations)):
            tabname = self.core_relations[i]
            attrib_list = self.global_all_attribs[i]
            total_attribs = total_attribs + len(attrib_list)
            for attrib in attrib_list:
                datatype = self.get_datatype((tabname, attrib))
                one_attrib = (tabname, attrib, attrib_max_length, d_plus_value)
                # if attrib not in self.global_key_attributes:  # filter is allowed only on non-key attribs
                self.extract_filter_on_attrib_set(filter_attribs, query, [one_attrib], datatype)

                # self.logger.debug("filter_attribs", filter_attribs)
        return filter_attribs

    def extract_filter_on_attrib_set(self, filter_attribs, query, attrib_list, datatype):
        if datatype == 'str':
            # Group mutation is not implemented for string/text/char/varchar data type
            one_attrib = attrib_list[0]
            tabname, attrib, attrib_max_length, d_plus_value = one_attrib[0], \
                one_attrib[1], one_attrib[2], one_attrib[3]
            self.handle_string_filter(attrib, attrib_max_length, d_plus_value, filter_attribs, tabname, query)
        else:
            min_val_domain, max_val_domain = get_min_and_max_val(datatype)
            self.handle_filter_for_nonTextTypes(attrib_list, datatype, filter_attribs, max_val_domain, min_val_domain,
                                                query)

    def handle_filter_for_nonTextTypes(self, attrib_list, datatype, filter_attribs,
                                       max_val_domain, min_val_domain, query):
        if datatype == 'int' or datatype == 'date':
            self.handle_point_filter(datatype, filter_attribs, query, attrib_list, min_val_domain, max_val_domain)
        elif datatype == 'numeric':
            self.handle_precision_filter(filter_attribs, query, attrib_list, min_val_domain, max_val_domain)
        else:
            raise ValueError(" Not Handled! ")

    def checkAttribValueEffect(self, query, val, attrib_list):
        prev_values = self.get_dmin_val_of_attrib_list(attrib_list)
        for tab_attrib in attrib_list:
            tabname, attrib = tab_attrib[0], tab_attrib[1]
            self.connectionHelper.execute_sql([f"update {tabname} set {attrib} = {str(val)};"])
        new_result = self.app.doJob(query)
        self.revert_filter_changes_in_tabset(attrib_list, prev_values)
        return not isQ_result_empty(new_result)

    def revert_filter_changes_in_tabset(self, attrib_list, prev_val_list):
        tab_attrib_set = set()
        for i in range(len(attrib_list)):
            tab_attrib = (attrib_list[i][0],attrib_list[i][1])
            tab_attrib_set.add(tab_attrib)
            val = prev_val_list[i]
            datatype = self.get_datatype(tab_attrib)
            self.mutate_dmin_with_val(datatype, tab_attrib, val)

    def handle_precision_filter(self, filterAttribs, query, attrib_list, min_val_domain, max_val_domain):
        # min_val_domain, max_val_domain = get_min_and_max_val(datatype)
        # NUMERIC HANDLING
        # PRECISION TO BE GET FROM SCHEMA GRAPH
        min_present = self.checkAttribValueEffect(query, min_val_domain,
                                                  attrib_list)  # True implies row was still present
        max_present = self.checkAttribValueEffect(query, max_val_domain,
                                                  attrib_list)  # True implies row was still present

        mandatory_attrib = attrib_list[0]
        tabname, attrib, attrib_max_length, d_plus_value = mandatory_attrib[0], mandatory_attrib[1], mandatory_attrib[
            2], mandatory_attrib[3]
        # inference based on flag_min and flag_max
        if not min_present and not max_present:
            equalto_flag = self.get_filter_value(query, 'int', float(d_plus_value[attrib]) - .01,
                                                 float(d_plus_value[attrib]) + .01, '=', attrib_list)
            if equalto_flag:
                filterAttribs.append(
                    (tabname, attrib, '=', float(d_plus_value[attrib]), float(d_plus_value[attrib])))
            else:
                val1 = self.get_filter_value(query, 'float', float(d_plus_value[attrib]), max_val_domain, '<=',
                                             attrib_list)
                val2 = self.get_filter_value(query, 'float', min_val_domain, math.floor(float(d_plus_value[attrib])),
                                             '>=', attrib_list)
                filterAttribs.append((tabname, attrib, 'range', float(val2), float(val1)))
        elif min_present and not max_present:
            val = self.get_filter_value(query, 'float', math.ceil(float(d_plus_value[attrib])) - 5, max_val_domain,
                                        '<=', attrib_list)
            val = float(val)
            val1 = self.get_filter_value(query, 'float', val, val + 0.99, '<=', attrib_list)
            filterAttribs.append((tabname, attrib, '<=', float(min_val_domain), float(round_floor(val1, 2))))
        elif not min_present and max_present:
            val = self.get_filter_value(query, 'float', min_val_domain, math.floor(float(d_plus_value[attrib]) + 5),
                                        '>=', attrib_list)
            val = float(val)
            val1 = self.get_filter_value(query, 'float', val - 1, val, '>=', attrib_list)
            filterAttribs.append((tabname, attrib, '>=', float(round_ceil(val1, 2)), float(max_val_domain)))

    def get_dmin_val_of_attrib_list(self, attrib_list: list) -> list:
        val_list = []
        for tab_attrib in attrib_list:
            tabname, attrib = tab_attrib[0], tab_attrib[1]
            val = self.get_dmin_val(attrib, tabname)
            val_list.append(val)
        return val_list

    def get_filter_value(self, query, datatype, min_val, max_val, operator, attrib_list):
        query_front_set = set()
        for tab_attrib in attrib_list:
            tabname, attrib = tab_attrib[0], tab_attrib[1]
            query_front = update_sql_query_tab_attribs(tabname, attrib)
            query_front_set.add(query_front)
        delta, while_cut_off = get_constants_for(datatype)

        low = min_val
        high = max_val

        if operator == '<=':
            prev_values = self.get_dmin_val_of_attrib_list(attrib_list)
            while is_left_less_than_right_by_cutoff(datatype, low, high, while_cut_off):
                mid_val, new_result = self.run_app_with_mid_val(datatype, high, low, query, query_front_set)
                if mid_val == low or high == mid_val:
                    break
                if isQ_result_empty(new_result):
                    high = mid_val
                else:
                    low = mid_val
            self.revert_filter_changes_in_tabset(attrib_list, prev_values)
            return low

        if operator == '>=':
            prev_values = self.get_dmin_val_of_attrib_list(attrib_list)
            while is_left_less_than_right_by_cutoff(datatype, low, high, while_cut_off):
                mid_val, new_result = self.run_app_with_mid_val(datatype, high, low, query, query_front_set)
                if mid_val == high or low == mid_val:
                    break
                if isQ_result_empty(new_result):
                    low = mid_val
                else:
                    high = mid_val
            self.revert_filter_changes_in_tabset(attrib_list, prev_values)
            return high

        else:  # =, i.e. datatype == 'int', date
            prev_values = self.get_dmin_val_of_attrib_list(attrib_list)
            is_low = self.run_app_for_a_val(datatype, low, query, query_front_set)
            self.revert_filter_changes_in_tabset(attrib_list, prev_values)
            is_high = self.run_app_for_a_val(datatype, high, query, query_front_set)
            self.revert_filter_changes_in_tabset(attrib_list, prev_values)
            return not is_low and not is_high

    def run_app_for_a_val(self, datatype, low, query, query_front_set):
        for query_front in query_front_set:
            low_query = form_update_query_with_value(query_front, datatype, low)
            self.connectionHelper.execute_sql([low_query])
        new_result = self.app.doJob(query)
        return not isQ_result_empty(new_result)

    def run_app_with_mid_val(self, datatype, high, low, query, query_front_set):
        mid_val = get_mid_val(datatype, high, low)
        for q_front in query_front_set:
            update_query = form_update_query_with_value(q_front, datatype, mid_val)
            self.connectionHelper.execute_sql([update_query])
        new_result = self.app.doJob(query)
        return mid_val, new_result

        # mukul

    def handle_point_filter(self, datatype, filterAttribs, query, attrib_list, min_val_domain, max_val_domain):
        # min and max domain values (initialize based on data type)
        # PLEASE CONFIRM THAT DATE FORMAT IN DATABASE IS YYYY-MM-DD
        # min_val_domain, max_val_domain = get_min_and_max_val(datatype)
        min_present = self.checkAttribValueEffect(query, get_format(datatype, min_val_domain),
                                                  attrib_list)  # True implies row
        # was still present
        max_present = self.checkAttribValueEffect(query, get_format(datatype, max_val_domain),
                                                  attrib_list)  # True implies row
        mandatory_attrib = attrib_list[0]
        tabname, attrib, attrib_max_length, d_plus_value = mandatory_attrib[0], mandatory_attrib[1], mandatory_attrib[
            2], mandatory_attrib[3]
        # inference based on flag_min and flag_max
        # was still present
        if not min_present and not max_present:
            equalto_flag = self.get_filter_value(query, datatype, get_val_plus_delta(datatype,
                                                                                     get_cast_value(datatype,
                                                                                                    d_plus_value[
                                                                                                        attrib]), -1),
                                                 get_val_plus_delta(datatype,
                                                                    get_cast_value(datatype, d_plus_value[attrib]), 1),
                                                 '=', attrib_list)
            if equalto_flag:
                filterAttribs.append((tabname, attrib, '=', d_plus_value[attrib], d_plus_value[attrib]))
            else:
                val1 = self.get_filter_value(query, datatype, get_cast_value(datatype, d_plus_value[attrib]),
                                             get_val_plus_delta(datatype,
                                                                get_cast_value(datatype, max_val_domain), -1), '<=',
                                             attrib_list)
                val2 = self.get_filter_value(query, datatype,
                                             get_val_plus_delta(datatype, get_cast_value(datatype, min_val_domain), 1),
                                             get_cast_value(datatype, d_plus_value[attrib]), '>=', attrib_list)
                filterAttribs.append((tabname, attrib, 'range', val2, val1))
        elif min_present and not max_present:
            val = self.get_filter_value(query, datatype, get_cast_value(datatype, d_plus_value[attrib]),
                                        get_val_plus_delta(datatype,
                                                           get_cast_value(datatype, max_val_domain), -1), '<=',
                                        attrib_list)
            filterAttribs.append((tabname, attrib, '<=', min_val_domain, val))
        elif not min_present and max_present:
            val = self.get_filter_value(query, datatype,
                                        get_val_plus_delta(datatype, get_cast_value(datatype, min_val_domain), 1),
                                        get_cast_value(datatype, d_plus_value[attrib]), '>=', attrib_list)
            filterAttribs.append((tabname, attrib, '>=', val, max_val_domain))

    def handle_string_filter(self, attrib, attrib_max_length, d_plus_value, filterAttribs, tabname, query):
        # STRING HANDLING
        # ESCAPE CHARACTERS IN STRING REMAINING
        if self.checkStringPredicate(query, tabname, attrib):
            # returns true if there is predicate on this string attribute
            representative = str(d_plus_value[attrib])
            max_length = 100000
            if (tabname, attrib) in attrib_max_length.keys():
                max_length = attrib_max_length[(tabname, attrib)]
            val = self.getStrFilterValue(query, tabname, attrib, representative, max_length)
            val = val.strip()
            if '%' in val or '_' in val:
                filterAttribs.append((tabname, attrib, 'LIKE', val, val))
            else:
                filterAttribs.append((tabname, attrib, 'equal', val, val))
        # update table so that result is not empty
        self.revert_filter_changes(tabname)

    def revert_filter_changes(self, tabname):
        if not self.mock:
            self.connectionHelper.execute_sql([truncate_table(tabname),
                                               insert_into_tab_select_star_fromtab(tabname, get_tabname_4(tabname))])
        else:
            values = self.global_min_instance_dict[tabname]
            headers = values[0]
            comma_sep_h = ", ".join(headers)
            tuple_ = [parse_for_int(e) for e in values[1]]
            comma_sep_v = ", ".join(tuple_)
            ddl_ql = f"insert into {tabname}({comma_sep_h}) values({comma_sep_v});"
            self.connectionHelper.execute_sql([truncate_table(tabname),
                                               ddl_ql])

    def checkStringPredicate(self, query, tabname, attrib):
        # updatequery
        if self.global_d_plus_value[attrib] is not None and self.global_d_plus_value[attrib][0] == 'a':
            val = 'b'
        else:
            val = 'a'
        new_result = self.run_updateQ_with_temp_str(attrib, query, tabname, val)
        if isQ_result_empty(new_result):
            self.revert_filter_changes(tabname)
            return True
        new_result = self.run_updateQ_with_temp_str(attrib, query, tabname, "" "")
        if isQ_result_empty(new_result):
            self.revert_filter_changes(tabname)
            return True
        return False

    def getStrFilterValue(self, query, tabname, attrib, representative, max_length):
        index = 0
        output = ""
        # currently inverted exclaimaination is being used assuming it will not be in the string
        # GET minimal string with _
        while index < len(representative):
            temp = list(representative)
            if temp[index] == 'a':
                temp[index] = 'b'
            else:
                temp[index] = 'a'
            temp = ''.join(temp)
            new_result = self.run_updateQ_with_temp_str(attrib, query, tabname, temp)
            if not isQ_result_empty(new_result):
                temp = copy.deepcopy(representative)
                temp = temp[:index] + temp[index + 1:]
                new_result = self.run_updateQ_with_temp_str(attrib, query, tabname, temp)
                if not isQ_result_empty(new_result):
                    representative = representative[:index] + representative[index + 1:]
                else:
                    output = output + "_"
                    representative = list(representative)
                    representative[index] = u"\u00A1"
                    representative = ''.join(representative)
                    index = index + 1
            else:
                output = output + representative[index]
                index = index + 1
        if output == '':
            return output
        # GET % positions
        index = 0
        representative = copy.deepcopy(output)
        if len(representative) < max_length:
            output = ""
            while index < len(representative):
                temp = list(representative)
                if temp[index] == 'a':
                    temp.insert(index, 'b')
                else:
                    temp.insert(index, 'a')
                temp = ''.join(temp)
                new_result = self.run_updateQ_with_temp_str(attrib, query, tabname, temp)
                if not isQ_result_empty(new_result):
                    output = output + '%'
                output = output + representative[index]
                index = index + 1
            temp = list(representative)
            if temp[index - 1] == 'a':
                temp.append('b')
            else:
                temp.append('a')
            temp = ''.join(temp)
            new_result = self.run_updateQ_with_temp_str(attrib, query, tabname, temp)
            if not isQ_result_empty(new_result):
                output = output + '%'
        return output

    def run_updateQ_with_temp_str(self, attrib, query, tabname, temp):
        up_query = update_tab_attrib_with_quoted_value(tabname, attrib, temp)
        self.connectionHelper.execute_sql([up_query])
        new_result = self.app.doJob(query)
        return new_result


def get_constants_for(datatype):
    if datatype in ('int', 'date'):
        while_cut_off = 0
        delta = 1
    elif datatype in ('float', 'numeric'):
        while_cut_off = 0.00
        delta = 0.01
    else:
        raise ValueError(f"Unsupported datatype: {datatype}")
    return delta, while_cut_off
