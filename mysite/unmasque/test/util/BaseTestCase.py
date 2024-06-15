import ast
import signal
import sys
import unittest
from _decimal import Decimal

from ...src.core.abstract.abstractConnection import AbstractConnectionHelper
from ...src.util import constants
from ...src.util.utils import get_unused_dummy_val, get_format, get_char
from ...test.util.TPCH_backup_restore import TPCHRestore
from ...src.util.ConnectionFactory import ConnectionHelperFactory
from ...src.util.PostgresConnectionHelper import PostgresConnectionHelper
from ...src.util.configParser import Config


def signal_handler(signum, frame):
    print('You pressed Ctrl+C!')
    sigconn = PostgresConnectionHelper(Config())
    sanitizer = TPCHRestore(sigconn)
    sanitizer.doJob()
    print("database restored!")
    sys.exit(0)


def construct_values_used(global_filter_predicates, attrib_types_dict):
    vu = []
    # Identifying projected attributs with no filter
    for pred in global_filter_predicates:
        vu.append(pred[1])
        if 'char' in attrib_types_dict[(pred[0], pred[1])] or 'text' in attrib_types_dict[
            (pred[0], pred[1])]:
            vu.append(pred[3].replace('%', ''))
        else:
            vu.append(pred[3])
    return vu


def construct_values_for_attribs(conn: AbstractConnectionHelper, value_used, global_join_graph, core_relations,
                                 global_all_attribs,
                                 attrib_types_dict):
    for elt in global_join_graph:
        dummy_int = get_unused_dummy_val('int', value_used)
        for val in elt:
            value_used.append(val)
            value_used.append(dummy_int)
    for i in range(len(core_relations)):
        tabname = core_relations[i]
        attrib_list = global_all_attribs[i]
        insert_values = []
        att_order = '('
        for attrib in attrib_list:
            att_order = att_order + attrib + ","
            if attrib in value_used:
                if 'int' in attrib_types_dict[(tabname, attrib)] \
                        or \
                        'numeric' in attrib_types_dict[(tabname, attrib)]:
                    insert_values.append(value_used[value_used.index(attrib) + 1])
                elif 'date' in attrib_types_dict[(tabname, attrib)]:
                    date_val = value_used[value_used.index(attrib) + 1]
                    date_insert = get_format('date', date_val)
                    insert_values.append(ast.literal_eval(date_insert))
                else:
                    insert_values.append(str(value_used[value_used.index(attrib) + 1]))

            else:
                value_used.append(attrib)
                if 'int' in attrib_types_dict[(tabname, attrib)] \
                        or \
                        'numeric' in attrib_types_dict[(tabname, attrib)]:
                    dummy_int = get_unused_dummy_val('int', value_used)
                    insert_values.append(dummy_int)
                    value_used.append(dummy_int)
                elif 'date' in attrib_types_dict[(tabname, attrib)]:
                    dummy_date = get_unused_dummy_val('date', value_used)
                    val = ast.literal_eval(get_format('date', dummy_date))
                    insert_values.append(val)
                    value_used.append(val)
                elif 'boolean' in attrib_types_dict[(tabname, attrib)]:
                    insert_values.append(constants.dummy_boolean)
                    value_used.append(str(constants.dummy_boolean))
                elif 'bit varying' in attrib_types_dict[(tabname, attrib)]:
                    value_used.append(attrib)
                    insert_values.append(constants.dummy_varbit)
                    value_used.append(str(constants.dummy_varbit))
                else:
                    dummy_char = get_unused_dummy_val('char', value_used)
                    dummy = get_char(dummy_char)
                    insert_values.append(dummy)
                    value_used.append(dummy)

        att_order = att_order[:-1]
        att_order += ")"

        insert_values = tuple(insert_values)
        count = len(insert_values)
        esc_string = "("
        for c in range(count):
            esc_string += "%s,"
        esc_string = esc_string[:-1]
        esc_string += ")"
        insert_query = conn.queries.insert_into_tab_attribs_format(att_order, esc_string, tabname)
        conn.execute_sql_with_params(insert_query, [insert_values])

    value_used = [str(val) for val in value_used]
    return value_used


def truncate_core_relations(from_rels, conn):
    for rel in from_rels:
        conn.execute_sql([conn.queries.truncate_table(rel)])


def create_dmin_for_test(from_rels, global_filter_predicates, attrib_types_dict, global_all_attribs,
                         global_join_graph, conn, app, global_min_instance_dict):
    truncate_core_relations(from_rels, conn)
    val_used = construct_values_used(global_filter_predicates, attrib_types_dict)
    val_used = construct_values_for_attribs(conn, val_used, global_join_graph, from_rels, global_all_attribs,
                                            attrib_types_dict)
    for tab_name in from_rels:
        al = app.doJob("select * from " + tab_name)
        global_min_instance_dict[tab_name] = [al[0], al[1]]
    print(global_min_instance_dict)


class BaseTestCase(unittest.TestCase):
    conn = ConnectionHelperFactory().createConnectionHelper()
    sigconn = ConnectionHelperFactory().createConnectionHelper()
    sanitizer = TPCHRestore(sigconn)
    global_attrib_types = []
    global_attrib_types_dict = {}
    global_min_instance_dict = {}

    def __init__(self, *args, **kwargs):
        print('BasicTest.__init__')
        super(BaseTestCase, self).__init__(*args, **kwargs)

    # @classmethod
    # def setUpClass(cls):
    #    signal.signal(signal.SIGINT, signal_handler)

    # def run(self, result=None):
    #    try:
    #        super(BaseTestCase, self).run(result)
    #    except Exception as e:
    #        # Handle the exception here
    #        print(f"Exception occurred: {e}")
    #        self.sanitizer.doJob()

    def get_dmin_val(self, attrib: str, tab: str):
        values = self.global_min_instance_dict[tab]
        attribs, vals = values[0], values[1]
        attrib_idx = attribs.index(attrib)
        val = vals[attrib_idx]
        ret_val = float(val) if isinstance(val, Decimal) else val
        return ret_val

    def do_init(self):
        for entry in self.global_attrib_types:
            # aoa change
            self.global_attrib_types_dict[(entry[0], entry[1])] = entry[2]

    def get_datatype(self, tab_attrib):
        if any(x in self.global_attrib_types_dict[tab_attrib] for x in ['int', 'integer', 'number']):
            return 'int'
        elif 'date' in self.global_attrib_types_dict[tab_attrib]:
            return 'date'
        elif any(x in self.global_attrib_types_dict[tab_attrib] for x in ['text', 'char', 'varbit']):
            return 'str'
        elif any(x in self.global_attrib_types_dict[tab_attrib] for x in ['numeric', 'float']):
            return 'numeric'
        else:
            raise ValueError

    def setUp(self):
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        self.sanitizer.doJob()

    # def tearDown(self):
    # self.sanitizer.doJob()
    #   super().tearDown()


if __name__ == '__main__':
    unittest.main()
