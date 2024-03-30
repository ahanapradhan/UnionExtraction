import re
from typing import Literal


class TpchSanitizer:

    def __init__(self, connectionHelper):
        self.all_relations = []
        self.connectionHelper = connectionHelper

    def set_all_relations(self, relations):
        self.all_relations.extend(relations)

    def select_query(self, projection_strs, predicate_strs):
        selections = " and ".join(projection_strs)
        wheres = " and ".join(predicate_strs)
        if len(predicate_strs) == 1:
            wheres = " and " + wheres
        query = f"Select {selections}  From information_schema.tables " + \
                f"WHERE table_schema = '{self.connectionHelper.config.schema}' and " \
                f"TABLE_CATALOG= '{self.connectionHelper.config.dbname}' {wheres} ;"
        query = re.sub(' +', ' ', query)
        return query

    def is_view_or_table(self, table_or_view_name: str) -> Literal['view', 'table']:
        # Reference: https://www.postgresql.org/docs/current/infoschema-tables.html
        check_query = self.select_query(["table_type"], [f" table_name = '{table_or_view_name}'"])
        # check_query = "select table_type " + self.from_where_catalog() + f" and table_name = '{table_or_view_name}'"
        res, _ = self.connectionHelper.execute_sql_fetchall(check_query)

        if len(res) > 0:
            if res[0][0] == 'VIEW':
                return 'view'
            else:
                return 'table'

    def begin_transaction(self):
        self.connectionHelper.execute_sql(["BEGIN;"])

    def commit_transaction(self):
        self.connectionHelper.execute_sql(["COMMIT;"])

    def sanitize(self):
        res, desc = self.connectionHelper.execute_sql_fetchall(self.select_query(["count(*)"], []))

        # if res[0][0] > len(self.all_relations):
        #    print("Database needs to be restored!")

        self.begin_transaction()
        res, desc = self.connectionHelper.execute_sql_fetchall(
            self.select_query(["SPLIT_PART(table_name, '_', 1) as original_name"],
                              ["table_name like '%_restore'"]))
        for row in res:
            table = row[0]
            self.drop_derived_relations(table)

            drop_fn = self.get_drop_fn(table)
            restore_name = self.connectionHelper.queries.get_restore_name(table)
            self.connectionHelper.execute_sql([[drop_fn, table],
                                               ["alter_table_rename_to", restore_name, table]])

        self.connectionHelper.execute_sql([["drop_table", "temp"],
                                           ["drop_view", "r_e"], ["drop_table", "r_h"]])
        self.commit_transaction()

    def get_drop_fn(self, table):
        return "drop_table_cascade" if self.is_view_or_table(table) == 'table' else "drop_view"

    def drop_derived_relations(self, table):
        derived_objects = [self.connectionHelper.queries.get_tabname_1(table),
                           self.connectionHelper.queries.get_tabname_4(table),
                           self.connectionHelper.queries.get_tabname_un(table),
                           self.connectionHelper.queries.get_tabname_nep(table),
                           table + "2",
                           table + "3"]
        drop_fns = [self.get_drop_fn(tab) for tab in derived_objects]
        for n in range(len(derived_objects)):
            drop_object = derived_objects[n]
            drop_command = drop_fns[n]
            self.connectionHelper.execute_sql([[drop_command, drop_object]])
