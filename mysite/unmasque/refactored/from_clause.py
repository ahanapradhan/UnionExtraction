from mysite.unmasque.refactored.abstract.ExtractorBase import Base

try:
    import psycopg2
except ImportError:
    pass

import executable


class FromClause(Base):

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "FromClause")
        self.all_relations = None
        self.core_relations = None

    def init_check(self):
        try:
            res, desc = self.connectionHelper.execute_sql_fetchall(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' and TABLE_CATALOG= '" + self.connectionHelper.db + "';")
            self.connectionHelper.execute_sql("SET search_path = 'public';")
            for val in res:
                self.all_relations.append(val[0])
        except Exception as error:
            print("Can not obtain table names. Error: " + str(error))
            return False

        if not self.all_relations:
            print("No table in the selected instance. Please select another instance.")
            return False

        if 'temp' in self.all_relations:
            self.connectionHelper.execute_sql('drop table temp;')

        return True

    def get_core_relations_by_rename(self):
        if not self.init_check():
            return

        for tabname in self.all_relations:
            try:
                self.connectionHelper.execute_sql('Alter table ' + tabname + ' rename to temp;')
                # create an empty table with name x

                self.connectionHelper.execute_sql('Create table ' + tabname + ' (like temp);')

                # check the result
                new_result = executable.getExecOutput()
                if len(new_result) <= 1:
                    self.core_relations.append(tabname)
                # revert the changes

                self.connectionHelper.execute_sql('drop table ' + tabname + ';')

                self.connectionHelper.execute_sql('Alter table temp rename to ' + tabname + ';')
            except Exception as error:
                print("Error Occurred in table extraction. Error: " + str(error))
                exit(1)

    def get_core_relations_by_error(self):
        if not self.init_check():
            return

        self.connectionHelper.execute_sql("set statement_timeout to '5s'")

        for tabname in self.all_relations:
            try:
                self.connectionHelper.execute_sql('Alter table ' + tabname + ' rename to temp;')

                try:
                    new_result = executable.getExecOutput()  # slow
                    if len(new_result) <= 1:
                        self.core_relations.append(tabname)
                except psycopg2.Error as e:
                    if e.pgcode == '42P01':
                        self.core_relations.append(tabname)
                    elif e.pgcode != '57014':
                        raise

                self.connectionHelper.execute_sql('Alter table temp rename to ' + tabname + ';')
            except Exception as error:
                print("Error Occurred in table extraction. Error: " + str(error))
            # exit(1)

    def doActualJob(self, method="rename"):
        if method == "rename":
            self.get_core_relations_by_rename()
        else:
            self.get_core_relations_by_error()


def getCoreRelations(connHelper):
    fc = FromClause(connHelper)
    fc.doJob("rename")
    return fc.core_relations
