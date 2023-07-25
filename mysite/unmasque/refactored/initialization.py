import csv

from mysite.unmasque.refactored.abstract.ExtractorBase import Base
from mysite.unmasque.refactored.util.common_queries import drop_table


class Initiator(Base):
    global_index_dict = {}
    global_key_lists = [[]]
    global_pk_dict = {}
    all_relations = []
    error = None
    pkfkfilename = "pkfkrelations.csv"
    create_index_filename = "create_indexes.sql"

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Initiator")

    def reset(self):
        self.global_index_dict = {}
        self.global_key_lists = [[]]
        self.global_pk_dict = {}
        self.all_relations = []
        self.error = None

    def get_all_relations(self):
        self.all_relations = set()
        try:
            res, desc = self.connectionHelper.execute_sql_fetchall(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' and TABLE_CATALOG= '" + self.connectionHelper.db + "';")
            self.connectionHelper.execute_sql(["SET search_path = 'public';"])
            for val in res:
                self.all_relations.add(val[0])
        except Exception as error:
            print("Can not obtain table names. Error: " + str(error))
            return False

        if not self.all_relations:
            print("No table in the selected instance. Please select another instance.")
            return False

        self.connectionHelper.execute_sql([drop_table("temp")])
        return True

    def verify_support_files(self):
        try:
            f1 = open(self.pkfkfilename, 'rt')
            f2 = open(self.create_index_filename, 'rt')
        except IOError as error:
            self.error = 'Unmasque Error: \n Support File Not Accessible. \n Postgres Error: \n ' + \
                         dict(error.args[0])['M']
            print('Support File Not Accessible. Error: ' + str(error))
            return False
        f1.close()
        f2.close()
        return True

    def doActualJob(self, args):
        print("inside -- initialization.initialization")
        self.reset()

        tables_check = self.get_all_relations()
        if not tables_check:
            return False

        check = self.verify_support_files()
        if not check:
            return False

        all_pkfk = self.get_all_pkfk()

        self.make_pkfk_complete_graph(all_pkfk)

        self.do_refinement()

        self.make_index_dict()
        return True

    def make_index_dict(self):
        # GET INDEXES
        f = open(self.create_index_filename, 'rt')
        for elt in self.all_relations:
            self.global_index_dict[elt] = []
        for row in f:
            for elt in self.all_relations:
                if str(row).find(str(elt.upper() + "_")) >= 0:
                    self.global_index_dict[elt].append(str(row.split()[4]))
        f.close()

    def do_refinement(self):
        # refinement
        for elt in self.global_key_lists:
            remove_list = []
            for val in elt:
                if val[0] not in self.all_relations:
                    remove_list.append(val)
            for val in remove_list:
                elt.remove(val)
        while [] in self.global_key_lists:
            self.global_key_lists.remove([])
        remove_list = []
        for elt in self.global_key_lists:
            if len(elt) <= 1:
                remove_list.append(elt)
        for elt in remove_list:
            self.global_key_lists.remove(elt)

    def make_pkfk_complete_graph(self, all_pkfk):
        temp = []
        # GET PK-FK COMPLETE GRAPHS IN FORM OF LISTS
        for row in all_pkfk:
            if row[2] == 'y' or row[2] == 'Y':  # this is used to check if the table contains one or more primary key
                if row[0] in temp:
                    self.global_pk_dict[row[0]] = self.global_pk_dict[row[0]] + "," + str(row[1])
                else:
                    self.global_pk_dict[row[0]] = str(row[1])
                temp.append(row[0])
            found_flag = False
            for elt in self.global_key_lists:
                if (row[0], row[1]) in elt or (row[4], row[5]) in elt:
                    if (row[0], row[1]) not in elt and row[1] != '':
                        elt.append((row[0], row[1]))
                    if (row[4], row[5]) not in elt and row[4] != '':
                        elt.append((row[4], row[5]))
                    found_flag = True
                    break
            if found_flag:
                continue
            if row[0] != '' and row[4] != '':
                self.global_key_lists.append([(row[0], row[1]), (row[4], row[5])])
            elif row[0] != '':
                self.global_key_lists.append([(row[0], row[1])])

    def get_all_pkfk(self):
        # GET PK_DICT and index_dict
        all_pkfk = []
        with open(self.pkfkfilename, 'rt') as f:
            data = csv.reader(f)
            for row in data:
                all_pkfk.append(list(row))
            if len(all_pkfk) > 0:
                del all_pkfk[0]  # it will delete the attribute name of the file
        return all_pkfk
