import csv
import os.path

from ..refactored.abstract.ExtractorBase import Base
from ..refactored.util.common_queries import drop_table
from ..src.pipeline.abstract.TpchSanitizer import TpchSanitizer


class Initiator(Base):

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Initiator")
        self.resource_path = connectionHelper.config.base_path
        self.pkfk_file_path = (self.resource_path / connectionHelper.config.pkfk).resolve()
        self.create_index_filepath = (self.resource_path / connectionHelper.config.index_maker).resolve()
        self.schema = connectionHelper.config.schema
        self.global_index_dict = {}
        self.global_key_lists = [[]]
        self.global_pk_dict = {}
        self.all_relations = []
        self.error = None

    def reset(self):
        self.global_index_dict = {}
        self.global_key_lists = [[]]
        self.global_pk_dict = {}
        self.all_relations = []
        self.error = None

    def get_all_relations(self):
        self.all_relations = set(TpchSanitizer.TABLES)
        """
        try:
            res, desc = self.connectionHelper.execute_sql_fetchall(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = '" + self.schema + "' and TABLE_CATALOG= '" + self.connectionHelper.db + "';")
            self.connectionHelper.execute_sql(["SET search_path = '" + self.schema + "';"])
            for val in res:
                self.all_relations.add(val[0])
        except Exception as error:
            print("Can not obtain table names. Error: " + str(error))
            return False

        if not self.all_relations:
            print("No table in the selected instance. Please select another instance.")
            return False

        self.connectionHelper.execute_sql([drop_table("temp")])
        """
        return True

    def verify_support_files(self):
        check_pkfk = os.path.isfile(self.pkfk_file_path)
        check_idx = os.path.isfile(self.create_index_filepath)
        if (not check_idx) or (not check_pkfk):
            self.error = 'Unmasque Error: \n Support File Not Accessible. '
            print(self.error)
        return check_pkfk and check_idx

    def doActualJob(self, args):
        print("inside -- initialization.initialization")
        self.sanitize()
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
        self.global_index_dict = {elt: [] for elt in self.all_relations}
        with open(self.create_index_filepath, 'rt') as f:
            for row in f:
                for elt in self.all_relations:
                    if str(row).find(str(elt.upper() + "_")) >= 0:
                        self.global_index_dict[elt].append(str(row.split()[4]))

    def do_refinement(self):
        self.global_key_lists = [list(filter(lambda val: val[0] in self.all_relations, elt)) for elt in
                                 self.global_key_lists if elt and len(elt) > 1]

    def make_pkfk_complete_graph(self, all_pkfk):
        temp = []
        for row in all_pkfk:
            if row[2].upper() == 'Y':
                self.global_pk_dict[row[0]] = self.global_pk_dict.get(row[0], '') + ("," if row[0] in temp else '') + \
                                              row[1]
                temp.append(row[0])
            found_flag = False
            for elt in self.global_key_lists:
                if (row[0], row[1]) in elt or (row[4], row[5]) in elt:
                    if (row[0], row[1]) not in elt and row[1]:
                        elt.append((row[0], row[1]))
                    if (row[4], row[5]) not in elt and row[4]:
                        elt.append((row[4], row[5]))
                    found_flag = True
                    break
            if found_flag:
                continue
            if row[0] and row[4]:
                self.global_key_lists.append([(row[0], row[1]), (row[4], row[5])])
            elif row[0]:
                self.global_key_lists.append([(row[0], row[1])])

    def get_all_pkfk(self):
        all_pkfk = []
        with open(self.pkfk_file_path, 'rt') as f:
            data = csv.reader(f)
            all_pkfk = list(data)[1:]
        return all_pkfk
