import csv
import os.path
import pathlib

from mysite.unmasque.src.core.abstract.ExtractorBase import Base


class Initiator(Base):

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Initiator")
        # Ensure base_path is correctly set in the configuration
        base_path = connectionHelper.config.base_path
        if base_path is None:
            raise ValueError("base_path in configuration cannot be None")
        # Convert base_path to a Path object if it's not already one
        self.resource_path = pathlib.Path(base_path) if not isinstance(base_path, pathlib.Path) else base_path
        self.pkfk_file_path = (self.resource_path / connectionHelper.config.pkfk).resolve()
        self.schema = connectionHelper.config.schema
        self.global_key_lists = [[]]
        self.global_pk_dict = {}
        self.error = None

    def reset(self):
        self.global_key_lists = [[]]
        self.global_pk_dict = {}
        self.all_relations = []
        self.error = None

    def verify_support_files(self):
        check_pkfk = os.path.isfile(self.pkfk_file_path)
        if not check_pkfk:
            self.logger.error("Unmasque Error: \n Support File Not Accessible. ")
        return check_pkfk

    def doActualJob(self, args):
        self.reset()
        check = self.verify_support_files()
        self.logger.info("support files verified..")
        if not check:
            return False
        all_pkfk = self.get_all_pkfk()
        self.make_pkfk_complete_graph(all_pkfk)
        self.do_refinement()
        self.logger.info("loaded pk-fk..", all_pkfk)
        self.take_backup()
        return True

    def do_refinement(self):
        self.global_key_lists = [list(filter(lambda val: val[0] in self.all_relations, elt)) for elt in
                                 self.global_key_lists if elt and len(elt) > 1]

    def make_pkfk_complete_graph(self, all_pkfk):
        all_relations = []
        temp = []
        for row in all_pkfk:
            all_relations.append(row[0])
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
        self.set_all_relations(list(set(all_relations)))
        self.all_relations.sort()
        self.logger.debug("all relations: ", self.all_relations)

    def get_all_pkfk(self):
        with open(self.pkfk_file_path, 'rt') as f:
            data = csv.reader(f)
            all_pkfk = list(data)[1:]
        return all_pkfk
