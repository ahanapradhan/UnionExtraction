import copy

from mysite.unmasque.src.pipeline.abstract.generic_pipeline import GenericPipeLine
from mysite.unmasque.src.util.aoa_utils import get_constants_for
from mysite.unmasque.src.util.constants import FILTER
from mysite.unmasque.src.util.utils import get_format, get_val_plus_delta


class DisjunctionPipeLine(GenericPipeLine):

    def __init__(self, extractionPipeLime):
        super().__init__(extractionPipeLime.connectionHelper, "Disjunction PipeLine")
        self.extractionPipeLime = extractionPipeLime
        self.mutation_pipeline = extractionPipeLime.mutation_pipeline
        self.in_candidates_host = self.extractionPipeLime.aoa
        self.get_datatype = self.extractionPipeLime.filter_extractor.get_datatype

    def get_predicates_in_action(self):
        return self.in_candidates_host.arithmetic_eq_predicates

    def process(self, query: str):
        raise NotImplementedError("Reaching here is absurd!")

    def doJob(self, query, qe=None):
        raise NotImplementedError("Reaching here is absurd!")

    def verify_correctness(self, query, result):
        raise NotImplementedError("Reaching here is absurd!")

    def extract_disjunction(self, core_relations, query, time_profile):  # for once
        curr_eq_predicates = copy.deepcopy(self.get_predicates_in_action())
        all_eq_predicates = [curr_eq_predicates]
        ids = list(range(len(curr_eq_predicates)))
        if self.connectionHelper.config.detect_or:
            try:
                time_profile = self.run_extraction_loop(all_eq_predicates, core_relations, ids, query, time_profile)
            except Exception as e:
                self.logger.error("Error in disjunction loop. ", str(e))
                return False, time_profile, None
            '''
            gaining sanity back from nullified attributes

            for tab in core_relations:
                aoa.app.sanitize_one_table(tab)
            # self.logger.debug("All tables restored to get a valid Dmin so that generation pipeline works.")
            aoa, time_profile = self.mutation_pipeline(core_relations, query, time_profile)
            '''
        all_ors = list(zip(*all_eq_predicates))
        return True, time_profile, all_ors

    def run_extraction_loop(self, all_eq_predicates, core_relations, ids, query, time_profile):
        while True:
            or_eq_predicates = []
            for i in ids:
                in_candidates = [copy.deepcopy(em[i]) for em in all_eq_predicates]
                self.logger.debug("Checking OR predicate of ", in_candidates)
                if not len(in_candidates[-1]):
                    or_eq_predicates.append(tuple())
                    continue

                restore_details = self.get_OR_db_restoration_details(core_relations, in_candidates)
                self.logger.debug(restore_details)
                check, time_profile = self.mutation_pipeline(core_relations, query, time_profile, restore_details)
                if not check or not self.get_predicates_in_action():
                    or_eq_predicates.append(tuple())
                else:
                    or_eq_predicates.append(self.get_predicates_in_action()[i])
                self.logger.debug("new or predicates...", all_eq_predicates, or_eq_predicates)
            if all(element == tuple() for element in or_eq_predicates):
                break
            all_eq_predicates.append(or_eq_predicates)
        return time_profile

    def get_OR_db_restoration_details(self, core_relations, in_candidates):
        restore_details = []
        for tab in core_relations:
            where_condition = self.falsify_predicates(tab, in_candidates)
            restore_details.append((tab, where_condition))
        return restore_details

    def falsify_predicates(self, tabname, held_predicates):
        always = "true"
        where_condition = always
        wheres = []
        for pred in held_predicates:
            if not len(pred):
                return where_condition
            tab, attrib, op, lb, ub = pred[0], pred[1], pred[2], pred[3], pred[4]
            if tab != tabname:
                continue
            datatype = self.get_datatype((tab, attrib))
            val_lb, val_ub = get_format(datatype, lb), get_format(datatype, ub)

            if op.lower() in ['equal', '=']:
                where_condition = f"{attrib} != {val_lb}"
            elif op.lower() == 'like':
                where_condition = f"{attrib} NOT LIKE {val_lb}"
            else:
                delta, _ = get_constants_for(datatype)
                val_lb_minus_one = get_format(datatype, get_val_plus_delta(datatype, lb, -1 * delta))
                val_ub_plus_one = get_format(datatype, get_val_plus_delta(datatype, ub, 1 * delta))
                where_condition = f"({attrib} < {val_lb_minus_one} or {attrib} > {val_ub_plus_one})"
            wheres.append(where_condition)
        where_condition = " and ".join(wheres) if len(wheres) else always
        self.logger.debug(where_condition)
        return where_condition

    def extract(self, args):
        core_relations, query, time_profile = args[0], args[1], args[2]
        self.extractionPipeLime.mutation_earlyExit = True
        check, time_profile, ors = self.extract_disjunction(core_relations, query, time_profile)
        self.extractionPipeLime.mutation_earlyExit = False
        return check, time_profile, ors



