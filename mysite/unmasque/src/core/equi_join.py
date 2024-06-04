import copy

from ...src.core.abstract.abstractConnection import AbstractConnectionHelper
from ...src.core.abstract.filter_holder import FilterHolder
from ...src.core.filter import Filter
from ...src.util.aoa_utils import get_op, get_tab, get_attrib, merge_equivalent_paritions

from typing import List, Tuple


class U2EquiJoin(FilterHolder):

    def __init__(self, connectionHelper: AbstractConnectionHelper,
                 core_relations: List[str],
                 filter_predicates: list,
                 filter_extractor: Filter,
                 global_min_instance_dict: dict):
        super().__init__(connectionHelper, core_relations, global_min_instance_dict, filter_extractor, "Equi Join")
        self.algebraic_eq_predicates = []
        self.arithmetic_eq_predicates = []
        self.filter_predicates = filter_predicates
        self.pending_predicates = None

    def is_it_equality_op(self, op):
        return op in [self.TEXT_EQUALITY_OP, self.MATH_EQUALITY_OP]

    def doActualJob(self, args=None):
        query = super().doActualJob(args)
        partition_eq_dict, ineqaoa_preds = self.algo2_preprocessing()
        self.logger.debug(partition_eq_dict)
        self.algo3_find_eq_joinGraph(query, partition_eq_dict, ineqaoa_preds)
        self.pending_predicates = ineqaoa_preds  # pending predicates
        self.logger.debug(self.pending_predicates)
        self.logger.debug(self.algebraic_eq_predicates)
        self.logger.debug(self.arithmetic_eq_predicates)
        return True

    def algo3_find_eq_joinGraph(self, query: str, partition_eq_dict: dict, ineqaoa_preds: list) -> None:
        self.logger.debug(partition_eq_dict)
        while partition_eq_dict:
            check_again_dict = {}
            for key in partition_eq_dict.keys():
                equi_join_group = partition_eq_dict[key]
                if len(equi_join_group) <= 3:
                    self.handle_unit_eq_group(equi_join_group, query)
                else:
                    done = self.handle_higher_eq_groups(equi_join_group, query)
                    remaining_group = [eq for eq in equi_join_group if eq not in done]
                    check_again_dict[key] = remaining_group
            partition_eq_dict = check_again_dict
        to_remove = []
        for i, el_eq in enumerate(self.algebraic_eq_predicates):
            for j, pred in enumerate(el_eq):
                if len(pred) > 2:
                    ineqaoa_preds.append(pred)
                    to_remove.append((i, j))
        for tup in to_remove:
            del self.algebraic_eq_predicates[tup[0]][tup[1]]

    def handle_unit_eq_group(self, equi_join_group, query) -> bool:
        filter_attribs = []
        datatype = self.get_datatype(equi_join_group[0])
        self._extract_filter_on_attrib_set(filter_attribs, query, equi_join_group, datatype)
        self.logger.debug("join group check", equi_join_group, filter_attribs)
        if len(filter_attribs) > 0:
            equi_join_group.extend(filter_attribs)
        self.algebraic_eq_predicates.append(equi_join_group)
        return True

    def handle_higher_eq_groups(self, equi_join_group, query):
        seq = list(range(len(equi_join_group)))
        t_all_paritions = merge_equivalent_paritions(seq)
        done = None
        for part in t_all_paritions:
            check_part = min(part, key=len)
            attrib_list = []
            for i in check_part:
                attrib_list.append(equi_join_group[i])
            check = self.handle_unit_eq_group(attrib_list, query)
            if check:
                done = attrib_list
                break
        return done

    def algo2_preprocessing(self) -> Tuple[dict, list]:
        eq_groups_dict = {}
        ineq_filter_predicates = []
        for pred in self.filter_predicates:
            if self.is_it_equality_op(get_op(pred)):
                dict_key = pred[3]
                if dict_key in eq_groups_dict:
                    eq_groups_dict[dict_key].append((pred[0], pred[1]))
                else:
                    eq_groups_dict[dict_key] = [(pred[0], pred[1])]
            else:
                ineq_filter_predicates.append(pred)

        for key in eq_groups_dict.keys():
            if len(eq_groups_dict[key]) == 1:
                op = self.TEXT_EQUALITY_OP if isinstance(key, str) else self.MATH_EQUALITY_OP
                self.arithmetic_eq_predicates.append((get_tab(eq_groups_dict[key][0]),
                                                      get_attrib(eq_groups_dict[key][0]), op, key, key))
        eqJoin_group_dict = {key: value for key, value in eq_groups_dict.items() if len(value) > 1}
        return eqJoin_group_dict, ineq_filter_predicates
