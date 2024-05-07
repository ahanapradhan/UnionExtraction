from ...src.core.outer_join import OuterJoin
from ...src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from ...src.util.constants import OUTER_JOIN, START, RUNNING, DONE


class OuterJoinPipeLine(ExtractionPipeLine):
    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Outer Join PipeLine")
        self.all_relations = None
        self.pipeLineError = False

    def _after_from_clause_extract(self, query, core_relations):
        eq, time_profile = super()._after_from_clause_extract(query, core_relations)
        self.time_profile.update(time_profile)
        if eq is None:
            return None, time_profile

        # self.connectionHelper.connectUsingParams()
        self.update_state(OUTER_JOIN + START)
        oj = OuterJoin(self.connectionHelper, self.global_pk_dict, self.aoa.pipeline_delivery,
                       self.pj.projected_attribs, self.q_generator, self.pj.projection_names)
        self.update_state(OUTER_JOIN + RUNNING)
        check = oj.doJob(query)
        self.update_state(OUTER_JOIN + DONE)
        self.time_profile.update_for_outer_join(oj.local_elapsed_time, oj.app_calls)
        # self.connectionHelper.closeConnection()
        if not oj.done:
            self.logger.error("Error in outer join extractor")
            return eq
        if not check:
            self.logger.info("No outer join")
            return eq
        if oj.Q_E is not None:
            eq = oj.Q_E
        return eq, self.time_profile
