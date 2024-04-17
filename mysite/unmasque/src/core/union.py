from . import algorithm1
from .union_from_clause import UnionFromClause
from mysite.unmasque.src.core.abstract.AppExtractorBase import AppExtractorBase


class Union(AppExtractorBase):

    def __init__(self, connectionHelper):
        super().__init__(connectionHelper, "Union")
        self.key_lists = None
        self.all_relations = None
        self.enabled = self.connectionHelper.config.detect_union

    def extract_params_from_args(self, args):
        return args[0]

    def doActualJob(self, args):
        query = self.extract_params_from_args(args)
        db = UnionFromClause(self.connectionHelper)
        p, pstr, union_profile = algorithm1.algo(db, query)
        self.all_relations = db.get_relations()
        self.key_lists = db.fromClause.init.global_key_lists
        return p, pstr, union_profile



