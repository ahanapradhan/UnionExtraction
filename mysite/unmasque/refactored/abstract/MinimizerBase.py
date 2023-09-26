from ...refactored.abstract.ExtractorBase import Base
from ...refactored.executable import Executable


class Minimizer(Base):

    def __init__(self, connectionHelper, core_relations, all_sizes, name):
        Base.__init__(self, connectionHelper, name)
        self.core_relations = core_relations
        self.app = Executable(connectionHelper)
        self.all_sizes = all_sizes

    def getCoreSizes(self):
        core_sizes = {}
        for table in self.core_relations:
            core_sizes[table] = self.all_sizes[table]
        return core_sizes
