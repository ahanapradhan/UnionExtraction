from mysite.unmasque.refactored.abstract.ExtractorBase import Base


class ExtractorModuleBase(Base):
    def __init__(self, connectionHelper, name):
        super().__init__(connectionHelper, name)

    def extract_params_from_args(self, args):
        return args[0]
