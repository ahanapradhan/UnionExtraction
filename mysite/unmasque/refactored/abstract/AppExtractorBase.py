from .ExtractorBase import Base
from ..executable import Executable


class AppExtractorBase(Base):

    def __init__(self, connectionHelper, name):
        super().__init__(connectionHelper, name)
        self.app = Executable(connectionHelper)
        self.app_calls = 0

    def doAppCountJob(self, args):
        print(self.app.method_call_count)
        self.app_calls = self.app.method_call_count
        self.result = self.doActualJob(args)
        self.app_calls = self.app.method_call_count - self.app_calls
        print(self.app.method_call_count)
        return self.result

