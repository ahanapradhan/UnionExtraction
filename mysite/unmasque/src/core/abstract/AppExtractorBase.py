from .ExtractorBase import Base
from mysite.unmasque.src.core.executable import Executable


class AppExtractorBase(Base):

    def __init__(self, connectionHelper, name):
        super().__init__(connectionHelper, name)
        self.app = Executable(connectionHelper)
        self.app_calls = 0
        self.enabled = True

    def doAppCountJob(self, args):
        if self.enabled:
            self.app_calls = self.app.method_call_count
            self.result = self.doActualJob(args)
            self.app_calls = self.app.method_call_count - self.app_calls
            return self.result
        return True

