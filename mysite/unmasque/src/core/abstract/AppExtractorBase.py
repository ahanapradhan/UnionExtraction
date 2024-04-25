from abc import ABC

from .ExtractorBase import Base
from ..factory.ExecutableFactory import ExecutableFactory


class AppExtractorBase(Base, ABC):

    def __init__(self, connectionHelper, name):
        super().__init__(connectionHelper, name)
        exe_factory = ExecutableFactory()
        self.app = exe_factory.create_exe(self.connectionHelper)
        self.app_calls = 0
        self.enabled = True

    def doAppCountJob(self, args):
        if self.enabled:
            self.app_calls = self.app.method_call_count
            self.result = self.doActualJob(args)
            self.app_calls = self.app.method_call_count - self.app_calls
            return self.result
        return True

