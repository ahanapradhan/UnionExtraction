from abc import ABC

from .ExtractorBase import Base
from ..factory.ExecutableFactory import ExecutableFactory
from ...util.constants import UNMASQUE


class AppExtractorBase(Base, ABC):

    def __init__(self, connectionHelper, name, all_sizes=None):
        super().__init__(connectionHelper, name, all_sizes)
        exe_factory = ExecutableFactory()
        self.app = exe_factory.create_exe(self.connectionHelper)
        self.app_calls = 0
        self.enabled = True
        self.dirty_name = UNMASQUE + self.extractor_name

    def _get_dirty_name(self, tab):
        return tab + self.dirty_name

    def reset_data_schema(self):
        self.app.data_schema = self.connectionHelper.config.user_schema

    def set_data_schema(self):
        self.app.data_schema = self.connectionHelper.config.schema

    def doAppCountJob(self, args):
        if self.enabled:
            self.app_calls = self.app.method_call_count
            self.result = self.doActualJob(args)
            self.app_calls = self.app.method_call_count - self.app_calls
            return self.result
        self.logger.debug("Feature is not enabled!")
        return True

