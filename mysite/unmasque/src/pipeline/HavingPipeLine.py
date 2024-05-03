from abc import ABC

from mysite.unmasque.src.core.abstract.abstractConnection import AbstractConnectionHelper
from ...src.pipeline.abstract.generic_pipeline import GenericPipeLine


class HavingPipeline(GenericPipeLine, ABC):
    def __init__(self, connectionHelper: AbstractConnectionHelper):
        super().__init__(connectionHelper, "Having PipeLine")

    def process(self, query: str):
        return GenericPipeLine.process(self, query)

    def extract(self, query: str):
        # TODO: Having pipline operations go here!
        pass

