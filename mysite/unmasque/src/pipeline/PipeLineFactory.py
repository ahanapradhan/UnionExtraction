from queue import Queue

from .ExtractionPipeLine import ExtractionPipeLine
from .UnionPipeLine import UnionPipeLine


class PipeLineFactory:
    _instance = None
    q = Queue(1)  # blocking queue of size one

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(PipeLineFactory, cls).__new__(cls)
        return cls._instance

    def doJob(self, query, connectionHelper):
        print("lock: ", query)
        self.q.put("locked", True)
        pipeline = self.get_element(connectionHelper)
        qe = pipeline.doJob(query)
        self.q.get_nowait()
        print("unlocked: ", query)
        return qe, pipeline.time_profile

    def get_element(self, connectionHelper):
        detect_union = connectionHelper.config.detect_union
        if detect_union:
            pipeline = UnionPipeLine(connectionHelper)
        else:
            pipeline = ExtractionPipeLine(connectionHelper)
        return pipeline
