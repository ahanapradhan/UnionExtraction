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

    def __init__(self):
        self.pipeline = None

    def doJob(self, query, connectionHelper):
        print("lock: ", query)
        self.q.put("locked", True)
        self.create_pipeline(connectionHelper)
        qe = self.pipeline.doJob(query)
        self.q.get_nowait()
        print("unlocked: ", query)
        return qe, self.pipeline.time_profile

    def create_pipeline(self, connectionHelper):
        detect_union = connectionHelper.config.detect_union
        if detect_union:
            self.pipeline = UnionPipeLine(connectionHelper)
        else:
            self.pipeline = ExtractionPipeLine(connectionHelper)

    def get_pipeline_state(self):
        return self.pipeline.get_state()
