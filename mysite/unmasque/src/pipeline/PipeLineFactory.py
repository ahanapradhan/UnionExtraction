import threading
import time
from queue import Queue

from .ExtractionPipeLine import ExtractionPipeLine
from .UnionPipeLine import UnionPipeLine
from ..util.constants import WAITING


class PipeLineFactory:
    _instance = None
    q = Queue(1)  # blocking queue of size one
    pipeline = None
    result = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(PipeLineFactory, cls).__new__(cls)
        return cls._instance

    def doJob(self, query):
        print("lock: ", query)
        self.q.put("locked", True)
        qe = self.pipeline.doJob(query)
        self.result = qe
        self.q.get_nowait()
        print("unlocked: ", query)

    def doJobAsync(self, query, connectionHelper):
        token = hash((query, time.time()))
        self.create_pipeline(connectionHelper)
        self.pipeline.token = token
        job = threading.Thread(target=self.doJob, args=(query,))
        job.start()
        print("TOKEN", token)
        return token

    def create_pipeline(self, connectionHelper):
        detect_union = connectionHelper.config.detect_union
        if detect_union:
            self.pipeline = UnionPipeLine(connectionHelper)
        else:
            self.pipeline = ExtractionPipeLine(connectionHelper)

    def get_pipeline_state(self, token):
        if self.pipeline is None:
            print("pipeline none")
        elif self.pipeline.token == token:
            print("..got..", self.pipeline.get_state())
            return self.pipeline.get_state()
        print("...waiting state....")
        return WAITING
