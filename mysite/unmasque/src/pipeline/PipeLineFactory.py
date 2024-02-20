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
    results = []
    pipelines = []
    queries = []
    progress = []

    # For maintaining a singleton class
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(PipeLineFactory, cls).__new__(cls)
        return cls._instance

    def doJob(self, query, token):
        # print("lock: ", query)
        print("waiting for queue")
        self.q.put("locked", True)
        print("got in")
        pipe = self.get_pipeline_obj(token)
        qe = pipe.doJob(query)
        self.result = qe
        print("Result", qe)
        self.results.append((token, query, qe))
        self.q.get_nowait()
        # print("unlocked: ", query)

    def doJobAsync(self, query, connectionHelper):
        token = hash((query, time.time()))
        pipe = self.create_pipeline(connectionHelper)
        pipe.token = token
        self.pipelines.append(pipe)
        self.queries.append((token, query))
        job = threading.Thread(target=self.doJob, args=(query, token,))
        job.start()
        # print("TOKEN", token)
        return token

    def create_pipeline(self, connectionHelper):
        detect_union = connectionHelper.config.detect_union
        pipe = None
        if detect_union:
            pipe = UnionPipeLine(connectionHelper)
        else:
            pipe = ExtractionPipeLine(connectionHelper)
        self.pipeline = pipe
        return pipe

    def get_pipeline_state(self, token):

        pipe = self.get_pipeline_obj(token)
        if pipe:
            return pipe.get_state()

        return WAITING

    def get_pipeline_obj(self, token):
        for i in self.pipelines:
            if i.token == token:
                return i
        return None

    def get_pipeline_query(self, token):
        for q in self.queries:
            if q[0] == token:
                return q[1]

        return None
