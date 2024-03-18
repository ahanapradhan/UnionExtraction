import threading
import sys
import multiprocessing as mp
import time
import ctypes
from queue import Queue

from .ExtractionPipeLine import ExtractionPipeLine
from .UnionPipeLine import UnionPipeLine
from ..util.constants import WAITING

def raise_exception(id):
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(id,
        ctypes.py_object(SystemExit))
    if res > 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(id, 0)
        print('Exception raise failure')

class PipeLineFactory:
    _instance = None
    q = Queue(1)  # blocking queue of size one
    pipeline = None
    result = None
    results = [] 
    pipelines = []
    queries = []
    progress = []
    threads = []
    events = []

    # For maintaining a singleton class
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(PipeLineFactory, cls).__new__(cls)
        return cls._instance

    def doJob(self, query, token):
        # print("lock: ", query)
        print("waiting for queue")
        self.q.put(token, True)
        print("got in")
        pipe = self.get_pipeline_obj(token)
        _stopper = self.get_pipeline_stopper(token)
        qe = [None]
        pipe.doJob(query, qe)
        self.result = qe[0]
        print("Result", qe)
        self.results.append((token, query, qe[0]))
        self.q.get_nowait()
        # print("unlocked: ", query)

    def doJobAsync(self, query, connectionHelper):
        token = hash((query, time.time()))
        pipe = self.create_pipeline(connectionHelper)
        pipe.token = token
        self.pipelines.append(pipe)
        self.queries.append((token, query))
        job = threading.Thread(target=self.doJob, args=(query, token,))
        _stopper = threading.Event()
        self.events.append((token, _stopper))
        self.threads.append((token, job))
        job.start()
        # print("TOKEN", token)
        return token

    def create_pipeline(self, connectionHelper):
        detect_union = connectionHelper.config.detect_union
        pipe = None
        if detect_union:
            pipe = UnionPipeLine(connxectionHelper)
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

    def get_pipeline_thread(self, token):
        for i in self.threads:
            if i[0] == token:
                return i[1]
        return None
    def get_pipeline_stopper(self, token):
        for i in self.events:
            if i[0] == token:
                return i[1]
        return None
    
    def cancel_pipeline_exec(self, token):
        p = self.get_pipeline_obj(token)
        if not p:
            return None
        try:
            if self.q.queue[0] == token:
                _stopper = self.get_pipeline_stopper(token)
                _stopper.set()
                self.results.append((token, self.get_pipeline_query(token), '__CANCELED__'))
                
        except Exception as e:
            print('Nothing being executed', e)
            return None
