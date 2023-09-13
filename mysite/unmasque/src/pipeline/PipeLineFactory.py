from .ExtractionPipeLine import ExtractionPipeLine
from .UnionPipeLine import UnionPipeLine


def get(connectionHelper):
    detect_union = connectionHelper.config.detect_union
    if detect_union:
        pipeline = UnionPipeLine(connectionHelper)
    else:
        pipeline = ExtractionPipeLine(connectionHelper)
    return pipeline
