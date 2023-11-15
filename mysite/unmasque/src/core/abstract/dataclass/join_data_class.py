from mysite.unmasque.src.core.abstract.dataclass.only_join_data_class import OnlyJoinData
from mysite.unmasque.src.core.abstract.dataclass.whereclause_data_class import WhereData


class JoinData(WhereData, OnlyJoinData):

    def __init__(self):
        WhereData.__init__(self)
        OnlyJoinData.__init__(self)

