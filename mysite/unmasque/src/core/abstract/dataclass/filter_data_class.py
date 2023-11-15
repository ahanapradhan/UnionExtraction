from mysite.unmasque.src.core.abstract.dataclass.only_filter_data import OnlyFilterData
from mysite.unmasque.src.core.abstract.dataclass.whereclause_data_class import WhereData


class FilterData(WhereData, OnlyFilterData):
    def __init__(self):
        WhereData.__init__(self)
        OnlyFilterData.__init__(self)
