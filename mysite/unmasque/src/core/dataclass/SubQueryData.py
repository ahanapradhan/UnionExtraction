from mysite.unmasque.src.core.dataclass.filter_data_class import FilterData
from mysite.unmasque.src.core.dataclass.from_clause_data_class import FromData
from mysite.unmasque.src.core.dataclass.join_data_class import JoinData
from mysite.unmasque.src.core.dataclass.projection_data_class import ProjectionData


class SubQueryData:
    def __init__(self):
        self.from_clause = FromData()
        self.equi_join = JoinData()
        self.filter = FilterData()
        self.projection = ProjectionData()
        self.d_min_dict = {}
