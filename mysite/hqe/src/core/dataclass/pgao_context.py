from ....src.core.aggregation import Aggregation
from ....src.core.groupby_clause import GroupBy
from ....src.core.orderby_clause import OrderBy
from ....src.core.projection import Projection


class PGAOcontext:

    def __init__(self):
        self.joined_attribs = None
        self.projection_names = None
        self.projected_attribs = None
        self.pj_rmin_card = 0
        self.has_groupby = None
        self.group_by_attrib = None
        self.projection_dependencies = None
        self.projection_solution = None
        self.projection_param_list = None
        self.aggregated_attributes = None
        self.orderby_string = None

    @property
    def projection(self):
        return self.pj_rmin_card # used in limit

    @projection.setter
    def projection(self, value: Projection):
        self.joined_attribs = value.joined_attribs
        self.projection_names = value.projection_names
        self.projected_attribs = value.projected_attribs
        self.projection_dependencies = value.dependencies
        self.projection_solution = value.solution
        self.projection_param_list = value.param_list
        self.pj_rmin_card = value.rmin_card

    @property
    def group_by(self):
        raise NotImplementedError

    @group_by.setter
    def group_by(self, value: GroupBy):
        self.has_groupby = value.has_groupby
        self.group_by_attrib = value.group_by_attrib

    @property
    def aggregate(self):
        raise NotImplementedError

    @aggregate.setter
    def aggregate(self, value: Aggregation):
        self.aggregated_attributes = value.global_aggregated_attributes

    @property
    def order_by(self):
        raise NotImplementedError

    @order_by.setter
    def order_by(self, value: OrderBy):
        self.orderby_string = value.orderBy_string

