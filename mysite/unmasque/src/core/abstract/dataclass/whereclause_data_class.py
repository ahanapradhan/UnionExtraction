class WhereData:
    def __init__(self,
                 global_key_lists=None,
                 global_key_attributes=None,
                 global_attrib_types=None,
                 global_all_attribs=None):
        if global_all_attribs is None:
            global_all_attribs = []
        if global_attrib_types is None:
            global_attrib_types = []
        if global_key_attributes is None:
            global_key_attributes = []
        if global_key_lists is None:
            global_key_lists = []
        self.global_key_lists = global_key_lists
        self.global_key_attributes = global_key_attributes
        self.global_attrib_types = global_attrib_types
        self.global_all_attribs = global_all_attribs
