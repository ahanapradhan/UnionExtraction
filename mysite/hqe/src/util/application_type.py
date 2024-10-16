from enum import Enum


class ApplicationType(Enum):
    SQL_ERR_FWD = 1
    SQL_NO_ERR_FWD = 2
    IMPERATIVE_ERR_FWD = 3
    IMPERATIVE_NO_ERR_FWD = 4
