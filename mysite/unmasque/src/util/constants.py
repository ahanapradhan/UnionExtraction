import datetime

# Constants
# These magic numbers are from https://www.postgresql.org/docs/8.1/datatype.htm
min_numeric_val = -2147483648.88
max_numeric_val = 2147483647.88
min_int_val = -2147483648
max_int_val = 2147483647
min_date_val = datetime.date(1, 1, 1)
max_date_val = datetime.date(9999, 12, 31)

# Others
dummy_int = 2
dummy_char = 65  # to avoid having space/tab
dummy_date = datetime.date(1000, 1, 1)
dummy_varbit = format(0, "b")
dummy_boolean = True

TEMP_TABLE = 'temp'
TEMP_TABNAME = "temp"
NO_UNION = "No union present in the query"
local_elapsed_time = 0
method_call_count = 0

SUM = 'Sum'
AVG = 'Avg'
MIN = 'Min'
MAX = 'Max'
COUNT = 'Count'
COUNT_STAR = 'Count(*)'
NO_ORDER = 'noorder'

# To differentiate between count and constants in SELECT clause.
CONST_1_VALUE = '1'
COUNT_THERE = 2
CONST_1_THERE = 1
ORPHAN_COLUMN = "'?column?'"

IDENTICAL_EXPR = "identical_expr_nc"
# we use 1 and 999 to limit, otw overflow can occur
pr_min = 0.01
pr_max = 999
un_precision = 2
max_str_len = 500

OK = "OK "

DATABASE_SECTION = "database"
SUPPORT_SECTION = "support"
LOGGING_SECTION = "logging"
FEATURE_SECTION = "feature"
OPTIONS_SECTION = "options"
TABLE_SIZE_SECTION = "table_sizes"
TABLE = "table"
DATABASE = "database"
HOST = "host"
PORT = "port"
USER = "user"
PASSWORD = "password"
DBNAME = "dbname"
SCHEMA = "schema"
LEVEL = "level"
DETECT_UNION = "union"
DETECT_NEP = "nep"
USE_CS2 = "cs2"
DETECT_OR = "or"
DETECT_OJ = "outer_join"


WAITING = "_WAITING"
DONE = "_DONE"
START = "_START"
ERROR = "_ERROR"
WRONG = "_WRONG"
RUNNING = "_RUNNING"
FROM_CLAUSE = "FROM CLAUSE"
EQUALITY = "EQUALITY PREDICATES"
FILTER = "FILTER"
INEQUALITY = "INEQUALITY PREDICATES"
PROJECTION = "PROJECTION"
GROUP_BY = "GROUP BY"
AGGREGATE = "AGGREGATE"
ORDER_BY = "ORDER BY"
LIMIT = "LIMIT"
UNION = "UNION"
SAMPLING = "SAMPLING"
DB_MINIMIZATION = "DB MINIMIZATION"
RESULT_COMPARE = "RESULT COMPARATOR"
RESTORE_DB = "RESTORE DATABASE"
OUTER_JOIN = "OUTER JOIN"
NEP_COMPARATOR = "NEP_COMPARATOR"
DOWN_SCALE = "scale_down"
SCALE_FACTOR = "scale_factor"
SCALE_RETRY = "scale_retry"
USE_INDEX = "use_index"


NEP_ = "NEP"

LOG_FORMAT = '%(name)s -- %(levelname)s -- %(message)s'


NO_REDUCTION = "NO_REDUCTION"

REL_ERROR = "does not exist"
RELATION = "Relation"
INT_TYPES = ['int', 'integer', 'number']
NUMERIC_TYPES = ['numeric', 'float', 'decimal', 'Decimal', 'real']
NUMBER_TYPES = INT_TYPES + NUMERIC_TYPES
TEXT_TYPES = ['char', 'character', 'character varying', 'str', 'text', 'varchar']
NON_TEXT_TYPES = ['date'] + NUMBER_TYPES

UNMASQUE = "_unmasque_"
WORKING_SCHEMA = "unmasque"
SCALE_DOWN = "_dscale"