import datetime

min_numeric_val = -2147483648.88
max_numeric_val = 2147483647.88
min_int_val = -2147483648
max_int_val = 2147483647
min_date_val = datetime.date(1, 1, 1)
max_date_val = datetime.date(9999, 12, 31)

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

IDENTICAL_EXPR = "identical_expr_nc"
# we use 1 and 999 to limit, otw overflow can occur
pr_min = 1
pr_max = 999

max_str_len = 500

DATABASE_SECTION = "database"
SUPPORT_SECTION = "support"
LOGGING_SECTION = "logging"
FEATURE_SECTION = "feature"
HOST = "host"
PORT = "port"
USER = "user"
PASSWORD = "password"
DBNAME = "dbname"
SCHEMA = "schema"
LEVEL = "level"
DETECT_UNION = "union"
DETECT_NEP = "nep"

WAITING = "_WAITING"
DONE = "_DONE"
START = "_START"
ERROR = "_ERROR"
WRONG = "_WRONG"
RUNNING = "_RUNNING"
FROM_CLAUSE = "FROM CLAUSE"
EQUI_JOIN = "EQUI JOIN"
FILTER = "FILTER"
PROJECTION = "PROJECTION"
GROUP_BY = "GROUP BY"
AGGREGATE = "AGGREGATE"
ORDER_BY = "ORDER BY"
LIMIT = "LIMIT"
UNION = "UNION"
SAMPLING = "SAMPLING"
DB_MINIMIZATION = "DB MINIMIZATION"
RESULT_COMPARE = "RESULT COMPARATOR"

LOG_FORMAT = '%(name)s -- %(levelname)s -- %(message)s'

