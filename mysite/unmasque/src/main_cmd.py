import signal
import sys

from .core.factory.PipeLineFactory import PipeLineFactory
from .pipeline.abstract.TpchSanitizer import TpchSanitizer
from .util.ConnectionFactory import ConnectionHelperFactory
from .util.workload_queries import TestQuery


def signal_handler(signum, frame):
    print('You pressed Ctrl+C!')
    sigconn = ConnectionHelperFactory().createConnectionHelper()
    sigconn.connectUsingParams()
    sanitizer = TpchSanitizer(sigconn)
    sanitizer.sanitize()
    sigconn.closeConnection()
    print("database restored!")
    sys.exit(0)


class TestQuery:
    def __init__(self, name: str, hidden_query: str, cs2: bool, union: bool, oj: bool, nep: bool, orf=None):
        self.qid = name
        self.cs2 = cs2
        self.union = union
        self.oj = oj
        self.nep = nep
        self.query = hidden_query
        self.orf = orf if orf is not None else False


def create_workload():
    test_workload = [TestQuery('Q889', """SELECT club_name, country, found_year FROM club WHERE found_year = (SELECT 
    MIN(found_year) FROM club) OR found_year = (SELECT MAX(found_year) FROM club)""", False, False, False, False, True),
                     TestQuery('Q259', """SELECT T2.teamname\nFROM world_cup AS T1\n    JOIN national_team AS T2 ON 
                     T1.winner = T2.team_id\nGROUP BY T2.teamname\n HAVING COUNT(t1.year) > 2;""", False, False,
                               False, False, True),
                     TestQuery('Q51', """SELECT m.year, nt.teamname, nt2.teamname1, 
                     m.home_team_goals, m.away_team_goals 
FROM match AS m     
JOIN national_team as nt on m.home_team_id = nt.team_id     
JOIN national_team1 as nt2 on nt2.team1_id = m.away_team_id
where m.stage = 'Quarter-finals' order by m.year desc;""", False, False, False, False, False)
                     ]
    return test_workload


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    workload = create_workload()

    workload_dict = {}
    for elem in workload:
        workload_dict[elem.qid] = workload.index(elem)

    # print(workload_dict)

    qid = sys.argv[1]
    hq = workload[workload_dict[qid]]
    query = hq.query
    conn = ConnectionHelperFactory().createConnectionHelper()
    conn.config.detect_union = hq.union
    conn.config.detect_oj = hq.oj
    conn.config.detect_nep = hq.nep
    conn.config.use_cs2 = hq.cs2
    conn.config.detect_or = hq.orf

    print(f"Flags: Union {conn.config.detect_union}, OJ {conn.config.detect_oj}, "
          f"NEP {conn.config.detect_nep}, CS2 {conn.config.use_cs2}")

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    factory = PipeLineFactory()
    token = factory.init_job(conn, query)
    factory.doJob(query, token)
    result = factory.result

    if result is not None:
        print("============= Given Query ===============")
        print(query)
        print("=========== Extracted Query =============")
        print(result)
        print("================ Profile ================")
        pipe = factory.get_pipeline_obj(token)
        pipe.time_profile.print()
    else:
        print("I had some Trouble! Check the log file for the details..")
