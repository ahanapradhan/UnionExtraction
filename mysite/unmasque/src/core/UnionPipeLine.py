import time

from . import algorithm1, OldPipeLine
from .UN1_from_clause import UN1FromClause
from .elapsed_time import create_zero_time_profile
from ...refactored.result_comparator import ResultComparator
from ...refactored.util.common_queries import alter_table_rename_to, create_table_like, drop_table, \
    get_restore_name, get_tabname_4, get_tabname_un


def extract(connectionHelper, query):
    t_union_profile = create_zero_time_profile()
    # opening and closing connection actions are vital.
    connectionHelper.connectUsingParams()

    db = UN1FromClause(connectionHelper)
    global_pk_dict = db.fromClause.init.global_pk_dict
    start_time = time.time()
    p, pstr = algorithm1.algo(db, query)

    all_relations = db.get_relations()
    end_time = time.time()
    t_union_profile.update_for_union(end_time - start_time)

    key_lists = db.fromClause.init.global_key_lists
    connectionHelper.closeConnection()

    u_eq = []
    pipeLineError = False

    for rels in p:
        core_relations = []
        for r in rels:
            core_relations.append(r)
        print(core_relations)

        nullify = set(all_relations).difference(core_relations)

        connectionHelper.connectUsingParams()
        nullify_relations(connectionHelper, nullify)
        eq, time_profile = OldPipeLine.extract(connectionHelper, query,
                                               all_relations,
                                               core_relations,
                                               key_lists,
                                               global_pk_dict)
        revert_nullifications(connectionHelper, nullify)
        connectionHelper.closeConnection()

        if eq is not None:
            print(eq)
            eq = eq.replace('Select', '(Select')
            eq = eq.replace(';', ')')
            u_eq.append(eq)
        else:
            pipeLineError = True
            break

        if time_profile is not None:
            t_union_profile.update(time_profile)

    u_Q = "\n UNION ALL \n".join(u_eq)
    u_Q += ";"

    result = ""
    if pipeLineError:
        result = "Could not extract the query due to errors.\nHere's what I have as a half-baked answer:\n" + pstr + "\n"
    result += u_Q
    '''
    connectionHelper.connectUsingParams()
    comparator = ResultComparator(connectionHelper)
    is_same = comparator.doJob(query, u_Q)
    connectionHelper.closeConnection()
    
    t_union_profile.update_for_result_comparator(comparator.local_elapsed_time)

    if is_same:
        print(" Hidden and extrcated Queries produce the same result!")
    else:
        print(" Hidden and extrcated Queries somehow produce different results!")
    '''
    return result, t_union_profile


def nullify_relations(connectionHelper, relations):
    for tab in relations:
        connectionHelper.execute_sql([alter_table_rename_to(tab, get_tabname_un(tab)),
                                      create_table_like(tab, get_tabname_un(tab))])


def revert_nullifications(connectionHelper, relations):
    for tab in relations:
        connectionHelper.execute_sql([drop_table(tab),
                                      alter_table_rename_to(get_tabname_un(tab), tab),
                                      drop_table(get_tabname_un(tab))])


def revert_sideEffects(connectionHelper, relations):
    for tab in relations:
        connectionHelper.execute_sql([drop_table(tab),
                                      alter_table_rename_to(get_restore_name(tab), tab),
                                      drop_table(get_tabname_4(tab))])
