from mysite.unmasque.refactored.util.common_queries import alter_table_rename_to, create_table_like, get_tabname_un, \
    drop_table
from mysite.unmasque.src.core import algorithm1, OldPipeLine
from mysite.unmasque.src.core.UN1_from_clause import UN1FromClause


def extract(connectionHelper, query):
    connectionHelper.connectUsingParams()

    db = UN1FromClause(connectionHelper)
    global_pk_dict = db.fromClause.init.global_pk_dict
    parttabs, comtabs = algorithm1.algo(db, query)
    all_relations = db.get_relations()
    key_lists = db.fromClause.init.global_key_lists

    nullify_relations(connectionHelper, parttabs)
    eq, _ = OldPipeLine.extract(connectionHelper, query,
                                all_relations,
                                comtabs,
                                key_lists,
                                global_pk_dict)
    revert_nullifications(connectionHelper, parttabs)
    connectionHelper.closeConnection()

    return eq, comtabs


def nullify_relations(connectionHelper, relations):
    for tab in relations:
        connectionHelper.execute_sql([alter_table_rename_to(tab, get_tabname_un(tab)),
                                      create_table_like(tab, get_tabname_un(tab))])


def revert_nullifications(connectionHelper, relations):
    for tab in relations:
        connectionHelper.execute_sql([drop_table(tab),
                                      alter_table_rename_to(get_tabname_un(tab), tab),
                                      drop_table(get_tabname_un(tab))])
