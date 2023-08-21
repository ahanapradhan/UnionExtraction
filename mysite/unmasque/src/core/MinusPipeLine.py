from mysite.unmasque.refactored.util.common_queries import alter_table_rename_to, create_table_like, get_tabname_un, \
    drop_table, get_tabname_4
from mysite.unmasque.src.core import algorithm1, OldPipeLine
from mysite.unmasque.src.core.UN1_from_clause import UN1FromClause

working_dir = "/Users/ahanapradhan/Desktop/Projects/UNMASQUE/reduced_data/"


def extract(connectionHelper, query):
    connectionHelper.connectUsingParams()

    db = UN1FromClause(connectionHelper)
    app = db.fromClause.app

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

    restore_to_Dmin(connectionHelper, comtabs)

    Res = app.doJob(query)
    print("======== Result after Q1 extraction ==========")
    print(Res)
    print("=========================")

    revert_nullifications(connectionHelper, parttabs)

    Res = app.doJob(query)
    print("======== Result after Q1 extraction and revert nullifications ==========")
    print(Res)
    print("=========================")
    connectionHelper.closeConnection()

    return eq, comtabs


def restore_to_Dmin(connectionHelper, relations):
    for tab in relations:
        connectionHelper.execute_sql([drop_table(tab),
                                      alter_table_rename_to(get_tabname_4(tab), tab)])


def nullify_relations(connectionHelper, relations):
    for tab in relations:
        connectionHelper.execute_sql([alter_table_rename_to(tab, get_tabname_un(tab)),
                                      create_table_like(tab, get_tabname_un(tab))])


def revert_nullifications(connectionHelper, relations):
    for tab in relations:
        connectionHelper.execute_sql([drop_table(tab),
                                      alter_table_rename_to(get_tabname_un(tab), tab),
                                      drop_table(get_tabname_un(tab))])


def export_vm_data(connectionHelper, tab):
    connectionHelper.execute_sql(["COPY " + tab + " TO '"
                                  + working_dir + tab
                                  + "_q1.csv' DELIMITER ',' CSV HEADER;"])
