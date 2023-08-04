from mysite.unmasque.refactored.util.common_queries import alter_table_rename_to, create_table_like, drop_table, \
    get_restore_name, get_tabname_4, get_tabname_un
from mysite.unmasque.src.core import algorithm1, OldPipeLine
from mysite.unmasque.src.core.UN1_from_clause import UN1FromClause


def extract(connectionHelper, query):
    # opening and closing connection actions are vital.
    connectionHelper.connectUsingParams()
    db = UN1FromClause(connectionHelper)
    p, pstr = algorithm1.algo(db, query)

    all_relations = db.get_relations()
    key_lists = db.fromClause.init.global_key_lists
    connectionHelper.closeConnection()

    u_eq = []

    for rels in p:
        core_relations = []
        for r in rels:
            core_relations.append(r)
        print(core_relations)

        nullify = set(all_relations).difference(core_relations)

        connectionHelper.connectUsingParams()
        nullify_relations(connectionHelper, nullify)
        eq = OldPipeLine.extract(connectionHelper, query,
                                 all_relations,
                                 core_relations,
                                 key_lists)
        revert_nullifications(connectionHelper, nullify)
        connectionHelper.closeConnection()

        if eq is not None:
            print(eq)
            eq = eq.replace('Select', '(Select')
            eq = eq.replace(';', ')')
            u_eq.append(eq)
        else:
            print("some error in the union pipeline.")
            return None

    u_Q = "\n union all \n".join(u_eq)
    u_Q += ";"
    return u_Q


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
