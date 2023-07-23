def drop_table(tab):
    return "drop table " + tab + ";"


def alter_table_rename_to(tab, retab):
    return "Alter table " + tab + " rename to " + retab + ";"


def create_table_like(tab, ctab):
    return "Create table " + tab + " (like " + ctab + ");"
