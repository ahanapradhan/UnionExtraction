def drop_table(tab):
    return "drop table if exists " + tab + ";"


def alter_table_rename_to(tab, retab):
    return "Alter table " + tab + " rename to " + retab + ";"


def create_table_like(tab, ctab):
    return "Create table " + tab + " (like " + ctab + ");"


def get_row_count(tab):
    return "select count(*) from " + tab + ";"
