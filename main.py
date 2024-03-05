import firebirdsql


def connect_to_db(database, user, password):
    conn = firebirdsql.connect(
        host='localhost',
        database=database,
        user=user,
        password=password,
        charset='ISO8859_1',
    )
    return conn


def get_db_tables(cr):
    sql_string = """
                SELECT RDB$RELATION_NAME
                FROM RDB$RELATIONS
                WHERE COALESCE(RDB$SYSTEM_FLAG, 0) = 0 AND RDB$RELATION_TYPE = 0
                ORDER BY RDB$RELATION_NAME;
                """
    cr.execute(sql_string)
    return cr.fetchall()


def get_table_columns(cr, table_name):
    sql_string = f"""
                select rdb$field_name
                from rdb$relation_fields
                where rdb$relation_name='{table_name}'
                order by rdb$field_position;
                """
    cr.execute(sql_string)
    return cr.fetchall()


def get_table_rows(cr, table_name, columns):
    columns_string = ",".join(columns)
    sql_string = f"""
                select {columns_string}
                from {table_name}
                """
    cr.execute(sql_string)
    return cr.fetchall()


if __name__ == '__main__':
    db_path = "/Library/Frameworks/Firebird.framework/Versions/A/Resources/examples/empbuild/themis5.fdb"
    # db_path = "employee"
    username = "SYSDBA"
    pwd = "4X2LVYh_VgXBbaR3"
    con = connect_to_db(db_path, username, pwd)
    # db_tables = get_db_tables(con.cursor())
    # for db_table in db_tables:
    #     print(db_table[0])
    table = "DOSSIER"
    table_cols = get_table_columns(con.cursor(), table)
    for col in table_cols:
        print(col[0])
    cols = [str(col[0]) for col in table_cols]
    records = get_table_rows(con.cursor(), table, cols)
    for r in records[:100]:
        print(r)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
# from firebird.driver import connect, driver_config
# driver_config.server_defaults.host.value = 'localhost'
# con = connect(database="employee", user="sysdba", password="masterkey")
# import sys; print(sys.version)
