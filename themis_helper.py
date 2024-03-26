# import firebirdsql
from firebird.driver import connect


def connect_to_db(database):
    conn = connect(
        database=database,
        charset='ISO8859_1',
    )
    return conn


# def connect_to_db(database):
#     conn = firebirdsql.connect(
#         host="localhost",
#         database=database,
#         user="SYSDBA",
#         password="4X2LVYh_VgXBbaR3",
#         charset='ISO8859_1',
#     )
#     return conn


def get_table_columns(cr, table_name):
    sql_string = f"""
                select rdb$field_name
                from rdb$relation_fields
                where rdb$relation_name='{table_name}'
                order by rdb$field_position;
                """
    cr.execute(sql_string)
    return cr.fetchall()


def get_table_rows(cr, table_name, columns=None):
    if not columns:
        table_cols = get_table_columns(cr, table_name)
        columns = [str(col[0]) for col in table_cols]
    columns_string = ",".join(columns)
    sql_string = f"""
                select {columns_string}
                from {table_name}
                """
    cr.execute(sql_string)
    return cr.fetchall()


def get_table_values(cr, table_name, value_mapping):
    columns = list(value_mapping.keys())
    rows = get_table_rows(cr, table_name, columns)
    keys = [value_mapping[col] for col in columns]
    res = []
    for row in rows:
        # row = map(lambda x: bytes(str(x or ''), 'utf-8'), row)
        res.append(dict(zip(keys, row)))
    return res

