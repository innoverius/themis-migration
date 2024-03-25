import os
from firebird.driver import connect


def connect_to_db(database, user, password):
    conn = connect(
        database=database,
        user=user,
        password=password,
        charset='ISO8859_1',
    )
    return conn


# def connect_to_db_old(database, user, password):
#     conn = firebirdsql.connect(
#         host='localhost',
#         database=database,
#         user=user,
#         password=password,
#         charset='ISO8859_1',
#     )
#     return conn


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


def print_db_tables(cr):
    db_tables = get_db_tables(cr)
    for db_table in db_tables:
        print(db_table[0])


def print_table_columns(cr, table_name):
    table_cols = get_table_columns(cr, table_name)
    for col in table_cols:
        print(col[0])


def print_table_info_for_id(cr, table_name, id_nr):
    table_cols = get_table_columns(cr, table_name)
    columns = [str(col[0]) for col in table_cols]
    columns_string = ",".join(columns)
    sql_string = f"""
                select {columns_string}
                from {table_name}
                where id={str(id_nr)}
                """
    cr.execute(sql_string)
    record = cr.fetchall()[0]
    print(columns_string)
    print(record)
    for name, value in zip(columns, record):
        print(name + ": " + str(value))


company_value_mapping = {
    "ID": "id",
    "NAAM": "name",
    "ONDERNEMINGSNUMMER": "company_registry",
    "BTWNUMMER": "vat",
    "ADRES": "street",
    "POSTCODE": "zip",
    "GEMEENTE": "city",
    "TELEFOON": "phone",
    "MOBIEL": "mobile",
    "EMAIL": "email",
    "URL": "website",
    "VENNOOTSCHAPSNAAM": "company_name",
    "LANDCODE": "country_code",
}


company_comlumns = [
    "ID",
    "NAAM",
    "ONDERNEMINGSNUMMER",
    "BTWNUMMER",
    "ADRES",
    "POSTCODE",
    "GEMEENTE",
    "TELEFOON",
    "MOBIEL",
    "EMAIL",
    "URL",
    "VENNOOTSCHAPSNAAM",
    "LANDCODE",
]


def get_table_values(cr, table_name, value_mapping):
    columns = list(value_mapping.keys())
    records = get_table_rows(cr, table_name, columns)
    for r in records[:100]:
        print(r)


# def create_table_csv(cr, table_name, filename, columns=None):
#     if not columns:
#         table_cols = get_table_columns(cr, table_name)
#         columns = [str(col[0]) for col in table_cols]
#     with open(filename, "w") as out:
#         csv_out = csv.writer(out)
#         csv_out.writerow(columns)
#         csv_out.writerows(get_table_rows(cr, table_name, columns))


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Migrate Themis data to Odoo")
    parser.add_argument("-db", "--database", required=True, help="Path to the database")
    parser.add_argument("-u", "--user", required=True, help="Username")
    parser.add_argument("-p", "--password", required=True, help="Password")
    args = parser.parse_args()
    # db_path = "/Library/Frameworks/Firebird.framework/Versions/A/Resources/examples/empbuild/themis5.fdb"
    db_path = args.database
    username = args.user
    pwd = args.password
    # create_csv_files(db_path, username, pwd, ["ADRESBOEK", "BEDRIJF", "DOSSIER", "GEBRUIKER", "DOSSIERADRESBOEK", "VENNOOTSCHAP"])
    con = connect_to_db(db_path, username, pwd)
    print_db_tables(con.cursor())
    con.close()
    # create_table_csv(con.cursor(), "BEDRIJF", "company.csv")
    # print_table_info_for_id(con.cursor(), "DOSSIERADRESBOEK", 5)
    # cols = [str(col[0]) for col in table_cols]
    # records = get_table_rows(con.cursor(), table, cols)
    # for r in records[:100]:
    #     print(r)
