import os
import argparse

from themis_helper import connect_to_db, get_table_rows, get_table_values
from odoo_helper import connect_to_odoo, create_themis_companies, create_themis_contacts


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


# def get_table_rows(cr, table_name, columns):
#     columns_string = ",".join(columns)
#     sql_string = f"""
#                 select {columns_string}
#                 from {table_name}
#                 """
#     cr.execute(sql_string)
#     return cr.fetchall()


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
    "ONDERNEMINGSNUMMER": "company_id_number",
    # "BTWNUMMER": "vat",
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


contact_value_mapping = {
    "ID": "id",
    # "BEDRIJF_ID": "parent_id",
    # "NAAMVOORNAAM": "name",
    # "ADRES": "street",
    # "POSTCODE": "zip",
    # "GEMEENTE": "city",
    # "TELEFOON": "phone",
    # "MOBIEL": "mobile",
    "EMAIL": "email",
    # "URL": "website",
    # "LANDCODE": "country_code",
}


# def get_table_values(cr, table_name, value_mapping):
#     columns = list(value_mapping.keys())
#     records = get_table_rows(cr, table_name, columns)
#     for r in records[:100]:
#         print(r)


# def create_table_csv(cr, table_name, filename, columns=None):
#     if not columns:
#         table_cols = get_table_columns(cr, table_name)
#         columns = [str(col[0]) for col in table_cols]
#     with open(filename, "w") as out:
#         csv_out = csv.writer(out)
#         csv_out.writerow(columns)
#         csv_out.writerows(get_table_rows(cr, table_name, columns))


def parse_arguments():
    parser = argparse.ArgumentParser(description="Migrate Themis data to Odoo")
    parser.add_argument("-tdb", dest="themisdb", required=True, help="Path to the Themis database")
    parser.add_argument("-url", dest="url", required=True, help="Url of the Odoo database")
    parser.add_argument("-odb", dest="odoodb", required=True, help="Name of the Odoo database")
    parser.add_argument("-u", dest="user", required=True, help="Odoo user name")
    parser.add_argument("-s", dest="secret", required=True, help="Odoo user password or API key")
    return parser.parse_args()


if __name__ == '__main__':
    # db_path = "/Library/Frameworks/Firebird.framework/Versions/A/Resources/examples/empbuild/themis5.fdb"
    args = parse_arguments()
    themis_db = args.themisdb
    # themis_db = "/Library/Frameworks/Firebird.framework/Versions/A/Resources/examples/empbuild/themis5.fdb"
    # models, uid = connect_to_odoo(args.url, args.odoodb, args.user, args.secret)
    con = connect_to_db(themis_db)
    # contacts = get_table_rows(con.cursor(), "ADRESBOEK", ["ID"])
    # print(len(contacts))
    company_vals = get_table_values(con.cursor(), "BEDRIJF", company_value_mapping)
    company_id_mapping = create_themis_companies(args.url, args.odoodb, args.user, args.secret, company_vals)
    contact_vals = get_table_values(con.cursor(), "ADRESBOEK", contact_value_mapping)
    contact_id_mapping = create_themis_contacts(args.url, args.odoodb, args.user, args.secret, contact_vals, company_id_mapping)
    con.close()
    # create_csv_files(db_path, username, pwd, ["ADRESBOEK", "BEDRIJF", "DOSSIER", "GEBRUIKER", "DOSSIERADRESBOEK", "VENNOOTSCHAP"])
    # con = connect_to_db(db_path)
    # print_db_tables(con.cursor())
    # con.close()
    # create_table_csv(con.cursor(), "BEDRIJF", "company.csv")
    # print_table_info_for_id(con.cursor(), "DOSSIERADRESBOEK", 5)
    # cols = [str(col[0]) for col in table_cols]
    # records = get_table_rows(con.cursor(), table, cols)
    # for r in records[:100]:
    #     print(r)
