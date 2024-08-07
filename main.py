import csv
import os
import argparse
import logging
from pathlib import Path

from themis_helper import connect_to_db, get_table_rows, get_table_values
from odoo_helper import *


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
    for name, value in zip(columns, record):
        print(name + ": " + str(value))


user_value_mapping = {
    "ID": "id",
    "NAAM": "name",
    "TEL_MOBIEL": "mobile",
    "EMAILADRES": "email",
    "UURTARIEF": "tariff",
    "ACTIEF": "active",
}

party_category_value_mapping = {
    "ID": "id",
    "OMSCHRIJVING": "name",
}

# TODO deal with multiple bank numbers?
company_value_mapping = {
    "ID": "id",
    "NAAM": "name",
    "ONDERNEMINGSNUMMER": "company_id_number",
    "BTWNUMMER": "vat",
    "BANKREKENING": "bank_number",
    "ADRES": "street",
    "POSTCODE": "zip",
    "GEMEENTE": "city",
    "TELEFOON": "phone",
    "TELEFOON2": "phone2",
    "MOBIEL": "mobile",
    "EMAIL": "email",
    "EMAIL2": "email2",
    "EMAIL3": "email3",
    "TAALCODE": "language",
    "URL": "website",
    "VENNOOTSCHAPSNAAM": "company_name",
    "LANDCODE": "country_code",
    "ADRESCATEGORIE_ID": "category_id",
    "OPMERKING": "comment",
    "CREATED": "create_date",
    "CREATEDBY_ID": "create_uid",
    "MODIFIED": "write_date",
}

contact_value_mapping = {
    "ID": "id",
    "BEDRIJF_ID": "parent_id",
    "NAAMVOORNAAM": "name",
    "BANKREKENING": "bank_number",
    "ADRES": "street",
    "POSTCODE": "zip",
    "MANUALZIP": "manualzip",
    "GEMEENTE": "city",
    "LANDCODE": "country_code",
    "TELEFOON": "phone",
    "TELEFOON2": "phone2",
    "MOBIEL": "mobile",
    "EMAIL": "email",
    "EMAIL2": "email2",
    "EMAIL3": "email3",
    "BEROEP": "function",
    "NAAM": "lastname",
    "VOORNAAM": "firstname",
    "AANSPREKING_TITEL": "title",
    "AANSPREKING_GEACHTE": "salutation",
    "GESLACHT": "gender",
    "GEBOORTEDATUM": "dateofbirth",
    "GEBOORTEPLAATS": "placeofbirth",
    "NATIONALITEIT": "nationality",
    "INSZ": "national_number",
    "TAALCODE": "language",
    "URL": "website",
    "ADRESCATEGORIE_ID": "category_id",
    "OPMERKING": "comment",
    "CREATED": "create_date",
    "CREATEDBY_ID": "create_uid",
    "MODIFIED": "write_date",
}
# TODO migrate dossier type?
case_category_value_mapping = {
    "ID": "id",
    "NEDERLANDS": "name",
}

case_value_mapping = {
    "ID": "id",
    "OMSCHRIJVING": "name",
    "NUMMER": "reference_number",
    "KANTOORBEHEERDER_ID": "user_id",
    "FACTURATIEADRESBOEK_ID": "invoice_contact_id",
    "FACTURATIEBEDRIJF_ID": "invoice_company_id",
    "DOSSIERCATEGORIE_ID": "category_id",
    "GEARCHIVEERD": "archived",
    "REGISTREERDER_ID": "create_uid",
    "OPENINGSDATUM": "create_date",
    "MODIFIED": "write_date",
    "UURTARIEF": "tariff",
}

party_value_mapping = {
    "DOSSIER_ID": "case_id",
    "ADRESBOEK_ID": "contact_id",
    "DOSSIERADRESCATEGORIE_ID": "category_id",
    "BEDRIJF_ID": "company_id",
}

timesheet_type_value_mapping = {
    "ID": "id",
    "OMSCHRIJVING": "name",
    "UURTARIEF": "list_price",
}

case_timesheet_value_mapping = {
    "DOSSIER_ID": "case_id",
    "GEBRUIKER_ID": "user_id",
    "UURTARIEF": "price_unit",
}

timesheet_value_mapping = {
    "OMSCHRIJVING": "name",
    "TIJDTYPE_ID": "type_id",
    "DOSSIER_ID": "case_id",
    "REGISTREERDER_ID": "user_id",
    "MINUTEN": "minutes",  # TODO GEPRESTEERD of MINUTEN?
    "UURTARIEF": "price_unit",
    "DATUM": "date",
    "AANREKENEN": "billable",
    "GEFACTUREERD": "billed",
}

cost_type_value_mapping = {
    "ID": "id",
    "N": "name",
    "EENHEIDSBEDRAG": "list_price",
}

case_cost_value_mapping = {
    "DOSSIER_ID": "case_id",
    "KOSTTYPE_ID": "type_id",
    "EENHEIDSBEDRAG": "price_unit",
}

cost_value_mapping = {
    "OMSCHRIJVING": "name",
    "KOSTTYPE_ID": "type_id",
    "DOSSIER_ID": "case_id",
    # "DATUM": "date",
    "AANTAL": "amount",
    "BEDRAG": "price",
    "KOSTEENHEIDSBEDRAG": "price_unit",
    "AANREKENEN": "billable",
    "GEFACTUREERD": "billed",
}

document_category_value_mapping = {
    "ID": "id",
    "OMSCHRIJVING": "name",
}

# TODO archive documents of archived case
# TODO create date and write date
document_value_mapping = {
    "LINKEDTO_ID": "case_id",
    "OMSCHRIJVING": "name",
    "BESTAND": "filename",
    "DOCUMENTMAP_ID": "category_id",
    "AANMAKER_ID": "create_uid",
    "AANMAAKDATUM": "create_date",
    "AANPASDATUM": "write_date",
}

case_description_type_value_mapping = {
    "ID": "id",
    "N": "name",
}

case_description_value_mapping = {
    "DOSSIER_ID": "case_id",
    "OPMERKINGTYPE_ID": "type_id",
    "OPMERKING": "description",
}

# def get_table_values(cr, table_name, value_mapping):
#     columns = list(value_mapping.keys())
#     records = get_table_rows(cr, table_name, columns)
#     for r in records[:100]:
#         print(r)


def create_table_csv(cr, table_name, filename, columns=None):
    if not columns:
        table_cols = get_table_columns(cr, table_name)
        columns = [str(col[0]) for col in table_cols]
    with open(filename, "w") as out:
        csv_out = csv.writer(out)
        csv_out.writerow(columns)
        csv_out.writerows(get_table_rows(cr, table_name, columns))


def parse_arguments():
    parser = argparse.ArgumentParser(description="Migrate Themis data to Odoo")
    parser.add_argument("-tdb", dest="themisdb", required=True, help="Path to the Themis database")
    parser.add_argument("-tdf", dest="documentpath", required=True, help="Path to the Themis documents folder")
    parser.add_argument("-url", dest="url", required=True, help="Url of the Odoo database")
    parser.add_argument("-odb", dest="odoodb", required=True, help="Name of the Odoo database")
    parser.add_argument("-u", dest="user", required=True, help="Odoo user name")
    parser.add_argument("-s", dest="secret", required=True, help="Odoo user password or API key")
    parser.add_argument("-lf", dest="logfile", required=True, help="File location for logs")
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_arguments()

    logfile = args.logfile
    logfilepath = Path(logfile)
    logfilepath.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(filename=logfile,
                        filemode='a',
                        format='%(asctime)s %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.DEBUG)
    logger = logging.getLogger('ThemisMigration')
    logger.info("Starting Themis migration ...")

    themis_db = args.themisdb
    # themis_db = "/Library/Frameworks/Firebird.framework/Versions/A/Resources/examples/empbuild/themis5.fdb"
    con = connect_to_db(themis_db)
    cr = con.cursor()

    user_vals = get_table_values(cr, "GEBRUIKER", user_value_mapping)
    user_id_mapping, user_tariff_mapping = create_themis_users(args.url, args.odoodb, args.user, args.secret, user_vals)
    country_code_id_mapping = get_country_code_id_mapping(args.url, args.odoodb, args.user, args.secret)
    company_vals = get_table_values(cr, "BEDRIJF", company_value_mapping)
    company_id_mapping, themis_company_category_id_mapping = create_themis_companies(args.url, args.odoodb, args.user, args.secret, company_vals, user_id_mapping, country_code_id_mapping)
    contact_vals = get_table_values(cr, "ADRESBOEK", contact_value_mapping)
    contact_id_mapping, themis_contact_category_id_mapping = create_themis_contacts(args.url, args.odoodb, args.user, args.secret, contact_vals, company_id_mapping, user_id_mapping, country_code_id_mapping)

    case_category_vals = get_table_values(cr, "DOSSIERCATEGORIE", case_category_value_mapping)
    case_category_id_mapping = create_themis_case_categories(args.url, args.odoodb, args.user, args.secret, case_category_vals)
    case_vals = get_table_values(cr, "DOSSIER", case_value_mapping)
    case_id_mapping, active_mapping, case_tariff_mapping = create_themis_cases(args.url, args.odoodb, args.user, args.secret, case_vals, company_id_mapping, contact_id_mapping, user_id_mapping, case_category_id_mapping)
    case_description_type_vals = get_table_values(cr, "OPMERKINGTYPE", case_description_type_value_mapping)
    case_description_vals = get_table_values(cr, "DOSSIEROPMERKING", case_description_value_mapping)
    write_case_descriptions(args.url, args.odoodb, args.user, args.secret, case_description_vals, case_description_type_vals, case_id_mapping)

    party_category_vals = get_table_values(cr, "ADRESCATEGORIE", party_category_value_mapping)
    party_category_id_mapping = create_themis_party_categories(args.url, args.odoodb, args.user, args.secret, party_category_vals)
    party_vals = get_table_values(cr, "DOSSIERADRESBOEK", party_value_mapping)
    create_themis_parties(args.url, args.odoodb, args.user, args.secret, party_vals, company_id_mapping, contact_id_mapping, case_id_mapping, themis_company_category_id_mapping, themis_contact_category_id_mapping, party_category_id_mapping)

    timesheet_type_vals = get_table_values(cr, "TIJDTYPE", timesheet_type_value_mapping)
    timesheet_type_id_mapping, timesheet_type_price_mapping = create_themis_timesheet_types(args.url, args.odoodb, args.user, args.secret, timesheet_type_vals)

    cost_type_vals = get_table_values(cr, "KOSTTYPE", cost_type_value_mapping)
    cost_type_id_mapping, cost_type_price_mapping = create_themis_cost_types(args.url, args.odoodb, args.user, args.secret, cost_type_vals)

    timesheet_vals = get_table_values(cr, "DOSSIERTIJD", timesheet_value_mapping)
    case_timesheet_vals = get_table_values(cr, "DOSSIERTIJDTARIEF", case_timesheet_value_mapping)
    cost_vals = get_table_values(cr, "DOSSIERKOST", cost_value_mapping)
    case_cost_vals = get_table_values(cr, "DOSSIERKOSTTARIEF", case_cost_value_mapping)
    timesheet_vals = list(filter(lambda x: x["billed"] != "T", timesheet_vals))
    cost_vals = list(filter(lambda x: x["billed"] != "T", cost_vals))
    create_themis_timesheets_costs(args.url, args.odoodb, args.user, args.secret, timesheet_vals, cost_vals, user_id_mapping, user_tariff_mapping, case_id_mapping, case_tariff_mapping, timesheet_type_id_mapping, timesheet_type_price_mapping, cost_type_id_mapping, cost_type_price_mapping, case_timesheet_vals, case_cost_vals)

    document_category_vals = get_table_values(cr, "DOSSIERDOCUMENTMAP", document_category_value_mapping)
    document_category_id_mapping = create_themis_document_categories(args.url, args.odoodb, args.user, args.secret, document_category_vals)
    document_vals = get_table_values(cr, "DOSSIERDOCUMENT", document_value_mapping)
    document_vals = list(filter(lambda x: x["case_id"], document_vals))
    create_themis_documents(args.url, args.odoodb, args.user, args.secret, document_vals, args.documentpath, case_id_mapping, active_mapping, user_id_mapping, document_category_id_mapping)

    cr.close()
    con.close()

    # create_csv_files(db_path, username, pwd, ["ADRESBOEK", "BEDRIJF", "DOSSIER", "GEBRUIKER", "DOSSIERADRESBOEK", "VENNOOTSCHAP"])
    # con = connect_to_db(themis_db)
    # cr = con.cursor()
    # (url, database, username, secret) = (args.url, args.odoodb, args.user, args.secret)
    # models, uid = connect_to_odoo(url, database, username, secret)
    # models.execute_kw(database, uid, secret, "cases.case", "write_from_themis", [write_dict])
    # print_db_tables(cr)
    # print_table_columns(cr, "DOCUMENTTYPE")
    # print_table_info_for_id(cr, "DOSSIER", 2835)
    # create_table_csv(cr, "GEBRUIKERPROFIEL", "GEBRUIKERPROFIEL.csv")
    # cr.close()
    # con.close()
    # create_table_csv(cr, "BEDRIJF", "company.csv")
    # print_table_info_for_id(cr, "DOSSIERADRESBOEK", 5)
    # cols = [str(col[0]) for col in table_cols]
    # records = get_table_rows(cr, table, cols)
    # for r in records[:100]:
    #     print(r)
