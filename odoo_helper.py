import os
import sys
import base64
import xmlrpc.client
from datetime import datetime


themis_datetime_format = "%Y-%m-%d %H:%M:%S"


def connect_to_odoo(url, database, username, secret):
    common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
    uid = common.authenticate(database, username, secret, {})
    models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url), allow_none=True)
    return models, uid


def get_country_code_id_mapping(url, database, username, secret):
    models, uid = connect_to_odoo(url, database, username, secret)
    response = models.execute_kw(database, uid, secret, 'res.country', 'search_read', [[]], {'fields': ['code', 'id']})
    code_id_mapping = {}
    for vals in response:
        code_id_mapping[vals["code"]] = vals["id"]
    return code_id_mapping


def convert_values_to_bytes(dic, keys):
    for key in keys:
        if key in dic:
            dic[key] = bytes(str(dic[key] or ''), 'utf-8')


def preprocess_party_category_values(party_category_vals):
    id_list = []
    for vals in party_category_vals:
        id_list.append(vals.pop("id"))
    return id_list


def create_themis_party_categories(url, database, username, secret, party_category_vals):
    models, uid = connect_to_odoo(url, database, username, secret)
    id_list = preprocess_party_category_values(party_category_vals)
    party_category_response = models.execute_kw(database, uid, secret, "cases.party_category", "create", [party_category_vals])
    if len(id_list) == len(party_category_response):
        print(len(id_list))
        party_category_id_mapping = dict(zip(id_list, party_category_response))
    else:
        party_category_id_mapping = {}
    return party_category_id_mapping


def preprocess_company_values(company_vals, country_code_id_mapping):
    id_list = []
    category_id_list = []
    for vals in company_vals:
        id_list.append(vals.pop("id"))
        country_code = vals.pop("country_code")
        vals["country_id"] = country_code_id_mapping.get(country_code, False)
        if vals["vat"] and vals["vat"][0].isdigit() and country_code:
            vals["vat"] = country_code + vals["vat"]
        themis_category_id = vals.pop("category_id")
        category_id_list.append(themis_category_id)
        if "is_company" not in vals:
            vals["is_company"] = True
        vals["create_date"] = vals["create_date"] and vals["create_date"].isoformat()
        vals["write_date"] = vals["write_date"] and vals["write_date"].isoformat()
    return id_list, category_id_list


def create_themis_companies(url, database, username, secret, company_vals, country_code_id_mapping):
    models, uid = connect_to_odoo(url, database, username, secret)
    id_list, category_id_list = preprocess_company_values(company_vals, country_code_id_mapping)
    category_id_mapping = dict(zip(id_list, category_id_list))
    response = models.execute_kw(database, uid, secret, "res.partner", "create_from_themis", [company_vals])
    print(len(response))
    id_mapping = dict(zip(id_list, response))
    return id_mapping, category_id_mapping


def preprocess_contact_values(contact_vals, company_id_mapping, country_code_id_mapping):
    id_list = []
    category_id_list = []
    for vals in contact_vals:
        id_list.append(vals.pop("id"))
        convert_values_to_bytes(vals, ["email"])
        vals["country_id"] = country_code_id_mapping.get(vals.pop("country_code"), False)
        manualzip = vals.pop("manualzip")
        vals["zip"] = vals["zip"] or manualzip
        vals["be_zip"] = vals["zip"]
        vals["be_streetandnumber"] = vals["street"]
        vals["be_municipality"] = vals["city"]
        vals["be_lastname"] = vals.pop("lastname")
        vals["be_firstname"] = vals.pop("firstname")
        vals["be_gender"] = vals.pop("gender")
        dateofbirth = vals.pop("dateofbirth")
        # vals["be_dateofbirth"] = dateofbirth and datetime.strptime(dateofbirth, themis_datetime_format)
        vals["be_dateofbirth"] = dateofbirth and dateofbirth.isoformat()
        vals["be_placeofbirth"] = vals.pop("placeofbirth")
        vals["be_nationality"] = vals.pop("nationality")
        vals["be_national_number"] = vals.pop("national_number")
        vals["parent_id"] = company_id_mapping.get(vals["parent_id"], False)
        themis_category_id = vals.pop("category_id")
        category_id_list.append(themis_category_id)
        vals["create_date"] = vals["create_date"] and vals["create_date"].isoformat()
        vals["write_date"] = vals["write_date"] and vals["write_date"].isoformat()
    return id_list, category_id_list


def create_themis_contacts(url, database, username, secret, contact_vals, company_id_mapping, country_code_id_mapping):
    models, uid = connect_to_odoo(url, database, username, secret)
    id_list, category_id_list = preprocess_contact_values(contact_vals, company_id_mapping, country_code_id_mapping)
    category_id_mapping = dict(zip(id_list, category_id_list))
    response = models.execute_kw(database, uid, secret, "res.partner", "create", [contact_vals])
    print(len(response))
    id_mapping = dict(zip(id_list, response))
    return id_mapping, category_id_mapping


def preprocess_case_category_values(case_category_vals):
    id_list = []
    for vals in case_category_vals:
        id_list.append(vals.pop("id"))
    return id_list


def create_themis_case_categories(url, database, username, secret, case_category_vals):
    models, uid = connect_to_odoo(url, database, username, secret)
    id_list = preprocess_case_category_values(case_category_vals)
    response = models.execute_kw(database, uid, secret, "cases.case_category", "create", [case_category_vals])
    if len(id_list) == len(response):
        print(len(id_list))
        id_mapping = dict(zip(id_list, response))
        return id_mapping
    else:
        return {}


def preprocess_case_values(case_vals, case_category_id_mapping):
    id_list = []
    for vals in case_vals:
        id_list.append(vals.pop("id"))
        if "archived" in vals:
            vals["active"] = vals.pop("archived") == "F"
        categ_id = case_category_id_mapping.get(vals.pop("category_id"), False)
        vals["case_category_ids"] = categ_id and [(6, 0, [categ_id])]
        vals["create_date"] = vals["create_date"] and vals["create_date"].isoformat()
        vals["write_date"] = vals["write_date"] and vals["write_date"].isoformat()
    return id_list


def create_themis_cases(url, database, username, secret, case_vals, case_category_id_mapping):
    models, uid = connect_to_odoo(url, database, username, secret)
    id_list = preprocess_case_values(case_vals, case_category_id_mapping)
    response = models.execute_kw(database, uid, secret, "cases.case", "create", [case_vals])
    if len(id_list) == len(response):
        print(len(id_list))
        id_mapping = dict(zip(id_list, response))
        return id_mapping
    else:
        return {}


def preprocess_party_values(party_vals, company_id_mapping, contact_id_mapping, case_id_mapping, themis_company_category_id_mapping, themis_contact_category_id_mapping, party_category_id_mapping):
    for vals in party_vals:
        themis_company_id = vals.pop("company_id")
        company_id = company_id_mapping.get(themis_company_id, False)
        themis_contact_id = vals.pop("contact_id")
        contact_id = contact_id_mapping.get(themis_contact_id, False)
        if contact_id:
            partner_id = contact_id
            themis_category_id = themis_contact_category_id_mapping.get(themis_contact_id, False)
            category_id = party_category_id_mapping.get(themis_category_id, False)
        elif company_id:
            partner_id = company_id
            themis_category_id = themis_company_category_id_mapping.get(themis_company_id, False)
            category_id = party_category_id_mapping.get(themis_category_id, False)
        else:
            partner_id = False
            category_id = False
        party_category_id = vals.pop("category_id")
        if party_category_id:
            category_id = party_category_id_mapping.get(party_category_id, False)
        vals["partner_id"] = partner_id
        vals["case_id"] = case_id_mapping.get(vals["case_id"], False)
        vals["party_category_ids"] = category_id and [(6, 0, [category_id])]


def create_themis_parties(url, database, username, secret, party_vals, company_id_mapping, contact_id_mapping, case_id_mapping, themis_company_category_id_mapping, themis_contact_category_id_mapping, party_category_id_mapping):
    models, uid = connect_to_odoo(url, database, username, secret)
    preprocess_party_values(party_vals, company_id_mapping, contact_id_mapping, case_id_mapping, themis_company_category_id_mapping, themis_contact_category_id_mapping, party_category_id_mapping)
    response = models.execute_kw(database, uid, secret, "cases.party", "create", [party_vals])
    print(len(response))
    return response


def preprocess_document_category_values(document_category_vals):
    id_list = []
    for vals in document_category_vals:
        id_list.append(vals.pop("id"))
    return id_list


def create_themis_document_categories(url, database, username, secret, document_category_vals):
    models, uid = connect_to_odoo(url, database, username, secret)
    id_list = preprocess_document_category_values(document_category_vals)
    response = models.execute_kw(database, uid, secret, "cases.document_category", "create", [document_category_vals])
    if len(id_list) == len(response):
        print(len(id_list))
        id_mapping = dict(zip(id_list, response))
        return id_mapping
    else:
        return {}


def preprocess_document_values(vals, document_path, case_id_mapping, document_category_id_mapping):
    if "case_id" in vals:
        dir_nb = vals["case_id"]
        filepath = os.path.join(document_path, str(dir_nb) + "/" + vals["filename"])
        try:
            with open(filepath, "rb") as data:
                datas = base64.b64encode(data.read()).decode("utf-8")
        except FileNotFoundError:
            print("File at " + str(filepath) + " not found.")
            return False
        else:
            vals["datas"] = datas
            vals["case_id"] = case_id_mapping.get(vals["case_id"], False)
            categ_id = document_category_id_mapping.get(vals.pop("category_id"), False)
            vals["document_category_ids"] = categ_id and [(6, 0, [categ_id])]
            vals["create_date"] = vals["create_date"] and vals["create_date"].isoformat()
            vals["write_date"] = vals["write_date"] and vals["write_date"].isoformat()
            return True
    else:
        return False


def create_themis_documents(url, database, username, secret, document_vals, document_path, case_id_mapping, document_category_id_mapping):
    models, uid = connect_to_odoo(url, database, username, secret)
    temp_vals = []
    temp_size = 0
    max_size = 30000000
    while len(document_vals) > 0:
        vals = document_vals.pop()
        if preprocess_document_values(vals, document_path, case_id_mapping, document_category_id_mapping):
            data_size = sys.getsizeof(vals["datas"])
            if temp_size + data_size < max_size:
                temp_vals.append(vals)
                temp_size += data_size
            else:
                response = models.execute_kw(database, uid, secret, "cases.document", "create", [temp_vals])
                temp_vals = [vals]
                temp_size = data_size
                print(len(response))
    if temp_vals:
        response = models.execute_kw(database, uid, secret, "cases.document", "create", [temp_vals])
        print(len(response))
