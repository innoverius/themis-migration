import os
import sys
import base64
import xmlrpc.client


def connect_to_odoo(url, database, username, secret):
    common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
    uid = common.authenticate(database, username, secret, {})
    models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url), allow_none=True)
    return models, uid


def convert_values_to_bytes(dic, keys):
    for key in keys:
        if key in dic:
            dic[key] = bytes(str(dic[key] or ''), 'utf-8')


def preprocess_company_values(company_vals):
    id_list = []
    for vals in company_vals:
        id_list.append(vals.pop("id"))
        if "is_company" not in vals:
            vals["is_company"] = True
    return id_list


def create_themis_companies(url, database, username, secret, company_vals):
    models, uid = connect_to_odoo(url, database, username, secret)
    id_list = preprocess_company_values(company_vals)
    response = models.execute_kw(database, uid, secret, "res.partner", "create", [company_vals])
    if len(id_list) == len(response):
        print(len(id_list))
        id_mapping = dict(zip(id_list, response))
        return id_mapping
    else:
        return {}


def preprocess_contact_values(contact_vals, company_id_mapping):
    id_list = []
    for vals in contact_vals:
        id_list.append(vals.pop("id"))
        convert_values_to_bytes(vals, ["email"])
        if "parent_id" in vals:
            vals["parent_id"] = company_id_mapping.get(vals["parent_id"], False)
    return id_list


def create_themis_contacts(url, database, username, secret, contact_vals, company_id_mapping):
    models, uid = connect_to_odoo(url, database, username, secret)
    id_list = preprocess_contact_values(contact_vals, company_id_mapping)
    response = models.execute_kw(database, uid, secret, "res.partner", "create", [contact_vals])
    if len(id_list) == len(response):
        print(len(id_list))
        id_mapping = dict(zip(id_list, response))
        return id_mapping
    else:
        return {}


def preprocess_case_values(case_vals):
    id_list = []
    for vals in case_vals:
        id_list.append(vals.pop("id"))
        if "archived" in vals:
            vals["active"] = vals.pop("archived") == "F"
    return id_list


def create_themis_cases(url, database, username, secret, case_vals):
    models, uid = connect_to_odoo(url, database, username, secret)
    id_list = preprocess_case_values(case_vals)
    response = models.execute_kw(database, uid, secret, "cases.case", "create", [case_vals])
    if len(id_list) == len(response):
        print(len(id_list))
        id_mapping = dict(zip(id_list, response))
        return id_mapping
    else:
        return {}


def preprocess_party_values(party_vals, company_id_mapping, contact_id_mapping, case_id_mapping):
    for vals in party_vals:
        company_id = company_id_mapping.get(vals.pop("company_id"), False)
        contact_id = contact_id_mapping.get(vals.pop("contact_id"), False)
        vals["partner_id"] = contact_id or company_id or False
        vals["case_id"] = case_id_mapping.get(vals["case_id"], False)


def create_themis_parties(url, database, username, secret, party_vals, company_id_mapping, contact_id_mapping, case_id_mapping):
    models, uid = connect_to_odoo(url, database, username, secret)
    preprocess_party_values(party_vals, company_id_mapping, contact_id_mapping, case_id_mapping)
    response = models.execute_kw(database, uid, secret, "cases.party", "create", [party_vals])
    print(len(response))
    return response


def preprocess_document_values(vals, document_path, case_id_mapping):
    if "case_id" in vals:
        dir_nb = vals["case_id"]
        with open(os.path.join(document_path, str(dir_nb) + "/" + vals["filename"]), "rb") as data:
            datas = base64.b64encode(data.read()).decode("utf-8")
            vals["datas"] = datas
        vals["case_id"] = case_id_mapping.get(vals["case_id"], False)


def create_themis_documents(url, database, username, secret, document_vals, document_path, case_id_mapping):
    models, uid = connect_to_odoo(url, database, username, secret)
    temp_vals = []
    temp_size = 0
    max_size = 30000000
    while len(document_vals) > 0:
        vals = document_vals.pop()
        preprocess_document_values(vals, document_path, case_id_mapping)
        data_size = sys.getsizeof(vals["datas"])
        if temp_size + data_size < max_size:
            temp_vals.append(vals)
            temp_size += data_size
        else:
            response = models.execute_kw(database, uid, secret, "cases.document", "create", [temp_vals])
            temp_vals = [vals]
            temp_size = data_size
            print(len(response))
