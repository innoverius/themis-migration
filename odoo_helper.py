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
        convert_values_to_bytes(contact_vals, ["email"])
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
