import os
import sys
import base64
import logging
import xmlrpc.client
from striprtf.striprtf import rtf_to_text


themis_datetime_format = "%Y-%m-%d %H:%M:%S"
odoo_datetime_format = "%Y-%m-%d %H:%M:%S"
odoo_date_format = "%Y-%m-%d"


language_mapping = {
    "N": "nl_BE",
    "F": "fr_BE",
    "E": "en_GB",
    "D": "de_DE",
}

logger = logging.getLogger('OdooHelper')


def connect_to_odoo(url, database, username, secret):
    common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
    uid = common.authenticate(database, username, secret, {})
    models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url), allow_none=True)
    return models, uid


def preprocess_user_values(user_vals):
    id_list = []
    inactive_id_list = []
    user_tariff_mapping = {}
    duplicate_logins = {}
    for vals in user_vals:
        user_id = vals.pop("id")
        id_list.append(user_id)
        temp_login = vals["email"] or "login"
        if temp_login in duplicate_logins:
            login = temp_login + str(duplicate_logins[temp_login])
            duplicate_logins[temp_login] += 1
        else:
            login = temp_login
            duplicate_logins[temp_login] = 1
        vals["login"] = login
        if vals.pop("active") != "T":
            inactive_id_list.append(user_id)
        user_tariff_mapping[user_id] = vals.pop("tariff")
    return id_list, inactive_id_list, user_tariff_mapping


def create_themis_users(url, database, username, secret, user_vals):
    logger.info("Migrating Themis users ...")
    models, uid = connect_to_odoo(url, database, username, secret)
    id_list, inactive_id_list, user_tariff_mapping = preprocess_user_values(user_vals)
    response = models.execute_kw(database, uid, secret, "res.users", "create_from_themis", [user_vals])
    logger.info("Created "+ str(len(response)) + " users.")
    id_mapping = dict(zip(id_list, response))
    to_write_ids = []
    for inactive_id in inactive_id_list:
        to_write_ids.append(id_mapping.get(inactive_id, False))
    models.execute_kw(database, uid, secret, "res.users", "write", [to_write_ids, {'active': False}])
    return id_mapping, user_tariff_mapping


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
    logger.info("Migrating Themis party categories ...")
    models, uid = connect_to_odoo(url, database, username, secret)
    id_list = preprocess_party_category_values(party_category_vals)
    party_category_response = models.execute_kw(database, uid, secret, "cases.party_category", "create", [party_category_vals])
    if len(id_list) == len(party_category_response):
        logger.info("Created " + str(len(id_list)) + " party categories.")
        party_category_id_mapping = dict(zip(id_list, party_category_response))
    else:
        party_category_id_mapping = {}
    return party_category_id_mapping


def preprocess_create_write_dates(vals):
    # vals["create_date"] = vals["create_date"] or vals["write_date"]
    # vals["write_date"] = vals["write_date"] or vals["create_date"]
    vals["create_date"] = vals["create_date"] and vals["create_date"].strftime(odoo_datetime_format)
    vals["write_date"] = vals["write_date"] and vals["write_date"].strftime(odoo_datetime_format)


def preprocess_company_values(company_vals, user_id_mapping, country_code_id_mapping):
    id_list = []
    category_id_list = []
    company_bank_vals = []
    for vals in company_vals:
        company_id = vals.pop("id")
        id_list.append(company_id)
        bank_number = vals.pop("bank_number")
        if bank_number:
            company_bank_vals.append({
                "acc_number": bank_number,
                "partner_id": company_id,
            })
        vals["comment"] = vals["comment"] and vals["comment"].replace("\n", "<br>\n")
        country_code = vals.pop("country_code")
        vals["country_id"] = country_code_id_mapping.get(country_code, False)
        if vals["vat"] and vals["vat"][0].isdigit() and country_code:
            vals["vat"] = country_code + vals["vat"]
        if vals["language"] and vals["language"] in language_mapping:
            vals["language"] = language_mapping[vals["language"]]
        elif vals["language"]:
            logger.info("Language not found: " + str(vals["language"]))
        themis_category_id = vals.pop("category_id")
        category_id_list.append(themis_category_id)
        if "is_company" not in vals:
            vals["is_company"] = True
        vals["create_uid"] = vals["create_uid"] and user_id_mapping.get(vals["create_uid"], False)
        preprocess_create_write_dates(vals)
        convert_values_to_bytes(vals, ["email", "email2", "email3", "comment"])
    category_id_mapping = dict(zip(id_list, category_id_list))
    return id_list, category_id_mapping, company_bank_vals


def create_themis_companies(url, database, username, secret, company_vals, user_id_mapping, country_code_id_mapping):
    logger.info("Migrating Themis companies ...")
    models, uid = connect_to_odoo(url, database, username, secret)
    id_list, category_id_mapping, bank_vals = preprocess_company_values(company_vals, user_id_mapping, country_code_id_mapping)
    response = models.execute_kw(database, uid, secret, "res.partner", "create_from_themis", [company_vals])
    logger.info("Created " + str(len(response)) + " companies.")
    id_mapping = dict(zip(id_list, response))
    for vals in bank_vals:
        vals["partner_id"] = id_mapping.get(vals["partner_id"], False)
    models.execute_kw(database, uid, secret, "res.partner.bank", "create", [bank_vals])
    return id_mapping, category_id_mapping


def preprocess_contact_values(contact_vals, company_id_mapping, user_id_mapping, country_code_id_mapping):
    id_list = []
    category_id_list = []
    contact_bank_vals = []
    for vals in contact_vals:
        contact_id = vals.pop("id")
        id_list.append(contact_id)
        bank_number = vals.pop("bank_number")
        if bank_number:
            contact_bank_vals.append({
                "acc_number": bank_number,
                "partner_id": contact_id,
            })
        vals["comment"] = vals["comment"] and vals["comment"].replace("\n", "<br>\n")
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
        if vals["language"] and vals["language"] in language_mapping:
            vals["language"] = language_mapping[vals["language"]]
        elif vals["language"]:
            logger.info("Language not found: " + str(vals["language"]))
        themis_category_id = vals.pop("category_id")
        category_id_list.append(themis_category_id)
        vals["create_uid"] = vals["create_uid"] and user_id_mapping.get(vals["create_uid"], False)
        preprocess_create_write_dates(vals)
        convert_values_to_bytes(vals, ["email", "email2", "email3", "comment"])
    category_id_mapping = dict(zip(id_list, category_id_list))
    return id_list, category_id_mapping, contact_bank_vals


def create_themis_contacts(url, database, username, secret, contact_vals, company_id_mapping, user_id_mapping, country_code_id_mapping):
    logger.info("Migrating Themis contacts ...")
    models, uid = connect_to_odoo(url, database, username, secret)
    id_list, category_id_mapping, bank_vals = preprocess_contact_values(contact_vals, company_id_mapping, user_id_mapping, country_code_id_mapping)
    response = models.execute_kw(database, uid, secret, "res.partner", "create_from_themis", [contact_vals])
    logger.info("Created " + str(len(response)) + " contacts.")
    id_mapping = dict(zip(id_list, response))
    for vals in bank_vals:
        vals["partner_id"] = id_mapping.get(vals["partner_id"], False)
    models.execute_kw(database, uid, secret, "res.partner.bank", "create", [bank_vals])
    return id_mapping, category_id_mapping


def preprocess_case_category_values(case_category_vals):
    id_list = []
    for vals in case_category_vals:
        id_list.append(vals.pop("id"))
    return id_list


def create_themis_case_categories(url, database, username, secret, case_category_vals):
    logger.info("Migrating Themis case categories ...")
    models, uid = connect_to_odoo(url, database, username, secret)
    id_list = preprocess_case_category_values(case_category_vals)
    response = models.execute_kw(database, uid, secret, "cases.case_category", "create", [case_category_vals])
    if len(id_list) == len(response):
        logger.info("Created " + str(len(id_list)) + " case categories.")
        id_mapping = dict(zip(id_list, response))
        return id_mapping
    else:
        return {}


def preprocess_case_values(case_vals, company_id_mapping, contact_id_mapping, user_id_mapping, case_category_id_mapping):
    id_list = []
    active_list = []
    case_tariff_mapping = {}
    for vals in case_vals:
        case_id = vals.pop("id")
        id_list.append(case_id)
        invoice_company_id = vals.pop("invoice_company_id")
        invoice_contact_id = vals.pop("invoice_contact_id")
        invoice_id = (invoice_company_id and company_id_mapping.get(invoice_company_id, False)) or\
                     (invoice_contact_id and contact_id_mapping.get(invoice_contact_id, False))
        vals["partner_id"] = invoice_id
        vals["user_id"] = vals["user_id"] and user_id_mapping.get(vals["user_id"], False)
        if "archived" in vals:
            vals["active"] = vals.pop("archived") == "F"
            active_list.append(vals["active"])
        else:
            active_list.append(True)
        categ_id = case_category_id_mapping.get(vals.pop("category_id"), False)
        vals["case_category_ids"] = categ_id and [(6, 0, [categ_id])]
        vals["create_uid"] = vals["create_uid"] and user_id_mapping.get(vals["create_uid"], False)
        preprocess_create_write_dates(vals)
        case_tariff_mapping[case_id] = vals.pop("tariff")
    active_mapping = dict(zip(id_list, active_list))
    return id_list, active_mapping, case_tariff_mapping


def create_themis_cases(url, database, username, secret, case_vals, company_id_mapping, contact_id_mapping, user_id_mapping, case_category_id_mapping):
    logger.info("Migrating Themis cases ...")
    models, uid = connect_to_odoo(url, database, username, secret)
    id_list, active_mapping, case_tariff_mapping = preprocess_case_values(case_vals, company_id_mapping, contact_id_mapping, user_id_mapping, case_category_id_mapping)
    response = models.execute_kw(database, uid, secret, "cases.case", "create_from_themis", [case_vals])
    if len(id_list) == len(response):
        logger.info("Created " + str(len(id_list)) + " cases.")
        id_mapping = dict(zip(id_list, response))
        return id_mapping, active_mapping, case_tariff_mapping
    else:
        return {}, {}, {}


def preprocess_case_description_vals(case_description_vals, case_description_type_vals, case_id_mapping):
    write_dict = {}
    case_description_name_mapping = {}
    for type_vals in case_description_type_vals:
        case_description_name_mapping[type_vals["id"]] = type_vals["name"]
    for vals in case_description_vals:
        case_id = case_id_mapping.get(vals["case_id"], False)
        if case_id and vals["description"]:
            description_name = case_description_name_mapping.get(vals["type_id"], False) or ""
            description_name = description_name and (description_name + ":")
            if type(vals["description"]) is not bytes:
                desc_bytes = vals["description"].read()
            else:
                desc_bytes = vals["description"]
            text = rtf_to_text(desc_bytes.decode("cp1252")).replace("\n", "<br>\n")
            text = description_name + "<br>\n" + text + "<br>\n"
            if str(case_id) in write_dict:
                logger.info(case_id)
                write_dict[str(case_id)]["description"] += text
            else:
                write_dict[str(case_id)] = {"description": text}
    return write_dict


def write_case_descriptions(url, database, username, secret, case_description_vals, case_description_type_vals, case_id_mapping):
    logger.info("Migrating Themis case descriptions ...")
    models, uid = connect_to_odoo(url, database, username, secret)
    write_dict = preprocess_case_description_vals(case_description_vals, case_description_type_vals, case_id_mapping)
    models.execute_kw(database, uid, secret, "cases.case", "write_from_themis", [write_dict])
    logger.info("Themis case descriptions migrated.")


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
    logger.info("Migrating Themis parties ...")
    models, uid = connect_to_odoo(url, database, username, secret)
    preprocess_party_values(party_vals, company_id_mapping, contact_id_mapping, case_id_mapping, themis_company_category_id_mapping, themis_contact_category_id_mapping, party_category_id_mapping)
    response = models.execute_kw(database, uid, secret, "cases.party", "create", [party_vals])
    logger.info("Created " + str(len(response)) + " parties.")
    return response


def preprocess_timesheet_type_values(timesheet_type_vals):
    id_list = []
    timesheet_type_price_mapping = {}
    for vals in timesheet_type_vals:
        type_id = vals.pop("id")
        id_list.append(type_id)
        vals["detailed_type"] = vals.get("detailed_type", False) or "service"
        vals["service_policy"] = vals.get("service_policy", False) or "delivered_timesheet"
        timesheet_type_price_mapping[type_id] = vals["list_price"]
    return id_list, timesheet_type_price_mapping


def create_themis_timesheet_types(url, database, username, secret, timesheet_type_vals):
    logger.info("Migrating Themis timesheet types ...")
    models, uid = connect_to_odoo(url, database, username, secret)
    id_list, timesheet_type_price_mapping = preprocess_timesheet_type_values(timesheet_type_vals)
    response = models.execute_kw(database, uid, secret, "product.template", "create", [timesheet_type_vals])
    if len(id_list) == len(response):
        logger.info("Created " + str(len(id_list)) + " timesheet types.")
        id_mapping = dict(zip(id_list, response))
        return id_mapping, timesheet_type_price_mapping
    else:
        return {}, timesheet_type_price_mapping


def process_case_timesheet_values(case_timesheet_vals):
    case_timesheet_mapping = {}
    for vals in case_timesheet_vals:
        if vals["case_id"] and vals["user_id"]:
            case_timesheet_mapping[(vals["case_id"], vals["user_id"])] = vals["price_unit"]
    return case_timesheet_mapping


def preprocess_timesheet_values(timesheet_vals, user_id_mapping, user_tariff_mapping, case_id_mapping, case_tariff_mapping, timesheet_type_id_mapping, timesheet_type_price_mapping, case_timesheet_vals):
    case_timesheet_mapping = process_case_timesheet_values(case_timesheet_vals)
    for vals in timesheet_vals:
        themis_type_id = vals["type_id"]
        vals["type_id"] = timesheet_type_id_mapping.get(themis_type_id, False)
        themis_case_id = vals["case_id"]
        vals["case_id"] = case_id_mapping.get(themis_case_id, False)
        themis_user_id = vals["user_id"]
        vals["user_id"] = user_id_mapping.get(themis_user_id, False)
        minutes = vals.pop("minutes") or 0
        vals["unit_amount"] = minutes / 60
        price_unit = vals["price_unit"]
        if not price_unit and price_unit != 0:
            case_user_tariff = case_timesheet_mapping.get((themis_case_id, themis_user_id), False)
            if not case_user_tariff and case_user_tariff != 0:
                user_tariff = user_tariff_mapping.get(themis_user_id, False)
                if not user_tariff and user_tariff != 0:
                    case_tariff = case_tariff_mapping.get(themis_case_id, False)
                    if not case_tariff and case_tariff != 0:
                        vals["price_unit"] = timesheet_type_price_mapping.get(themis_type_id, False) or 0
                    else:
                        vals["price_unit"] = case_tariff
                else:
                    vals["price_unit"] = user_tariff
            else:
                vals["price_unit"] = case_user_tariff
        vals["date"] = vals["date"] and vals["date"].strftime(odoo_date_format)
        vals["billable"] = vals["billable"] == "T"
        vals["billed"] = vals["billed"] == "T"


def preprocess_cost_type_values(cost_type_vals):
    id_list = []
    cost_type_price_mapping = {}
    for vals in cost_type_vals:
        type_id = vals.pop("id")
        id_list.append(type_id)
        vals["detailed_type"] = vals.get("detailed_type", False) or "service"
        vals["service_policy"] = vals.get("service_policy", False) or "ordered_prepaid"
        cost_type_price_mapping[type_id] = vals["list_price"]
    return id_list, cost_type_price_mapping


def create_themis_cost_types(url, database, username, secret, cost_type_vals):
    logger.info("Migrating Themis cost types ...")
    models, uid = connect_to_odoo(url, database, username, secret)
    id_list, cost_type_price_mapping = preprocess_cost_type_values(cost_type_vals)
    response = models.execute_kw(database, uid, secret, "product.template", "create", [cost_type_vals])
    if len(id_list) == len(response):
        logger.info("Created " + str(len(id_list)) + " cost types.")
        id_mapping = dict(zip(id_list, response))
        return id_mapping, cost_type_price_mapping
    else:
        return {}, cost_type_price_mapping


def process_case_cost_values(case_cost_vals):
    case_cost_mapping = {}
    for vals in case_cost_vals:
        if vals["case_id"] and vals["type_id"]:
            case_cost_mapping[(vals["case_id"], vals["type_id"])] = vals["price_unit"]
    return case_cost_mapping


def preprocess_cost_values(cost_vals, case_id_mapping, cost_type_id_mapping, cost_type_price_mapping, case_cost_vals):
    case_cost_mapping = process_case_cost_values(case_cost_vals)
    for vals in cost_vals:
        themis_type_id = vals["type_id"]
        vals["type_id"] = cost_type_id_mapping.get(themis_type_id, False)
        themis_case_id = vals["case_id"]
        vals["case_id"] = case_id_mapping.get(themis_case_id, False)
        vals["date"] = vals["date"] and vals["date"].strftime(odoo_date_format)
        amount = vals["amount"]
        if amount:
            price = vals["price"]
            if not price and price != 0:
                price_unit = vals["price_unit"]
                if not price_unit and price_unit != 0:
                    case_cost = case_cost_mapping.get((themis_case_id, themis_type_id), False)
                    if not case_cost and case_cost != 0:
                        vals["price_unit"] = cost_type_price_mapping.get(themis_type_id, False) or 0
                    else:
                        vals["price_unit"] = case_cost
                else:
                    vals["price_unit"] = price_unit
            else:
                vals["price_unit"] = price / amount
        vals["billable"] = vals["billable"] == "T"
        vals["billed"] = vals["billed"] == "T"


def create_themis_timesheets_costs(url, database, username, secret, timesheet_vals, cost_vals, user_id_mapping, user_tariff_mapping, case_id_mapping, case_tariff_mapping, timesheet_type_id_mapping, timesheet_type_price_mapping, cost_type_id_mapping, cost_type_price_mapping, case_timesheet_vals, case_cost_vals):
    logger.info("Migrating Themis timesheets and costs ...")
    models, uid = connect_to_odoo(url, database, username, secret)
    preprocess_timesheet_values(timesheet_vals, user_id_mapping, user_tariff_mapping, case_id_mapping, case_tariff_mapping, timesheet_type_id_mapping, timesheet_type_price_mapping, case_timesheet_vals)
    preprocess_cost_values(cost_vals, case_id_mapping, cost_type_id_mapping, cost_type_price_mapping, case_cost_vals)
    response = models.execute_kw(database, uid, secret, "cases.case", "create_timesheets_costs_from_themis", [timesheet_vals, cost_vals])
    logger.info("Created " + str(len(response[0])) + " timesheets.")
    logger.info("Created " + str(len(response[1])) + " costs.")
    return response


def preprocess_document_category_values(document_category_vals):
    id_list = []
    for vals in document_category_vals:
        id_list.append(vals.pop("id"))
    return id_list


def create_themis_document_categories(url, database, username, secret, document_category_vals):
    logger.info("Migrating Themis document categories ...")
    models, uid = connect_to_odoo(url, database, username, secret)
    id_list = preprocess_document_category_values(document_category_vals)
    response = models.execute_kw(database, uid, secret, "cases.document_category", "create", [document_category_vals])
    if len(id_list) == len(response):
        logger.info("Created " + str(len(id_list)) + " document categories.")
        id_mapping = dict(zip(id_list, response))
        return id_mapping
    else:
        return {}


def preprocess_document_values(vals, document_path, case_id_mapping, active_mapping, user_id_mapping, document_category_id_mapping):
    if "case_id" in vals:
        dir_nb = vals["case_id"]
        filepath = os.path.join(document_path, str(dir_nb) + "/" + vals["filename"])
        try:
            with open(filepath, "rb") as data:
                datas = base64.b64encode(data.read()).decode("utf-8")
                # datas = base64.b64encode(data.read()).decode("utf-8", errors="replace").replace("\x00", "\uFFFD")
        except FileNotFoundError:
            # logger.info("File at " + str(filepath) + " not found.")
            return False
        else:
            vals["datas"] = datas
            themis_case_id = vals["case_id"]
            vals["active"] = active_mapping.get(themis_case_id, True)
            vals["case_id"] = case_id_mapping.get(themis_case_id, False)
            categ_id = document_category_id_mapping.get(vals.pop("category_id"), False)
            vals["document_category_ids"] = categ_id and [(6, 0, [categ_id])]
            vals["create_uid"] = vals["create_uid"] and user_id_mapping.get(vals["create_uid"], False)
            preprocess_create_write_dates(vals)
            return True
    else:
        return False


def create_documents(models, database, uid, secret, vals_list):
    if vals_list:
        try:
            response = models.execute_kw(database, uid, secret, "cases.document", "create_from_themis", [vals_list])
        except Exception as e:
            if len(vals_list) == 1:
                logger.info("Error occured when migrating document: \n", e)
                logger.info("Document name: " + str(vals_list[0].get("filename", "")))
                logger.info("Odoo case id: " + str(vals_list[0].get("case_id", "")))
            else:
                logger.info("Error occured when migrating documents: \n", e)
                logger.info("Splitting documents into 2 ...")
                lst1 = vals_list[:len(vals_list) // 2]
                create_documents(models, database, uid, secret, lst1)
                lst2 = vals_list[len(vals_list) // 2:]
                create_documents(models, database, uid, secret, lst2)
        else:
            logger.info("Created " + str(len(response)) + " documents.")


def create_themis_documents(url, database, username, secret, document_vals, document_path, case_id_mapping, active_mapping, user_id_mapping, document_category_id_mapping):
    logger.info("Migrating Themis documents ...")
    models, uid = connect_to_odoo(url, database, username, secret)
    temp_vals = []
    temp_size = 0
    max_size = 30000000
    while len(document_vals) > 0:
        vals = document_vals.pop()
        if preprocess_document_values(vals, document_path, case_id_mapping, active_mapping, user_id_mapping, document_category_id_mapping):
            data_size = sys.getsizeof(vals["datas"])
            if temp_size + data_size < max_size:
                temp_vals.append(vals)
                temp_size += data_size
            else:
                create_documents(models, database, uid, secret, temp_vals)
                temp_vals = [vals]
                temp_size = data_size
    if temp_vals:
        response = models.execute_kw(database, uid, secret, "cases.document", "create_from_themis", [temp_vals])
        logger.info("Created " + str(len(response)) + " documents.")
