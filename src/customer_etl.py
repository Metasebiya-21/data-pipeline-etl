import json
import uuid
from datetime import datetime, date
from typing import Any, Dict, Iterable, List, Optional


def _bool_flag(value: Any) -> Optional[str]:
    if value is None:
        return None
    return "Y" if bool(value) else "N"


def _uuid_to_raw(value: Optional[str]) -> Optional[bytes]:
    if not value:
        return None
    try:
        return uuid.UUID(str(value)).bytes
    except (ValueError, AttributeError):
        return None


def _to_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _to_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _json_dumps(value: Any) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"))


def build_customer_payload(doc: Dict[str, Any]) -> Dict[str, Any]:
    customer_id = str(doc.get("_id"))

    customer = {
        "customer_id": customer_id,
        "phone": doc.get("phone"),

        "device_token": doc.get("deviceToken"),
        "birth_date_epoch": doc.get("birthDate"),
        "full_name": doc.get("fullName"),
        "gender": doc.get("gender"),
        "id_number": doc.get("idNumber"),
        "id_type": doc.get("idType"),
        "tin_number": doc.get("tinNumber"),
        "emergency_contact_name": doc.get("emergencyContactName"),
        "emergency_contact_phone": doc.get("emergencyContactPhone"),
        "request_id": _uuid_to_raw(doc.get("requestId")),
        "created_date_epoch": doc.get("createdDate"),
        "updated_date_epoch": doc.get("updatedDate"),
    }

    verification = doc.get("verification") or {}
    verification_row = {
        "customer_id": customer_id,
        "phone_verified": _bool_flag(verification.get("phone")) or "N",
    }

    account_info = doc.get("userAccountInformation") or {}
    account_row = {
        "customer_id": customer_id,
        "preferred_language": account_info.get("preferredLanguage"),
        "status": account_info.get("status"),
        "current_stage": account_info.get("currentStage"),
        "loan_status": account_info.get("loanStatus"),
        "loan_product_code": account_info.get("loanProductCode"),
    }

    addresses = []
    for addr in doc.get("personalAddresses") or []:
        addresses.append(
            {
                "customer_id": customer_id,
                "region": addr.get("region"),
                "city": addr.get("city"),
                "subcity_zone": addr.get("subcityZone"),
                "woreda": addr.get("woreda"),
                "house_number": addr.get("houseNumber"),
                "created_date_epoch": addr.get("createdDate"),
            }
        )

    bank_accounts = []
    for bank in doc.get("bankInformations") or []:
        bank_accounts.append(
            {
                "customer_id": customer_id,
                "account_number": bank.get("accountNumber"),
                "exists_flag": _bool_flag(bank.get("exists")),
                "owner_verified": _bool_flag(bank.get("ownerVerified")),
                "is_primary": _bool_flag(bank.get("isPrimary")),
                "customer_number": bank.get("customerNumber"),
                "additional_info": bank.get("additionalInformation"),
            }
        )

    education = []
    for edu in doc.get("educationalLevels") or []:
        education.append(
            {
                "customer_id": customer_id,
                "education_level": edu.get("level"),
                "start_date_epoch": edu.get("startDate"),
                "end_date_epoch": edu.get("endDate"),
                "school": edu.get("school"),
                "updated_date_epoch": edu.get("updatedDate"),
            }
        )

    marital_statuses = []
    for status in doc.get("maritalStatuses") or []:
        if status:
            marital_statuses.append({"customer_id": customer_id, "status": status})

    loan_products = []
    for loan in doc.get("loanInformations") or []:
        loan_products.append(
            {
                "customer_id": customer_id,
                "loan_product_id": _uuid_to_raw(loan.get("loanProductId")),
                "loan_product_code": loan.get("loanProductCode"),
                "loan_product_name": loan.get("loanProductName"),
            }
        )

    businesses = []
    business_addresses = []
    for business in doc.get("businesses") or []:
        business_id = business.get("businessId")
        if not business_id:
            continue
        businesses.append(
            {
                "business_id": business_id,
                "customer_id": customer_id,
                "tin": business.get("tin"),
                "business_license_number": business.get("businessLicenseNumber"),
                "tin_of_spouse": business.get("tinOfSpouse"),
                "business_name": business.get("businessName"),
                "manager_tin_number": business.get("managerTinNumber"),
                "business_sector": business.get("businessSector"),
                "association_type": business.get("associationType"),
                "form_of_business": business.get("formOfBusiness"),
                "line_of_business": business.get("lineOfBusiness"),
                "business_registered_date_epoch": business.get("businessRegisteredDate"),
                "number_of_employees": business.get("numberOfEmployees"),
                "starting_number_of_employees": business.get("startingNumberOfEmployees"),
                "source_of_initial_capital": business.get("sourceOfInitialCapital"),
                "starting_capital": business.get("startingCapital"),
                "current_capital": business.get("currentCapital"),
                "annual_sales_income": business.get("annualSalesIncome"),
                "annual_profit": business.get("annualProfit"),
                "business_renewal_date_epoch": business.get("businessRenewalDate"),
                "additional_info_json": _json_dumps(business.get("additionalInformation")),
            }
        )

        for addr in business.get("businessAddresses") or []:
            business_addresses.append(
                {
                    "business_id": business_id,
                    "region": addr.get("region"),
                    "city": addr.get("city"),
                    "subcity_zone": addr.get("subcityZone"),
                    "woreda": addr.get("woreda"),
                    "house_number": addr.get("houseNumber"),
                    "created_date_epoch": addr.get("createdDate"),
                }
            )

    documents = []
    for doc_item in doc.get("documents") or []:
        document_id = doc_item.get("id")
        if not document_id:
            continue
        documents.append(
            {
                "document_id": document_id,
                "customer_id": customer_id,
                "doc_type": doc_item.get("type"),
                "extension": doc_item.get("extension"),
                "url": doc_item.get("url"),
                "status": doc_item.get("status"),
                "uploaded_date": _to_date(doc_item.get("uploadedDate")),
            }
        )

    assigned_officer = None
    officer = doc.get("assignedOfficer")
    if officer and officer.get("id"):
        assigned_officer = {
            "officer": {"officer_id": officer.get("id"), "name": officer.get("name")},
            "assignment": {"customer_id": customer_id, "officer_id": officer.get("id")},
        }

    return {
        "customer": customer,
        "verification": verification_row,
        "account_info": account_row,
        "addresses": addresses,
        "bank_accounts": bank_accounts,
        "education": education,
        "marital_statuses": marital_statuses,
        "loan_products": loan_products,
        "businesses": businesses,
        "business_addresses": business_addresses,
        "documents": documents,
        "assigned_officer": assigned_officer,
    }


def _merge_sql(table: str, key_cols: Iterable[str], set_cols: Iterable[str]) -> str:
    key_on = " AND ".join([f"t.{col} = s.{col}" for col in key_cols])
    update_set = ", ".join([f"t.{col} = s.{col}" for col in set_cols])
    insert_cols = ", ".join(list(key_cols) + list(set_cols))
    insert_vals = ", ".join([f"s.{col}" for col in list(key_cols) + list(set_cols)])
    select_cols = ", ".join([f":{col} AS {col}" for col in list(key_cols) + list(set_cols)])

    return f"""
        MERGE INTO {table} t
        USING (
            SELECT {select_cols}
            FROM dual
        ) s
        ON ({key_on})
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """


def load_customer(cursor, payload: Dict[str, Any]) -> None:
    customer = payload["customer"]
    customer_id = customer["customer_id"]

    customer_sql = _merge_sql(
        "CUSTOMERS",
        ["customer_id"],
        [
            "phone",
            "device_token",
            "birth_date_epoch",
            "full_name",
            "gender",
            "id_number",
            "id_type",
            "tin_number",
            "emergency_contact_name",
            "emergency_contact_phone",
            "request_id",
            "created_date_epoch",
            "updated_date_epoch",
        ],
    )
    cursor.execute(customer_sql, customer)

    verification_sql = _merge_sql(
        "CUSTOMER_VERIFICATION",
        ["customer_id"],
        ["phone_verified"],
    )
    cursor.execute(verification_sql, payload["verification"])

    account_sql = _merge_sql(
        "CUSTOMER_ACCOUNT_INFO",
        ["customer_id"],
        [
            "preferred_language",
            "status",
            "current_stage",
            "loan_status",
            "loan_product_code",
        ],
    )
    cursor.execute(account_sql, payload["account_info"])

    cursor.execute(
        "DELETE FROM CUSTOMER_PERSONAL_ADDRESSES WHERE CUSTOMER_ID = :customer_id",
        {"customer_id": customer_id},
    )
    for addr in payload["addresses"]:
        cursor.execute(
            """
            INSERT INTO CUSTOMER_PERSONAL_ADDRESSES
              (CUSTOMER_ID, REGION, CITY, SUBCITY_ZONE, WOREDA, HOUSE_NUMBER, CREATED_DATE_EPOCH)
            VALUES
              (:customer_id, :region, :city, :subcity_zone, :woreda, :house_number, :created_date_epoch)
            """,
            addr,
        )

    cursor.execute(
        "DELETE FROM CUSTOMER_EDUCATION WHERE CUSTOMER_ID = :customer_id",
        {"customer_id": customer_id},
    )
    for edu in payload["education"]:
        cursor.execute(
            """
            INSERT INTO CUSTOMER_EDUCATION
              (CUSTOMER_ID, EDUCATION_LEVEL, START_DATE_EPOCH, END_DATE_EPOCH, SCHOOL, UPDATED_DATE_EPOCH)
            VALUES
              (:customer_id, :education_level, :start_date_epoch, :end_date_epoch, :school, :updated_date_epoch)
            """,
            edu,
        )

    cursor.execute(
        "DELETE FROM CUSTOMER_MARITAL_STATUSES WHERE CUSTOMER_ID = :customer_id",
        {"customer_id": customer_id},
    )
    for status in payload["marital_statuses"]:
        cursor.execute(
            """
            INSERT INTO CUSTOMER_MARITAL_STATUSES (CUSTOMER_ID, STATUS)
            VALUES (:customer_id, :status)
            """,
            status,
        )

    cursor.execute(
        "DELETE FROM CUSTOMER_LOAN_PRODUCTS WHERE CUSTOMER_ID = :customer_id",
        {"customer_id": customer_id},
    )
    for loan in payload["loan_products"]:
        cursor.execute(
            """
            INSERT INTO CUSTOMER_LOAN_PRODUCTS
              (CUSTOMER_ID, LOAN_PRODUCT_ID, LOAN_PRODUCT_CODE, LOAN_PRODUCT_NAME)
            VALUES
              (:customer_id, :loan_product_id, :loan_product_code, :loan_product_name)
            """,
            loan,
        )

    cursor.execute(
        "DELETE FROM CUSTOMER_BUSINESSES WHERE CUSTOMER_ID = :customer_id",
        {"customer_id": customer_id},
    )
    for business in payload["businesses"]:
        cursor.execute(
            """
            INSERT INTO CUSTOMER_BUSINESSES
              (BUSINESS_ID, CUSTOMER_ID, TIN, BUSINESS_LICENSE_NUMBER, TIN_OF_SPOUSE,
               BUSINESS_NAME, MANAGER_TIN_NUMBER, BUSINESS_SECTOR, ASSOCIATION_TYPE,
               FORM_OF_BUSINESS, LINE_OF_BUSINESS, BUSINESS_REGISTERED_DATE_EPOCH,
               NUMBER_OF_EMPLOYEES, STARTING_NUMBER_OF_EMPLOYEES, SOURCE_OF_INITIAL_CAPITAL,
               STARTING_CAPITAL, CURRENT_CAPITAL, ANNUAL_SALES_INCOME, ANNUAL_PROFIT,
               BUSINESS_RENEWAL_DATE_EPOCH, ADDITIONAL_INFO_JSON)
            VALUES
              (:business_id, :customer_id, :tin, :business_license_number, :tin_of_spouse,
               :business_name, :manager_tin_number, :business_sector, :association_type,
               :form_of_business, :line_of_business, :business_registered_date_epoch,
               :number_of_employees, :starting_number_of_employees, :source_of_initial_capital,
               :starting_capital, :current_capital, :annual_sales_income, :annual_profit,
               :business_renewal_date_epoch, :additional_info_json)
            """,
            business,
        )

    cursor.execute(
        "DELETE FROM BUSINESS_ADDRESSES WHERE BUSINESS_ID IN (SELECT BUSINESS_ID FROM CUSTOMER_BUSINESSES WHERE CUSTOMER_ID = :customer_id)",
        {"customer_id": customer_id},
    )
    for addr in payload["business_addresses"]:
        cursor.execute(
            """
            INSERT INTO BUSINESS_ADDRESSES
              (BUSINESS_ID, REGION, CITY, SUBCITY_ZONE, WOREDA, HOUSE_NUMBER, CREATED_DATE_EPOCH)
            VALUES
              (:business_id, :region, :city, :subcity_zone, :woreda, :house_number, :created_date_epoch)
            """,
            addr,
        )

    cursor.execute(
        "DELETE FROM CUSTOMER_DOCUMENTS WHERE CUSTOMER_ID = :customer_id",
        {"customer_id": customer_id},
    )
    for doc in payload["documents"]:
        cursor.execute(
            """
            INSERT INTO CUSTOMER_DOCUMENTS
              (DOCUMENT_ID, CUSTOMER_ID, DOC_TYPE, EXTENSION, URL, STATUS, UPLOADED_DATE)
            VALUES
              (:document_id, :customer_id, :doc_type, :extension, :url, :status, :uploaded_date)
            """,
            doc,
        )

    cursor.execute(
        "DELETE FROM CUSTOMER_BANK_ACCOUNTS WHERE CUSTOMER_ID = :customer_id",
        {"customer_id": customer_id},
    )
    for bank in payload["bank_accounts"]:
        bank_id_var = cursor.var(int)
        cursor.execute(
            """
            INSERT INTO CUSTOMER_BANK_ACCOUNTS
              (CUSTOMER_ID, ACCOUNT_NUMBER, EXISTS_FLAG, OWNER_VERIFIED, IS_PRIMARY, CUSTOMER_NUMBER)
            VALUES
              (:customer_id, :account_number, :exists_flag, :owner_verified, :is_primary, :customer_number)
            RETURNING ID INTO :bank_id
            """,
            {
                "customer_id": bank["customer_id"],
                "account_number": bank["account_number"],
                "exists_flag": bank["exists_flag"],
                "owner_verified": bank["owner_verified"],
                "is_primary": bank["is_primary"],
                "customer_number": bank["customer_number"],
                "bank_id": bank_id_var,
            },
        )
        bank_id = bank_id_var.getvalue()[0]
        additional_info = bank.get("additional_info") or {}
        if additional_info:
            cursor.execute(
                """
                INSERT INTO BANK_ACCOUNT_ADDITIONAL_INFO
                  (BANK_ACCOUNT_ID, ACCOUNT_NO, CUSTOMER_ID_REF, NAME, BRANCH_ID, STATUS, MOBILE,
                   ADDRESS, GENDER, DOB, OPENING_DATE)
                VALUES
                  (:bank_account_id, :account_no, :customer_id_ref, :name, :branch_id, :status,
                   :mobile, :address, :gender, :dob, :opening_date)
                """,
                {
                    "bank_account_id": bank_id,
                    "account_no": additional_info.get("accountNo"),
                    "customer_id_ref": additional_info.get("customerId"),
                    "name": additional_info.get("name"),
                    "branch_id": additional_info.get("branchId"),
                    "status": additional_info.get("status"),
                    "mobile": additional_info.get("mobile"),
                    "address": additional_info.get("address"),
                    "gender": additional_info.get("gender"),
                    "dob": additional_info.get("dob"),
                    "opening_date": _to_timestamp(additional_info.get("openingDate")),
                },
            )

    assigned = payload.get("assigned_officer")
    if assigned:
        officer_sql = _merge_sql(
            "OFFICERS",
            ["officer_id"],
            ["name"],
        )
        cursor.execute(officer_sql, assigned["officer"])

        assignment_sql = _merge_sql(
            "CUSTOMER_ASSIGNED_OFFICER",
            ["customer_id"],
            ["officer_id"],
        )
        cursor.execute(assignment_sql, assigned["assignment"])
    else:
        cursor.execute(
            "DELETE FROM CUSTOMER_ASSIGNED_OFFICER WHERE CUSTOMER_ID = :customer_id",
            {"customer_id": customer_id},
        )
