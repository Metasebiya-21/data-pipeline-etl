# customer_etl.py — Detailed Walkthrough

This document explains `src/customer_etl.py` line by line. The module is responsible for:
- Converting a MongoDB customer document into structured Oracle rows,
- Performing inserts/merges into the `REPORT_ETL` schema tables.

## Source file
`src/customer_etl.py`

## Full code with line-by-line notes

```python
 1 | import json
 2 | import uuid
 3 | from datetime import datetime, date
 4 | from typing import Any, Dict, Iterable, List, Optional
```

- Lines 1–4: Standard library imports for JSON, UUID, time conversion, and type hints.

```python
 7 | def _bool_flag(value: Any) -> Optional[str]:
 8 |     if value is None:
 9 |         return None
10 |     return "Y" if bool(value) else "N"
```

- Lines 7–10: Converts a truthy value to Oracle `Y/N` flags, preserving `None` if missing.

```python
13 | def _uuid_to_raw(value: Optional[str]) -> Optional[bytes]:
14 |     if not value:
15 |         return None
16 |     try:
17 |         return uuid.UUID(str(value)).bytes
18 |     except (ValueError, AttributeError):
19 |         return None
```

- Lines 13–19: Converts UUID strings into 16‑byte RAW values for Oracle.

```python
22 | def _to_date(value: Optional[str]) -> Optional[date]:
23 |     if not value:
24 |         return None
25 |     try:
26 |         return date.fromisoformat(value)
27 |     except ValueError:
28 |         return None
```

- Lines 22–28: Converts ISO date strings (YYYY‑MM‑DD) into Python `date`.

```python
31 | def _to_timestamp(value: Optional[str]) -> Optional[datetime]:
32 |     if not value:
33 |         return None
34 |     try:
35 |         return datetime.fromisoformat(value)
36 |     except ValueError:
37 |         return None
```

- Lines 31–37: Converts ISO timestamps into Python `datetime` (Oracle TIMESTAMP).

```python
40 | def _json_dumps(value: Any) -> Optional[str]:
41 |     if value is None:
42 |         return None
43 |     return json.dumps(value, ensure_ascii=True, separators=(",", ":"))
```

- Lines 40–43: Serializes JSON to a compact string for CLOB storage.

```python
46 | def build_customer_payload(doc: Dict[str, Any]) -> Dict[str, Any]:
47 |     customer_id = str(doc.get("_id"))
```

- Lines 46–47: Entrypoint to transform a Mongo document; `_id` becomes the Oracle PK.

```python
49 |     customer = {
50 |         "customer_id": customer_id,
51 |         "phone": doc.get("phone"),
52 |         "password_hash": doc.get("password"),
53 |         "device_token": doc.get("deviceToken"),
54 |         "birth_date_epoch": doc.get("birthDate"),
55 |         "full_name": doc.get("fullName"),
56 |         "gender": doc.get("gender"),
57 |         "id_number": doc.get("idNumber"),
58 |         "id_type": doc.get("idType"),
59 |         "tin_number": doc.get("tinNumber"),
60 |         "emergency_contact_name": doc.get("emergencyContactName"),
61 |         "emergency_contact_phone": doc.get("emergencyContactPhone"),
62 |         "request_id": _uuid_to_raw(doc.get("requestId")),
63 |         "created_date_epoch": doc.get("createdDate"),
64 |         "updated_date_epoch": doc.get("updatedDate"),
65 |     }
```

- Lines 49–65: Primary customer table mapping (`CUSTOMERS`).

```python
67 |     verification = doc.get("verification") or {}
68 |     verification_row = {
69 |         "customer_id": customer_id,
70 |         "phone_verified": _bool_flag(verification.get("phone")) or "N",
71 |     }
```

- Lines 67–71: Customer verification (`CUSTOMER_VERIFICATION`).

```python
73 |     account_info = doc.get("userAccountInformation") or {}
74 |     account_row = {
75 |         "customer_id": customer_id,
76 |         "preferred_language": account_info.get("preferredLanguage"),
77 |         "status": account_info.get("status"),
78 |         "current_stage": account_info.get("currentStage"),
79 |         "loan_status": account_info.get("loanStatus"),
80 |         "loan_product_code": account_info.get("loanProductCode"),
81 |     }
```

- Lines 73–81: Account info mapping (`CUSTOMER_ACCOUNT_INFO`).

```python
83 |     addresses = []
84 |     for addr in doc.get("personalAddresses") or []:
85 |         addresses.append(
86 |             {
87 |                 "customer_id": customer_id,
88 |                 "region": addr.get("region"),
89 |                 "city": addr.get("city"),
90 |                 "subcity_zone": addr.get("subcityZone"),
91 |                 "woreda": addr.get("woreda"),
92 |                 "house_number": addr.get("houseNumber"),
93 |                 "created_date_epoch": addr.get("createdDate"),
94 |             }
95 |         )
```

- Lines 83–95: Personal addresses (`CUSTOMER_PERSONAL_ADDRESSES`).

```python
97 |     bank_accounts = []
98 |     for bank in doc.get("bankInformations") or []:
99 |         bank_accounts.append(
100|             {
101|                 "customer_id": customer_id,
102|                 "account_number": bank.get("accountNumber"),
103|                 "exists_flag": _bool_flag(bank.get("exists")),
104|                 "owner_verified": _bool_flag(bank.get("ownerVerified")),
105|                 "is_primary": _bool_flag(bank.get("isPrimary")),
106|                 "customer_number": bank.get("customerNumber"),
107|                 "additional_info": bank.get("additionalInformation"),
108|             }
109|         )
```

- Lines 97–109: Bank account rows plus additional info payload (for a related table).

```python
111|     education = []
112|     for edu in doc.get("educationalLevels") or []:
113|         education.append(
114|             {
115|                 "customer_id": customer_id,
116|                 "education_level": edu.get("level"),
117|                 "start_date_epoch": edu.get("startDate"),
118|                 "end_date_epoch": edu.get("endDate"),
119|                 "school": edu.get("school"),
120|                 "updated_date_epoch": edu.get("updatedDate"),
121|             }
122|         )
```

- Lines 111–122: Educational levels (`CUSTOMER_EDUCATION`).

```python
124|     marital_statuses = []
125|     for status in doc.get("maritalStatuses") or []:
126|         if status:
127|             marital_statuses.append({"customer_id": customer_id, "status": status})
```

- Lines 124–127: Marital statuses (`CUSTOMER_MARITAL_STATUSES`).

```python
129|     loan_products = []
130|     for loan in doc.get("loanInformations") or []:
131|         loan_products.append(
132|             {
133|                 "customer_id": customer_id,
134|                 "loan_product_id": _uuid_to_raw(loan.get("loanProductId")),
135|                 "loan_product_code": loan.get("loanProductCode"),
136|                 "loan_product_name": loan.get("loanProductName"),
137|             }
138|         )
```

- Lines 129–138: Loan products (`CUSTOMER_LOAN_PRODUCTS`).

```python
140|     businesses = []
141|     business_addresses = []
142|     for business in doc.get("businesses") or []:
143|         business_id = business.get("businessId")
144|         if not business_id:
145|             continue
146|         businesses.append(
147|             {
148|                 "business_id": business_id,
149|                 "customer_id": customer_id,
150|                 "tin": business.get("tin"),
151|                 "business_license_number": business.get("businessLicenseNumber"),
152|                 "tin_of_spouse": business.get("tinOfSpouse"),
153|                 "business_name": business.get("businessName"),
154|                 "manager_tin_number": business.get("managerTinNumber"),
155|                 "business_sector": business.get("businessSector"),
156|                 "association_type": business.get("associationType"),
157|                 "form_of_business": business.get("formOfBusiness"),
158|                 "line_of_business": business.get("lineOfBusiness"),
159|                 "business_registered_date_epoch": business.get("businessRegisteredDate"),
160|                 "number_of_employees": business.get("numberOfEmployees"),
161|                 "starting_number_of_employees": business.get("startingNumberOfEmployees"),
162|                 "source_of_initial_capital": business.get("sourceOfInitialCapital"),
163|                 "starting_capital": business.get("startingCapital"),
164|                 "current_capital": business.get("currentCapital"),
165|                 "annual_sales_income": business.get("annualSalesIncome"),
166|                 "annual_profit": business.get("annualProfit"),
167|                 "business_renewal_date_epoch": business.get("businessRenewalDate"),
168|                 "additional_info_json": _json_dumps(business.get("additionalInformation")),
169|             }
170|         )
```

- Lines 140–170: Business rows (`CUSTOMER_BUSINESSES`).
- `additional_info_json` stores a JSON payload in a CLOB column.

```python
172|         for addr in business.get("businessAddresses") or []:
173|             business_addresses.append(
174|                 {
175|                     "business_id": business_id,
176|                     "region": addr.get("region"),
177|                     "city": addr.get("city"),
178|                     "subcity_zone": addr.get("subcityZone"),
179|                     "woreda": addr.get("woreda"),
180|                     "house_number": addr.get("houseNumber"),
181|                     "created_date_epoch": addr.get("createdDate"),
182|                 }
183|             )
```

- Lines 172–183: Business addresses (`BUSINESS_ADDRESSES`).

```python
185|     documents = []
186|     for doc_item in doc.get("documents") or []:
187|         document_id = doc_item.get("id")
188|         if not document_id:
189|             continue
190|         documents.append(
191|             {
192|                 "document_id": document_id,
193|                 "customer_id": customer_id,
194|                 "doc_type": doc_item.get("type"),
195|                 "extension": doc_item.get("extension"),
196|                 "url": doc_item.get("url"),
197|                 "status": doc_item.get("status"),
198|                 "uploaded_date": _to_date(doc_item.get("uploadedDate")),
199|             }
200|         )
```

- Lines 185–200: Customer documents (`CUSTOMER_DOCUMENTS`).

```python
202|     assigned_officer = None
203|     officer = doc.get("assignedOfficer")
204|     if officer and officer.get("id"):
205|         assigned_officer = {
206|             "officer": {"officer_id": officer.get("id"), "name": officer.get("name")},
207|             "assignment": {"customer_id": customer_id, "officer_id": officer.get("id")},
208|         }
```

- Lines 202–208: Officer and assignment rows (`OFFICERS`, `CUSTOMER_ASSIGNED_OFFICER`).

```python
210|     return {
211|         "customer": customer,
212|         "verification": verification_row,
213|         "account_info": account_row,
214|         "addresses": addresses,
215|         "bank_accounts": bank_accounts,
216|         "education": education,
217|         "marital_statuses": marital_statuses,
218|         "loan_products": loan_products,
219|         "businesses": businesses,
220|         "business_addresses": business_addresses,
221|         "documents": documents,
222|         "assigned_officer": assigned_officer,
223|     }
```

- Lines 210–223: Returns a structured payload consumed by `load_customer()`.

```python
226| def _merge_sql(table: str, key_cols: Iterable[str], set_cols: Iterable[str]) -> str:
227|     key_on = " AND ".join([f"t.{col} = s.{col}" for col in key_cols])
228|     update_set = ", ".join([f"t.{col} = s.{col}" for col in set_cols])
229|     insert_cols = ", ".join(list(key_cols) + list(set_cols))
230|     insert_vals = ", ".join([f"s.{col}" for col in list(key_cols) + list(set_cols)])
231|     select_cols = ", ".join([f":{col} AS {col}" for col in list(key_cols) + list(set_cols)])
232|
233|     return f"""
234|         MERGE INTO {table} t
235|         USING (
236|             SELECT {select_cols}
237|             FROM dual
238|         ) s
239|         ON ({key_on})
240|         WHEN MATCHED THEN UPDATE SET {update_set}
241|         WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
242|     """
```

- Lines 226–242: Generates a parameterized Oracle MERGE statement for upserts.

```python
245| def load_customer(cursor, payload: Dict[str, Any]) -> None:
246|     customer = payload["customer"]
247|     customer_id = customer["customer_id"]
```

- Lines 245–247: Entry point for writing a single customer to Oracle.

```python
249|     customer_sql = _merge_sql(
250|         "CUSTOMERS",
251|         ["customer_id"],
252|         [
253|             "phone",
254|             "password_hash",
255|             "device_token",
256|             "birth_date_epoch",
257|             "full_name",
258|             "gender",
259|             "id_number",
260|             "id_type",
261|             "tin_number",
262|             "emergency_contact_name",
263|             "emergency_contact_phone",
264|             "request_id",
265|             "created_date_epoch",
266|             "updated_date_epoch",
267|         ],
268|     )
269|     cursor.execute(customer_sql, customer)
```

- Lines 249–269: Upsert into `CUSTOMERS` table.

```python
271|     verification_sql = _merge_sql(
272|         "CUSTOMER_VERIFICATION",
273|         ["customer_id"],
274|         ["phone_verified"],
275|     )
276|     cursor.execute(verification_sql, payload["verification"])
```

- Lines 271–276: Upsert into `CUSTOMER_VERIFICATION`.

```python
278|     account_sql = _merge_sql(
279|         "CUSTOMER_ACCOUNT_INFO",
280|         ["customer_id"],
281|         [
282|             "preferred_language",
283|             "status",
284|             "current_stage",
285|             "loan_status",
286|             "loan_product_code",
287|         ],
288|     )
289|     cursor.execute(account_sql, payload["account_info"])
```

- Lines 278–289: Upsert into `CUSTOMER_ACCOUNT_INFO`.

```python
291|     cursor.execute(
292|         "DELETE FROM CUSTOMER_PERSONAL_ADDRESSES WHERE CUSTOMER_ID = :customer_id",
293|         {"customer_id": customer_id},
294|     )
295|     for addr in payload["addresses"]:
296|         cursor.execute(
297|             """
298|             INSERT INTO CUSTOMER_PERSONAL_ADDRESSES
299|               (CUSTOMER_ID, REGION, CITY, SUBCITY_ZONE, WOREDA, HOUSE_NUMBER, CREATED_DATE_EPOCH)
300|             VALUES
301|               (:customer_id, :region, :city, :subcity_zone, :woreda, :house_number, :created_date_epoch)
302|             """,
303|             addr,
304|         )
```

- Lines 291–304: Replace personal addresses by deleting and re-inserting.

```python
306|     cursor.execute(
307|         "DELETE FROM CUSTOMER_EDUCATION WHERE CUSTOMER_ID = :customer_id",
308|         {"customer_id": customer_id},
309|     )
310|     for edu in payload["education"]:
311|         cursor.execute(
312|             """
313|             INSERT INTO CUSTOMER_EDUCATION
314|               (CUSTOMER_ID, EDUCATION_LEVEL, START_DATE_EPOCH, END_DATE_EPOCH, SCHOOL, UPDATED_DATE_EPOCH)
315|             VALUES
316|               (:customer_id, :education_level, :start_date_epoch, :end_date_epoch, :school, :updated_date_epoch)
317|             """,
318|             edu,
319|         )
```

- Lines 306–319: Replace education records.

```python
321|     cursor.execute(
322|         "DELETE FROM CUSTOMER_MARITAL_STATUSES WHERE CUSTOMER_ID = :customer_id",
323|         {"customer_id": customer_id},
324|     )
325|     for status in payload["marital_statuses"]:
326|         cursor.execute(
327|             """
328|             INSERT INTO CUSTOMER_MARITAL_STATUSES (CUSTOMER_ID, STATUS)
329|             VALUES (:customer_id, :status)
330|             """,
331|             status,
332|         )
```

- Lines 321–332: Replace marital status entries.

```python
334|     cursor.execute(
335|         "DELETE FROM CUSTOMER_LOAN_PRODUCTS WHERE CUSTOMER_ID = :customer_id",
336|         {"customer_id": customer_id},
337|     )
338|     for loan in payload["loan_products"]:
339|         cursor.execute(
340|             """
341|             INSERT INTO CUSTOMER_LOAN_PRODUCTS
342|               (CUSTOMER_ID, LOAN_PRODUCT_ID, LOAN_PRODUCT_CODE, LOAN_PRODUCT_NAME)
343|             VALUES
344|               (:customer_id, :loan_product_id, :loan_product_code, :loan_product_name)
345|             """,
346|             loan,
347|         )
```

- Lines 334–347: Replace loan product rows.

```python
349|     cursor.execute(
350|         "DELETE FROM CUSTOMER_BUSINESSES WHERE CUSTOMER_ID = :customer_id",
351|         {"customer_id": customer_id},
352|     )
353|     for business in payload["businesses"]:
354|         cursor.execute(
355|             """
356|             INSERT INTO CUSTOMER_BUSINESSES
357|               (BUSINESS_ID, CUSTOMER_ID, TIN, BUSINESS_LICENSE_NUMBER, TIN_OF_SPOUSE,
358|                BUSINESS_NAME, MANAGER_TIN_NUMBER, BUSINESS_SECTOR, ASSOCIATION_TYPE,
359|                FORM_OF_BUSINESS, LINE_OF_BUSINESS, BUSINESS_REGISTERED_DATE_EPOCH,
360|                NUMBER_OF_EMPLOYEES, STARTING_NUMBER_OF_EMPLOYEES, SOURCE_OF_INITIAL_CAPITAL,
361|                STARTING_CAPITAL, CURRENT_CAPITAL, ANNUAL_SALES_INCOME, ANNUAL_PROFIT,
362|                BUSINESS_RENEWAL_DATE_EPOCH, ADDITIONAL_INFO_JSON)
363|             VALUES
364|               (:business_id, :customer_id, :tin, :business_license_number, :tin_of_spouse,
365|                :business_name, :manager_tin_number, :business_sector, :association_type,
366|                :form_of_business, :line_of_business, :business_registered_date_epoch,
367|                :number_of_employees, :starting_number_of_employees, :source_of_initial_capital,
368|                :starting_capital, :current_capital, :annual_sales_income, :annual_profit,
369|                :business_renewal_date_epoch, :additional_info_json)
370|             """,
371|             business,
372|         )
```

- Lines 349–372: Replace customer businesses.

```python
374|     cursor.execute(
375|         "DELETE FROM BUSINESS_ADDRESSES WHERE BUSINESS_ID IN (SELECT BUSINESS_ID FROM CUSTOMER_BUSINESSES WHERE CUSTOMER_ID = :customer_id)",
376|         {"customer_id": customer_id},
377|     )
378|     for addr in payload["business_addresses"]:
379|         cursor.execute(
380|             """
381|             INSERT INTO BUSINESS_ADDRESSES
382|               (BUSINESS_ID, REGION, CITY, SUBCITY_ZONE, WOREDA, HOUSE_NUMBER, CREATED_DATE_EPOCH)
383|             VALUES
384|               (:business_id, :region, :city, :subcity_zone, :woreda, :house_number, :created_date_epoch)
385|             """,
386|             addr,
387|         )
```

- Lines 374–387: Replace business addresses.

```python
389|     cursor.execute(
390|         "DELETE FROM CUSTOMER_DOCUMENTS WHERE CUSTOMER_ID = :customer_id",
391|         {"customer_id": customer_id},
392|     )
393|     for doc in payload["documents"]:
394|         cursor.execute(
395|             """
396|             INSERT INTO CUSTOMER_DOCUMENTS
397|               (DOCUMENT_ID, CUSTOMER_ID, DOC_TYPE, EXTENSION, URL, STATUS, UPLOADED_DATE)
398|             VALUES
399|               (:document_id, :customer_id, :doc_type, :extension, :url, :status, :uploaded_date)
400|             """,
401|             doc,
402|         )
```

- Lines 389–402: Replace customer documents.

```python
404|     cursor.execute(
405|         "DELETE FROM CUSTOMER_BANK_ACCOUNTS WHERE CUSTOMER_ID = :customer_id",
406|         {"customer_id": customer_id},
407|     )
408|     for bank in payload["bank_accounts"]:
409|         bank_id_var = cursor.var(int)
410|         cursor.execute(
411|             """
412|             INSERT INTO CUSTOMER_BANK_ACCOUNTS
413|               (CUSTOMER_ID, ACCOUNT_NUMBER, EXISTS_FLAG, OWNER_VERIFIED, IS_PRIMARY, CUSTOMER_NUMBER)
414|             VALUES
415|               (:customer_id, :account_number, :exists_flag, :owner_verified, :is_primary, :customer_number)
416|             RETURNING ID INTO :bank_id
417|             """,
418|             {
419|                 "customer_id": bank["customer_id"],
420|                 "account_number": bank["account_number"],
421|                 "exists_flag": bank["exists_flag"],
422|                 "owner_verified": bank["owner_verified"],
423|                 "is_primary": bank["is_primary"],
424|                 "customer_number": bank["customer_number"],
425|                 "bank_id": bank_id_var,
426|             },
427|         )
428|         bank_id = bank_id_var.getvalue()[0]
429|         additional_info = bank.get("additional_info") or {}
430|         if additional_info:
431|             cursor.execute(
432|                 """
433|                 INSERT INTO BANK_ACCOUNT_ADDITIONAL_INFO
434|                   (BANK_ACCOUNT_ID, ACCOUNT_NO, CUSTOMER_ID_REF, NAME, BRANCH_ID, STATUS, MOBILE,
435|                    ADDRESS, GENDER, DOB, OPENING_DATE)
436|                 VALUES
437|                   (:bank_account_id, :account_no, :customer_id_ref, :name, :branch_id, :status,
438|                    :mobile, :address, :gender, :dob, :opening_date)
439|                 """,
440|                 {
441|                     "bank_account_id": bank_id,
442|                     "account_no": additional_info.get("accountNo"),
443|                     "customer_id_ref": additional_info.get("customerId"),
444|                     "name": additional_info.get("name"),
445|                     "branch_id": additional_info.get("branchId"),
446|                     "status": additional_info.get("status"),
447|                     "mobile": additional_info.get("mobile"),
448|                     "address": additional_info.get("address"),
449|                     "gender": additional_info.get("gender"),
450|                     "dob": additional_info.get("dob"),
451|                     "opening_date": _to_timestamp(additional_info.get("openingDate")),
452|                 },
453|             )
```

- Lines 404–453: Replace bank accounts and insert related “additional info” rows using the generated bank ID.

```python
455|     assigned = payload.get("assigned_officer")
456|     if assigned:
457|         officer_sql = _merge_sql(
458|             "OFFICERS",
459|             ["officer_id"],
460|             ["name"],
461|         )
462|         cursor.execute(officer_sql, assigned["officer"])
463|
464|         assignment_sql = _merge_sql(
465|             "CUSTOMER_ASSIGNED_OFFICER",
466|             ["customer_id"],
467|             ["officer_id"],
468|         )
469|         cursor.execute(assignment_sql, assigned["assignment"])
470|     else:
471|         cursor.execute(
472|             "DELETE FROM CUSTOMER_ASSIGNED_OFFICER WHERE CUSTOMER_ID = :customer_id",
473|             {"customer_id": customer_id},
474|         )
```

- Lines 455–474: Upsert officer and assignment, or delete assignment if none.

## Behavior summary

- `build_customer_payload()` takes one Mongo document and builds normalized rows for all tables.
- `load_customer()` applies the payload to Oracle using a mix of `MERGE` and “delete + insert” strategies.
- Lists (addresses, education, documents, etc.) are replaced entirely to keep them consistent.

## Notes

- If you add new fields in Mongo, extend `build_customer_payload()` and the corresponding SQL blocks.
- For very large datasets, consider batching commits or using `executemany` for the insert-heavy sections.
