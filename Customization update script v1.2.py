import requests
import csv
import time
import numpy as np

if input("Search ALL GateDeliverableTracker files for project customization? Y/N (5-20mins) \n").strip().upper() == "Y":
    GATE_SEARCH=True
else:
    GATE_SEARCH=False

if input("Search ALL Historical KPI files for project customization? Y/N (5-20mins) \n").strip().upper() == "Y":  
    KPI_SEARCH=True
else:
    KPI_SEARCH=False

if KPI_SEARCH or GATE_SEARCH:
    pass
else:
    print("Done, no files updated")

    import sys
    sys.exit(0)  # Exit with success code





# =========================
# CONFIG
# =========================
TOKEN = input("Enter Smartsheet API Token:\n") 



DESTINATION_SHEET_NAME = "Customization_GateDeliverableTracker_Source"  # destination sheet
CSV_NAME = "powerbi_project_types.csv"

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}



# =========================
# GET ALL SOURCE SHEETS
# =========================
all_sheets = []
page = 1
page_size = 30000
#Actualy Get all sheets

while True:
    resp = requests.get(
        "https://api.smartsheet.com/2.0/sheets",
        headers=headers,
        params={"page": page, "pageSize": page_size}
    )
    
    if resp.status_code == 429:  # Rate limit
        retry_after = int(resp.headers.get("Retry-After", 5))
        print(f"Rate limit hit, retrying in {retry_after} seconds...")
        time.sleep(retry_after)
        continue
    
    resp.raise_for_status()
    data = resp.json()
    sheets = data.get("data", [])

    if not sheets:
        break

    all_sheets.extend(sheets)
    print(f"Fetched page {page}, total sheets so far: {len(all_sheets)}")

    if len(sheets) < page_size:
        break  # last page reached

    page += 1


all_sheets = {"data": all_sheets}
print(np.shape(all_sheets))
print(np.shape(all_sheets['data']))

sheets=all_sheets




# 
sheets_data = sheets['data']

# CSV filename
csv_filename = "Sheet_Names.csv"

# Write each sheet name as a row
try:
    
    with open(csv_filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Sheet_Name"])  # header
        for sheet in sheets_data:
            writer.writerow([sheet['name']])

    print(f"Wrote {len(sheets_data)} sheet names to {csv_filename}")
except:
    pass #if its open



#
# Find destination sheet ID
#
DESTINATION_SHEET_ID = None
for sheet in sheets['data']:
    nnnn=sheet["name"]
    #print(nnnn)
    if nnnn == DESTINATION_SHEET_NAME:
        DESTINATION_SHEET_ID = sheet["id"]
        break

print(f"destination sheet ID: {DESTINATION_SHEET_ID}",DESTINATION_SHEET_NAME)


# =========================
# GET DESTINATION COLUMN IDS
# =========================
dest_resp = requests.get(
    f"https://api.smartsheet.com/2.0/sheets/{DESTINATION_SHEET_ID}",
    headers=headers
)
dest_resp.raise_for_status()

dest_sheet = dest_resp.json()

column_map = {
    col["title"]: col["id"]
    for col in dest_sheet["columns"]
}

PROJECT_ID_COL = column_map["Project ID"]
PROJECT_TYPE_COL = column_map["Project_Type"]
SHEET_NAME_COL = column_map["sheet_name"]



if GATE_SEARCH:


    #
    # Find currently searched sheets
    #

    SHEET_NAME_COLS=[]
    existing_rows_resp = requests.get(        f"https://api.smartsheet.com/2.0/sheets/{DESTINATION_SHEET_ID}", headers=headers )
    existing_rows_resp.raise_for_status()
    dest_data = existing_rows_resp.json()

    # Find existing sheets
    for row in dest_data.get("rows", []):
        row_cells = {cell["columnId"]: cell.get("value") for cell in row.get("cells", [])}
        #Add all the sheet names to this list:
        SHEET_NAME_COLS.append(row_cells.get(SHEET_NAME_COL))
    print("Existing sheets",SHEET_NAME_COLS)

    # =========================
    # PROCESS SHEETS
    # =========================


    rows_to_append = []
    lmi = 0

    for sheet in sheets['data']:
        lmi += 1

        if lmi < 6240:
            continue

        sheet_name = sheet["name"]
        sheet_id = sheet["id"]

        name_lower = sheet_name.lower()
        # Check for gate files or check if the file is already appended to the list
        if not any(x in name_lower for x in [
            "gatedeliverabletracker",
            "gate deliverable tracker",
            "deliverable tracker",
            "deliverabletracker"
        ]) or sheet_name in SHEET_NAME_COLS:
            continue

        project_id_value = None
        project_type_value = None

        for _ in range(5):
            try:
                sheet_resp = requests.get(
                    f"https://api.smartsheet.com/2.0/sheets/{sheet_id}",
                    headers=headers
                )
                sheet_resp.raise_for_status()
                sheet_data = sheet_resp.json()
                break
            except Exception:
                time.sleep(1)
                sheet_data = None

        if not sheet_data:
            continue

        for row in sheet_data.get("rows", []):
            cells = row.get("cells", [])

            for i, cell in enumerate(cells):
                if cell.get("formula") == "=[Project ID]#":
                    project_id_value = cell.get("value")

                if cell.get("value") == "NPDI Process/Customization":
                    if i + 1 < len(cells):
                        project_type_value = cells[i + 1].get("displayValue")

            if project_id_value and project_type_value:
                break

        # =========================
        # PRINT WHEN FOUND
        # =========================
        if project_id_value and project_type_value:
            print(f"Sheet: {sheet_name} | Project_Type: {project_type_value}")

            rows_to_append.append({
                "toBottom": True,
                "cells": [
                    {"columnId": PROJECT_ID_COL, "value": project_id_value},
                    {"columnId": PROJECT_TYPE_COL, "value": project_type_value},
                    {"columnId": SHEET_NAME_COL, "value": sheet_name}
                ]
            })








    # =========================
    # APPEND ROWS (BATCH) WITH DUPLICATE CHECK
    # =========================

    # Step 1: Fetch existing rows from the destination Smartsheet
    existing_rows_resp = requests.get(
        f"https://api.smartsheet.com/2.0/sheets/{DESTINATION_SHEET_ID}",
        headers=headers
    )
    existing_rows_resp.raise_for_status()
    dest_data = existing_rows_resp.json()

    # Build a set of existing row keys (Project ID, Project_Type, sheet_name)
    existing_row_keys = set()
    for row in dest_data.get("rows", []):
        row_cells = {cell["columnId"]: cell.get("value") for cell in row.get("cells", [])}
        key = (
            row_cells.get(PROJECT_ID_COL),
            row_cells.get(PROJECT_TYPE_COL),
            row_cells.get(SHEET_NAME_COL)
        )
        existing_row_keys.add(key)

    # Step 2: Filter new rows to append
    filtered_rows = []
    for row in rows_to_append:
        project_id = next(c["value"] for c in row["cells"] if c["columnId"] == PROJECT_ID_COL)
        project_type = next(c["value"] for c in row["cells"] if c["columnId"] == PROJECT_TYPE_COL)
        sheet_name = next(c["value"] for c in row["cells"] if c["columnId"] == SHEET_NAME_COL)

        key = (project_id, project_type, sheet_name)

        if key not in existing_row_keys:
            filtered_rows.append(row)
            existing_row_keys.add(key)  # mark as existing

    # Step 3: Append filtered rows
    if filtered_rows:
        append_resp = requests.post(
            f"https://api.smartsheet.com/2.0/sheets/{DESTINATION_SHEET_ID}/rows",
            headers=headers,
            json=filtered_rows
        )
        append_resp.raise_for_status()
        print(f"Appended {len(filtered_rows)} new rows to destination Smartsheet")
    else:
        print("Checked",len(rows_to_append),"rows. They already exist")






#
#
#
#   Historical KPIS search
#
#
#

DESTINATION_SHEET_NAME = "Customization_KPIs_Source"  # destination sheet
CSV_NAME = "powerbi_project_types_2.csv"
#
# Find destination sheet ID
#
DESTINATION_SHEET_ID = None
for sheet in sheets['data']:
    nnnn=sheet["name"]
    #print(nnnn)
    if nnnn == DESTINATION_SHEET_NAME:
        DESTINATION_SHEET_ID = sheet["id"]
        break

print(f"destination sheet ID: {DESTINATION_SHEET_ID}",DESTINATION_SHEET_NAME)


# =========================
# GET DESTINATION COLUMN IDS
# =========================
dest_resp = requests.get(
    f"https://api.smartsheet.com/2.0/sheets/{DESTINATION_SHEET_ID}",
    headers=headers
)
dest_resp.raise_for_status()

dest_sheet = dest_resp.json()

column_map = {
    col["title"]: col["id"]
    for col in dest_sheet["columns"]
}

PROJECT_ID_COL = column_map["Project ID"]
PROJECT_TYPE_COL = column_map["Project_Type"]
SHEET_NAME_COL = column_map["sheet_name"]





if KPI_SEARCH:

    #
    # Find currently searched sheets
    #

    SHEET_NAME_COLS=[]
    existing_rows_resp = requests.get(        f"https://api.smartsheet.com/2.0/sheets/{DESTINATION_SHEET_ID}", headers=headers )
    existing_rows_resp.raise_for_status()
    dest_data = existing_rows_resp.json()

    # Find existing sheets
    for row in dest_data.get("rows", []):
        row_cells = {cell["columnId"]: cell.get("value") for cell in row.get("cells", [])}
        #Add all the sheet names to this list:
        SHEET_NAME_COLS.append(row_cells.get(SHEET_NAME_COL))
    print("Existing sheets",SHEET_NAME_COLS)


    # =========================
    # PROCESS SHEETS
    # =========================


    rows_to_append = []
    lmi = 0

    for sheet in sheets["data"]:
        lmi += 1

        if lmi < 43000:
            continue

        sheet_name = sheet["name"]

        # NAME FILTER / dupliucate checker
        if sheet_name.find("Historical Project KPI") == -1 or sheet_name in SHEET_NAME_COLS:
            continue

        sheet_id = sheet["id"]
        print(sheet_name, "Sheet:", lmi)

        project_id_value = None
        project_type_value = None

        # =========================
        # FETCH SHEET (WITH RETRY)
        # =========================
        for _ in range(5):
            try:
                sheet_resp = requests.get(
                    f"https://api.smartsheet.com/2.0/sheets/{sheet_id}",
                    headers=headers
                )
                sheet_resp.raise_for_status()
                sheet_data = sheet_resp.json()
                break
            except Exception:
                time.sleep(1)
                sheet_data = None

        if not sheet_data:
            continue

        # =========================
        # FIND COLUMN IDS
        # =========================
        customization_col_id = None
        project_id_col_id = None

        for col in sheet_data.get("columns", []):
            if col.get("title") == "Customization":
                customization_col_id = col.get("id")
            elif col.get("title") == "Project ID":
                project_id_col_id = col.get("id")

        if not customization_col_id or not project_id_col_id:
            continue

        # =========================
        # READ FIRST DATA ROW
        # =========================
        for row in sheet_data.get("rows", []):
            for cell in row.get("cells", []):
                if cell.get("columnId") == customization_col_id:
                    project_type_value = cell.get("displayValue")

                if cell.get("columnId") == project_id_col_id:
                    project_id_value = cell.get("displayValue")
            break  # only first row

        # =========================
        # PRINT + APPEND
        # =========================
        if project_id_value and project_type_value:
            print(
                f"FOUND → Sheet: {sheet_name} | "
                f"Project ID: {project_id_value} | "
                f"Project Type: {project_type_value}"
            )

            rows_to_append.append({
                "toBottom": True,
                "cells": [
                    {"columnId": PROJECT_ID_COL, "value": project_id_value},
                    {"columnId": PROJECT_TYPE_COL, "value": project_type_value},
                    {"columnId": SHEET_NAME_COL, "value": sheet_name}
                ]
            })








    # =========================
    # APPEND ROWS (BATCH) WITH DUPLICATE CHECK
    # =========================

    # Step 1: Fetch existing rows from the destination Smartsheet
    existing_rows_resp = requests.get(
        f"https://api.smartsheet.com/2.0/sheets/{DESTINATION_SHEET_ID}",
        headers=headers
    )
    existing_rows_resp.raise_for_status()
    dest_data = existing_rows_resp.json()

    # Build a set of existing row keys (Project ID, Project_Type, sheet_name)
    existing_row_keys = set()
    for row in dest_data.get("rows", []):
        row_cells = {cell["columnId"]: cell.get("value") for cell in row.get("cells", [])}
        key = (
            row_cells.get(PROJECT_ID_COL),
            row_cells.get(PROJECT_TYPE_COL),
            row_cells.get(SHEET_NAME_COL)
        )
        existing_row_keys.add(key)

    # Step 2: Filter new rows to append
    filtered_rows = []
    for row in rows_to_append:
        project_id = next(c["value"] for c in row["cells"] if c["columnId"] == PROJECT_ID_COL)
        project_type = next(c["value"] for c in row["cells"] if c["columnId"] == PROJECT_TYPE_COL)
        sheet_name = next(c["value"] for c in row["cells"] if c["columnId"] == SHEET_NAME_COL)

        key = (project_id, project_type, sheet_name)

        if key not in existing_row_keys:
            filtered_rows.append(row)
            existing_row_keys.add(key)  # mark as existing

    # Step 3: Append filtered rows
    if filtered_rows:
        append_resp = requests.post(
            f"https://api.smartsheet.com/2.0/sheets/{DESTINATION_SHEET_ID}/rows",
            headers=headers,
            json=filtered_rows
        )
        append_resp.raise_for_status()
        print(f"Appended {len(filtered_rows)} new rows to destination Smartsheet")
    else:
        print("Checked",len(rows_to_append),"rows. They already exist")
