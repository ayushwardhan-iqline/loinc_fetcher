import requests
import pandas as pd
import time
import os
import json
import re
from requests.auth import HTTPBasicAuth

# --- Configuration ---
LOINC_USERNAME = os.getenv("LOINC_USERNAME")
LOINC_PASSWORD = os.getenv("LOINC_PASSWORD")
INPUT_CSV = "test_to_param_mapping.csv"
OUTPUT_EXCEL = "loinc_mapping_results_with_lcn.xlsx" # Changed output filename

# API Endpoints
LOINC_SEARCH_API = "https://loinc.regenstrief.org/searchapi/loincs"
LOINC_FHIR_QUESTIONNAIRE_API = "https://fhir.loinc.org/Questionnaire/" # Query params added later

# Headers for requests
HEADERS = {
    'User-Agent': 'LIMSMappingScript/2.1 (Contact: ayush.wardhan@iqline.co.in)', # Version bump
    'Accept': 'application/json'
}
FHIR_HEADERS = {
    'User-Agent': 'LIMSMappingScript/2.1 (Contact: ayush.wardhan@iqline.co.in)',
    'Accept': 'application/fhir+json'
}


# --- Pre-filtering Configuration for LOINC Search API ---
ENABLE_PRE_FILTERING = True
FILTER_ON_STATUS = True
FILTER_STATUS_KEEP = 'ACTIVE'
FILTER_ON_CLASSTYPE = False # As per your settings
FILTER_CLASSTYPE_KEEP = 1 # 1=Lab (Ignored if FILTER_ON_CLASSTYPE is False)
FILTER_ON_SCALE = False # As per your settings
FILTER_SCALE_EXCLUDE = 'Doc' # (Ignored if FILTER_ON_SCALE is False)
# --- End Filter Criteria ---

# --- Helper Function to Clean Sheet Names ---
def clean_sheet_name(name):
    """Removes invalid characters and truncates name for Excel sheet names."""
    name = re.sub(r'[\\/*?:\[\]]', '_', name)
    return name[:31] # Excel limit is 31

# --- Helper Function to Fetch LOINC Test Codes ---
# (Identical to your provided function - no changes needed here)
def search_loinc_tests(term, auth, headers, max_retries=2, initial_delay=1):
    """Searches the LOINC Search API for a given term and applies filters."""
    print(f"  Searching LOINC for test term: '{term}'")
    results_list = []
    retry_count = 0
    delay = initial_delay

    while retry_count <= max_retries:
        try:
            response = requests.get(
                LOINC_SEARCH_API,
                params={"query": term},
                auth=auth,
                headers=headers,
                timeout=45
            )
            response.raise_for_status()
            data = response.json()
            loinc_results = data.get("Results", [])
            results_found_total = len(loinc_results)
            results_kept_count = 0

            if not loinc_results:
                print(f"    -> No LOINC results found.")
                return []

            for i, hit in enumerate(loinc_results):
                loinc_num = hit.get("LOINC_NUM", "Parse Error")
                loinc_url = f"https://loinc.org/{loinc_num}" if loinc_num != "Parse Error" else "N/A"
                scale_type = hit.get("SCALE_TYP", "N/A")

                result_entry = {
                    "search_term": term,
                    "match_rank": i + 1,
                    "loinc_test_code": loinc_num,
                    "loinc_test_long_name": hit.get("LONG_COMMON_NAME", "N/A"),
                    "loinc_test_status": hit.get("STATUS", "N/A"),
                    "loinc_test_class_type": hit.get("CLASSTYPE", None),
                    "loinc_test_component": hit.get("COMPONENT", "N/A"),
                    "loinc_test_property": hit.get("PROPERTY", "N/A"),
                    "loinc_test_time": hit.get("TIME_ASPCT", "N/A"),
                    "loinc_test_system": hit.get("SYSTEM", "N/A"),
                    "loinc_test_scale": scale_type,
                    "loinc_test_method": hit.get("METHOD_TYP", "N/A"),
                    "loinc_test_class": hit.get("CLASS", "N/A"),
                    "loinc_test_short_name": hit.get("SHORTNAME", "N/A"),
                    "loinc_test_url": loinc_url
                }

                passes_filter = True
                if ENABLE_PRE_FILTERING:
                    if FILTER_ON_STATUS and result_entry['loinc_test_status'] != FILTER_STATUS_KEEP:
                        passes_filter = False
                    if passes_filter and FILTER_ON_CLASSTYPE and result_entry['loinc_test_class_type'] != FILTER_CLASSTYPE_KEEP:
                        passes_filter = False
                    if passes_filter and FILTER_ON_SCALE and result_entry['loinc_test_scale'] == FILTER_SCALE_EXCLUDE:
                        passes_filter = False

                if passes_filter:
                    results_list.append(result_entry)
                    results_kept_count += 1

            print(f"    -> Found {results_found_total} results. Kept {results_kept_count} after filtering.")
            return results_list

        except requests.exceptions.HTTPError as http_err:
            print(f"    -> HTTP error on search API: {http_err} (Status: {response.status_code})")
            if response.status_code == 429 or 500 <= response.status_code < 600:
                print(f"    -> Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2
                retry_count += 1
            else:
                 print(f"    -> Unrecoverable HTTP error for term '{term}'. Skipping.")
                 return []
        except requests.exceptions.RequestException as req_err:
             print(f"    -> Request error on search API for term '{term}': {req_err}. Retrying in {delay} seconds...")
             time.sleep(delay)
             delay *= 2
             retry_count += 1
        except json.JSONDecodeError as json_err:
             print(f"    -> JSON decoding error on search API for term '{term}': {json_err}. Response text: {response.text[:200]}... Skipping.")
             return []
        except Exception as e:
            print(f"    -> Unexpected error during LOINC search for '{term}': {e}. Skipping.")
            return []

        time.sleep(0.2)

    print(f"    -> Failed to get results for term '{term}' after {max_retries + 1} attempts.")
    return []

# --- Helper Function to Fetch LOINC Panel Parameter Codes via FHIR API ---
# (Modified slightly to *only* return codes or an error string)
def get_loinc_parameter_codes_from_fhir(loinc_panel_code, auth, headers, max_retries=2, initial_delay=1):
    """Fetches panel member codes for a given LOINC code using the FHIR Questionnaire API.
       Returns a list of codes or an error/status string."""
    print(f"      Fetching FHIR Questionnaire for LOINC Panel: {loinc_panel_code}")
    param_codes = []
    retry_count = 0
    delay = initial_delay

    if not loinc_panel_code or loinc_panel_code == "Parse Error":
        print("      -> Invalid LOINC panel code provided. Skipping FHIR search.")
        return "Invalid LOINC Code"

    params = {"url": f"http://loinc.org/q/{loinc_panel_code}"}

    while retry_count <= max_retries:
        try:
            response = requests.get(
                LOINC_FHIR_QUESTIONNAIRE_API,
                params=params,
                auth=auth,
                headers=headers,
                timeout=45
            )
            response.raise_for_status()
            data = response.json()

            if data.get("total", 0) > 0 and data.get("entry"):
                questionnaire_resource = data["entry"][0].get("resource")
                if questionnaire_resource and questionnaire_resource.get("resourceType") == "Questionnaire":
                    items = questionnaire_resource.get("item", [])
                    if not items:
                         print(f"      -> Questionnaire found for {loinc_panel_code}, but contains no 'item' elements (parameters).")
                         return "No Params Found" # Special string indicating success but no items

                    for item in items:
                        code_info = item.get("code", [{}])[0]
                        code = code_info.get("code", "No Code")
                        param_codes.append(code)

                    print(f"      -> Found {len(param_codes)} parameter codes via FHIR.")
                    return param_codes # Return the list of codes
                else:
                    print(f"      -> FHIR response for {loinc_panel_code} does not contain a valid Questionnaire resource.")
                    return "FHIR Resource Error"
            else:
                print(f"      -> No FHIR Questionnaire found for LOINC panel code: {loinc_panel_code}")
                return "FHIR Not Found"

        except requests.exceptions.HTTPError as http_err:
            print(f"      -> HTTP error on FHIR API for {loinc_panel_code}: {http_err} (Status: {response.status_code})")
            if response.status_code == 429 or 500 <= response.status_code < 600:
                print(f"      -> Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2
                retry_count += 1
            else:
                print(f"      -> Unrecoverable FHIR HTTP error for {loinc_panel_code}.")
                return f"FHIR HTTP Error {response.status_code}"
        except requests.exceptions.RequestException as req_err:
            print(f"      -> Request error on FHIR API for {loinc_panel_code}: {req_err}. Retrying...")
            time.sleep(delay)
            delay *= 2
            retry_count += 1
        except json.JSONDecodeError as json_err:
            print(f"      -> JSON decoding error on FHIR API for {loinc_panel_code}: {json_err}. Response: {response.text[:200]}...")
            return "FHIR JSON Error"
        except Exception as e:
            print(f"      -> Unexpected error during FHIR fetch for {loinc_panel_code}: {e}.")
            return "FHIR Unexpected Error"

        time.sleep(0.2)

    print(f"      -> Failed to get FHIR results for {loinc_panel_code} after {max_retries + 1} attempts.")
    return "FHIR Fetch Failed"

# --- NEW Helper Function to Get Long Common Name for a Specific LOINC Code ---
def get_long_common_name_for_code(loinc_code, auth, headers, max_retries=2, initial_delay=1):
    """Fetches the Long Common Name for a specific LOINC code using the search API."""
    print(f"        Fetching LCN for parameter code: {loinc_code}")

    if not loinc_code or loinc_code in ["Parse Error", "No Code"]:
        print(f"        -> Invalid or missing code ('{loinc_code}'). Cannot fetch LCN.")
        return f"{loinc_code} (LCN N/A)" # Return original code with note

    retry_count = 0
    delay = initial_delay

    while retry_count <= max_retries:
        try:
            # Search specifically for the LOINC code
            response = requests.get(
                LOINC_SEARCH_API,
                params={"query": f'"{loinc_code}"'}, # Exact match search if possible
                auth=auth,
                headers=headers,
                timeout=30 # Can likely use shorter timeout for code lookup
            )
            response.raise_for_status()
            data = response.json()
            loinc_results = data.get("Results", [])

            if not loinc_results:
                print(f"        -> No search results found for code {loinc_code}.")
                return f"{loinc_code} (LCN Not Found)"

            # Find the result that exactly matches the requested LOINC code
            for hit in loinc_results:
                if hit.get("LOINC_NUM") == loinc_code:
                    lcn = hit.get("LONG_COMMON_NAME", f"{loinc_code} (LCN Missing in Record)")
                    print(f"        -> Found LCN: {lcn[:50]}...") # Print truncated LCN
                    return lcn # Return the found Long Common Name

            # If loop finishes without finding an exact match (should be rare when searching by code)
            print(f"        -> Search results found, but none matched code {loinc_code} exactly.")
            return f"{loinc_code} (LCN Not Found - No Exact Match)"

        except requests.exceptions.HTTPError as http_err:
            print(f"        -> HTTP error fetching LCN for {loinc_code}: {http_err} (Status: {response.status_code})")
            if response.status_code == 429 or 500 <= response.status_code < 600:
                 print(f"        -> Retrying in {delay} seconds...")
                 time.sleep(delay)
                 delay *= 2
                 retry_count += 1
            else:
                 print(f"        -> Unrecoverable HTTP error for code {loinc_code}. Cannot get LCN.")
                 return f"{loinc_code} (LCN HTTP Error)"
        except requests.exceptions.RequestException as req_err:
             print(f"        -> Request error fetching LCN for {loinc_code}: {req_err}. Retrying...")
             time.sleep(delay)
             delay *= 2
             retry_count += 1
        except json.JSONDecodeError as json_err:
             print(f"        -> JSON error fetching LCN for {loinc_code}: {json_err}.")
             return f"{loinc_code} (LCN JSON Error)"
        except Exception as e:
            print(f"        -> Unexpected error fetching LCN for {loinc_code}: {e}.")
            return f"{loinc_code} (LCN Unexpected Error)"

        time.sleep(0.1) # Shorter delay between LCN lookups is probably fine

    print(f"        -> Failed to get LCN for code {loinc_code} after {max_retries + 1} attempts.")
    return f"{loinc_code} (LCN Fetch Failed)"


# --- Main Execution ---
if __name__ == "__main__":
    if not LOINC_USERNAME or not LOINC_PASSWORD:
        print("ERROR: LOINC_USERNAME and LOINC_PASSWORD environment variables must be set.")
        exit(1)

    loinc_auth = HTTPBasicAuth(LOINC_USERNAME, LOINC_PASSWORD)

    # --- 1. Read and Process Input CSV ---
    print(f"Reading input file: {INPUT_CSV}")
    try:
        input_df = pd.read_csv(INPUT_CSV, dtype=str)
        input_df.fillna('', inplace=True)
    except FileNotFoundError:
        print(f"ERROR: Input file not found: {INPUT_CSV}")
        exit(1)
    except Exception as e:
        print(f"ERROR: Failed to read input CSV: {e}")
        exit(1)

    print("Aggregating internal parameters by test...")
    agg_funcs = {
        'parameter_id': lambda x: '\n'.join(x.astype(str).unique()),
        'parameter_name': lambda x: '\n'.join(x.astype(str).unique()),
        'test_name': 'first',
        'test_alias_name': 'first',
        'test_code': 'first'
    }
    unique_tests_df = input_df.groupby('test_id', as_index=False).agg(agg_funcs)

    summary_cols = ['test_id', 'test_name', 'test_alias_name', 'test_code', 'parameter_name', 'parameter_id']
    summary_df = unique_tests_df[summary_cols]
    summary_df = summary_df.rename(columns={
        'parameter_id': 'internal_parameter_ids',
        'parameter_name': 'internal_parameter_names'
    })

    print(f"Found {len(unique_tests_df)} unique tests.")

    # --- 2. Prepare Excel Writer ---
    print(f"Preparing Excel output file: {OUTPUT_EXCEL}")
    try:
        # Ensure the directory exists if OUTPUT_EXCEL includes a path
        output_dir = os.path.dirname(OUTPUT_EXCEL)
        if output_dir and not os.path.exists(output_dir):
             os.makedirs(output_dir)
             print(f"Created output directory: {output_dir}")
        writer = pd.ExcelWriter(OUTPUT_EXCEL, engine='openpyxl')
    except Exception as e:
        print(f"ERROR: Could not create Excel writer for {OUTPUT_EXCEL}: {e}")
        exit(1)

    # --- 3. Write Summary Sheet ---
    print("Writing summary sheet...")
    try:
        summary_df.to_excel(writer, sheet_name='Test Summary', index=False)
        worksheet = writer.sheets['Test Summary']
        for i, col in enumerate(summary_df.columns):
             try: # Added try-except for robustness in width calculation
                 max_len = max(summary_df[col].astype(str).map(len).max(), len(col)) + 2
                 worksheet.column_dimensions[chr(65+i)].width = min(max_len, 80) # Increased limit slightly
             except Exception as width_e:
                 print(f"Warning: Could not auto-adjust width for column '{col}' in Summary Sheet: {width_e}")
    except Exception as e:
        print(f"ERROR: Failed to write summary sheet: {e}")

    # --- 4. Iterate Through Tests, Search LOINC, Fetch Parameters, Write Sheets ---
    start_time = time.time()
    total_tests = len(unique_tests_df)

    for loop_count, (index, test_row) in enumerate(unique_tests_df.iterrows()):
        internal_test_id = test_row['test_id']
        internal_test_name = test_row['test_name']
        print(f"\n[{loop_count + 1}/{total_tests}] Processing Test ID: {internal_test_id}, Name: '{internal_test_name}'")

        search_term = re.sub(r'(?i)\s+(test|panel)$', '', internal_test_name).strip()
        if not search_term:
            search_term = internal_test_name

        # --- 4a. Search LOINC for potential test matches ---
        loinc_test_matches = search_loinc_tests(search_term, loinc_auth, HEADERS)
        time.sleep(0.2)

        test_sheet_data = []

        if not loinc_test_matches:
            print(f"  No suitable LOINC test matches found or kept for '{internal_test_name}'. Adding placeholder row.")
            placeholder_row = {
                "search_term": search_term, "match_rank": 0,
                "loinc_test_code": "Not Found", "loinc_test_long_name": "No matching LOINC term found/kept",
                # Fill other test fields as N/A
                "loinc_test_status": "N/A", "loinc_test_class_type": "N/A", "loinc_test_component": "N/A",
                "loinc_test_property": "N/A", "loinc_test_time": "N/A", "loinc_test_system": "N/A",
                "loinc_test_scale": "N/A", "loinc_test_method": "N/A", "loinc_test_class": "N/A",
                "loinc_test_short_name": "N/A", "loinc_test_url": "N/A",
                # Parameter fields also N/A
                "loinc_parameter_codes": "N/A", "loinc_parameter_names": "N/A"
            }
            test_sheet_data.append(placeholder_row)
        else:
            # --- 4b. For each potential test match... ---
            for test_match in loinc_test_matches:
                loinc_test_code = test_match['loinc_test_code']

                # --- 4b-i. Get parameter codes from FHIR ---
                parameter_codes_result = get_loinc_parameter_codes_from_fhir(loinc_test_code, loinc_auth, FHIR_HEADERS)
                time.sleep(0.1) # Delay after FHIR call

                final_param_codes_str = ""
                final_param_names_str = ""

                # --- 4b-ii. If codes found, get LCN for each code ---
                if isinstance(parameter_codes_result, list): # Success, got a list of codes
                    parameter_codes = parameter_codes_result
                    long_common_names = []
                    actual_codes_found = [] # Store codes for which we attempt LCN lookup

                    for p_code in parameter_codes:
                         actual_codes_found.append(p_code) # Add code regardless of LCN success
                         lcn = get_long_common_name_for_code(p_code, loinc_auth, HEADERS)
                         long_common_names.append(lcn)
                         time.sleep(0.1) # Politeness delay *between* LCN lookups

                    final_param_codes_str = "\n".join(actual_codes_found)
                    final_param_names_str = "\n".join(long_common_names)

                else: # Handle error strings or "No Params Found" from FHIR function
                    error_or_status_msg = parameter_codes_result
                    print(f"      -> Parameter fetch status for {loinc_test_code}: {error_or_status_msg}")
                    final_param_codes_str = error_or_status_msg # e.g., "No Params Found", "FHIR HTTP Error 404"
                    final_param_names_str = error_or_status_msg # Keep message consistent

                # --- 4b-iii. Combine test match info with final parameter info ---
                row_data = test_match.copy()
                row_data["loinc_parameter_codes"] = final_param_codes_str
                row_data["loinc_parameter_names"] = final_param_names_str # Now contains LCNs or error messages
                test_sheet_data.append(row_data)
                # time.sleep(0.2) # Moved delays inside loops


        # --- 4c. Write the sheet for this internal test ---
        if test_sheet_data:
            # Use clean_sheet_name function for safety
            sheet_name = internal_test_name[:31]
            print(f"  Writing sheet: '{sheet_name}' ({len(test_sheet_data)} rows)")
            try:
                test_df = pd.DataFrame(test_sheet_data)
                # Define column order - keeping names descriptive
                cols_order = [
                    "loinc_test_code", "search_term", "loinc_test_long_name", "loinc_test_short_name", "loinc_test_url",
                    "loinc_parameter_names", "loinc_parameter_codes", # Swapped order slightly, LCN first
                    "loinc_test_status", "loinc_test_class_type", "loinc_test_class", "loinc_test_component",
                    "loinc_test_property", "loinc_test_time", "loinc_test_system", "loinc_test_scale",
                    "loinc_test_method", "match_rank"
                ]
                # Ensure all expected columns exist, add if missing (e.g., if all rows were placeholders)
                for col in cols_order:
                    if col not in test_df.columns:
                        test_df[col] = "N/A" # Or appropriate default

                test_df = test_df[cols_order]
                test_df.to_excel(writer, sheet_name=sheet_name, index=False)

                worksheet = writer.sheets[sheet_name]
                for j, col in enumerate(test_df.columns):
                    try:
                        if col in ["loinc_parameter_codes", "loinc_parameter_names"]:
                             # Calculate max line length within multiline cells
                             max_line_len = test_df[col].astype(str).map(lambda x: max((len(line) for line in x.split('\n')), default=0)).max()
                             max_len = max(max_line_len, len(col)) + 2
                        else:
                             max_len = max(test_df[col].astype(str).map(len).max(), len(col)) + 2
                        worksheet.column_dimensions[chr(65+j)].width = min(max_len, 80) # Limit max width
                    except Exception as width_e:
                        print(f"Warning: Could not auto-adjust width for column '{col}' in sheet '{sheet_name}': {width_e}")

            except Exception as e:
                print(f"ERROR: Failed to write sheet '{sheet_name}': {e}")
        else:
             print(f"  No data generated for test '{internal_test_name}' (ID: {internal_test_id}). Skipping sheet creation.")


    # --- 5. Save and Close Excel File ---
    print("\nSaving Excel file...")
    try:
        writer.close()
        print(f"Successfully saved results to {OUTPUT_EXCEL}")
    except Exception as e:
        # Specific check for file possibly being open
        if isinstance(e, PermissionError):
             print(f"ERROR: Failed to save Excel file: {e}. Please ensure the file '{OUTPUT_EXCEL}' is not open in another application.")
        else:
             print(f"ERROR: Failed to save Excel file: {e}")


    end_time = time.time()
    print(f"\nScript finished in {end_time - start_time:.2f} seconds.")