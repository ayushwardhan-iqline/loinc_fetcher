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
OUTPUT_EXCEL = "loinc_mapping_results.xlsx"

# API Endpoints
LOINC_SEARCH_API = "https://loinc.regenstrief.org/searchapi/loincs"
LOINC_FHIR_QUESTIONNAIRE_API = "https://fhir.loinc.org/Questionnaire/" # Query params added later

# Headers for requests
HEADERS = {
    'User-Agent': 'LIMSMappingScript/2.0 (Contact: ayush.wardhan@iqline.co.in)',
    'Accept': 'application/json' # Explicitly accept JSON
}
FHIR_HEADERS = {
    'User-Agent': 'LIMSMappingScript/2.0 (Contact: ayush.wardhan@iqline.co.in)',
    'Accept': 'application/fhir+json' # Standard FHIR JSON mime type
}


# --- Pre-filtering Configuration for LOINC Search API ---
ENABLE_PRE_FILTERING = True
FILTER_ON_STATUS = True
FILTER_STATUS_KEEP = 'ACTIVE'
FILTER_ON_CLASSTYPE = False
FILTER_CLASSTYPE_KEEP = 1 # 1=Lab
FILTER_ON_SCALE = False
FILTER_SCALE_EXCLUDE = 'Doc'
# --- End Filter Criteria ---

# --- Helper Function to Fetch LOINC Test Codes (Modified from original) ---
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
            response.raise_for_status() # Raises HTTPError for bad responses (4XX, 5XX)
            data = response.json()
            loinc_results = data.get("Results", [])
            results_found_total = len(loinc_results)
            results_kept_count = 0

            if not loinc_results:
                print(f"    -> No LOINC results found.")
                return [] # Return empty list if no results

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

                # --- Apply Pre-filtering ---
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
                # --- End Pre-filtering ---

            print(f"    -> Found {results_found_total} results. Kept {results_kept_count} after filtering.")
            return results_list # Success

        except requests.exceptions.HTTPError as http_err:
            print(f"    -> HTTP error on search API: {http_err} (Status: {response.status_code})")
            if response.status_code == 429: # Too Many Requests
                print(f"    -> Rate limited. Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2 # Exponential backoff
                retry_count += 1
            elif 500 <= response.status_code < 600: # Server-side errors
                 print(f"    -> Server error. Retrying in {delay} seconds...")
                 time.sleep(delay)
                 delay *= 2
                 retry_count += 1
            else:
                 print(f"    -> Unrecoverable HTTP error for term '{term}'. Skipping.")
                 return [] # Give up on this term for non-retriable errors
        except requests.exceptions.RequestException as req_err: # Catch other request errors (conn, timeout)
             print(f"    -> Request error on search API for term '{term}': {req_err}. Retrying in {delay} seconds...")
             time.sleep(delay)
             delay *= 2
             retry_count += 1
        except json.JSONDecodeError as json_err:
             print(f"    -> JSON decoding error on search API for term '{term}': {json_err}. Response text: {response.text[:200]}... Skipping.")
             return [] # Give up if response is not valid JSON
        except Exception as e:
            print(f"    -> Unexpected error during LOINC search for '{term}': {e}. Skipping.")
            return [] # Give up on unexpected errors

        time.sleep(0.2) # Small delay between retries

    print(f"    -> Failed to get results for term '{term}' after {max_retries + 1} attempts.")
    return []


# --- Helper Function to Fetch LOINC Panel Parameters via FHIR API ---
def get_loinc_panel_parameters(loinc_code, auth, headers, max_retries=2, initial_delay=1):
    """Fetches panel members (parameters) for a given LOINC code using the FHIR Questionnaire API."""
    print(f"      Fetching FHIR Questionnaire for LOINC: {loinc_code}")
    param_codes = []
    param_names = []
    retry_count = 0
    delay = initial_delay

    if not loinc_code or loinc_code == "Parse Error":
        print("      -> Invalid LOINC code provided. Skipping FHIR search.")
        return "Invalid LOINC", "Invalid LOINC"

    # Construct the query URL
    params = {"url": f"http://loinc.org/q/{loinc_code}"}

    while retry_count <= max_retries:
        try:
            response = requests.get(
                LOINC_FHIR_QUESTIONNAIRE_API,
                params=params,
                auth=auth, # Assuming same auth works for FHIR endpoint
                headers=headers,
                timeout=45
            )
            response.raise_for_status()
            data = response.json()

            # Check if the bundle contains any entries
            if data.get("total", 0) > 0 and data.get("entry"):
                questionnaire_resource = data["entry"][0].get("resource")
                if questionnaire_resource and questionnaire_resource.get("resourceType") == "Questionnaire":
                    items = questionnaire_resource.get("item", [])
                    if not items:
                         print(f"      -> Questionnaire found for {loinc_code}, but contains no 'item' elements (parameters).")
                         return "No Params Found", "No Params Found"

                    for item in items:
                        # Extract code and display - prioritize code[0] if multiple exist
                        code_info = item.get("code", [{}])[0] # Get first code element or empty dict
                        code = code_info.get("code", "No Code")
                        display = code_info.get("display", item.get("text", "No Name")) # Fallback to item.text

                        param_codes.append(code)
                        param_names.append(display)

                    print(f"      -> Found {len(param_codes)} parameters.")
                    # Join with newlines for multi-line cell in Excel
                    return "\n".join(param_codes), "\n".join(param_names)
                else:
                    print(f"      -> FHIR response for {loinc_code} does not contain a valid Questionnaire resource.")
                    return "Resource Error", "Resource Error"
            else:
                print(f"      -> No FHIR Questionnaire found for LOINC code: {loinc_code}")
                return "Not Found", "Not Found"

        except requests.exceptions.HTTPError as http_err:
            print(f"      -> HTTP error on FHIR API for {loinc_code}: {http_err} (Status: {response.status_code})")
            if response.status_code == 429: # Too Many Requests
                print(f"      -> Rate limited. Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2 # Exponential backoff
                retry_count += 1
            elif 500 <= response.status_code < 600: # Server-side errors
                 print(f"      -> Server error. Retrying in {delay} seconds...")
                 time.sleep(delay)
                 delay *= 2
                 retry_count += 1
            else:
                print(f"      -> Unrecoverable FHIR HTTP error for {loinc_code}. Marking as 'HTTP Error'.")
                return f"HTTP Error {response.status_code}", f"HTTP Error {response.status_code}"
        except requests.exceptions.RequestException as req_err:
            print(f"      -> Request error on FHIR API for {loinc_code}: {req_err}. Retrying in {delay} seconds...")
            time.sleep(delay)
            delay *= 2
            retry_count += 1
        except json.JSONDecodeError as json_err:
            print(f"      -> JSON decoding error on FHIR API for {loinc_code}: {json_err}. Response: {response.text[:200]}... Marking as 'JSON Error'.")
            return "JSON Error", "JSON Error"
        except Exception as e:
            print(f"      -> Unexpected error during FHIR fetch for {loinc_code}: {e}. Marking as 'Unexpected Error'.")
            return "Unexpected Error", "Unexpected Error"

        time.sleep(0.2) # Small delay between retries

    print(f"      -> Failed to get FHIR results for {loinc_code} after {max_retries + 1} attempts.")
    return "Fetch Failed", "Fetch Failed"


# --- Main Execution ---
if __name__ == "__main__":
    if not LOINC_USERNAME or not LOINC_PASSWORD:
        print("ERROR: LOINC_USERNAME and LOINC_PASSWORD environment variables must be set.")
        exit(1)

    # Use Basic Auth for both APIs
    loinc_auth = HTTPBasicAuth(LOINC_USERNAME, LOINC_PASSWORD)

    # --- 1. Read and Process Input CSV ---
    print(f"Reading input file: {INPUT_CSV}")
    try:
        input_df = pd.read_csv(INPUT_CSV, dtype=str) # Read all as string initially
        input_df.fillna('', inplace=True) # Replace NaN with empty strings
    except FileNotFoundError:
        print(f"ERROR: Input file not found: {INPUT_CSV}")
        exit(1)
    except Exception as e:
        print(f"ERROR: Failed to read input CSV: {e}")
        exit(1)

    print("Aggregating internal parameters by test...")
    # Group by test information and aggregate parameters
    # Using first() assumes test_id, name, alias, code are unique per test
    agg_funcs = {
        'parameter_id': lambda x: '\n'.join(x.astype(str).unique()),
        'parameter_name': lambda x: '\n'.join(x.astype(str).unique()),
        'test_name': 'first',
        'test_alias_name': 'first',
        'test_code': 'first'
    }
    # Group by test_id, ensure we keep other test columns
    unique_tests_df = input_df.groupby('test_id', as_index=False).agg(agg_funcs)

    # Reorder columns for summary sheet clarity
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
        writer = pd.ExcelWriter(OUTPUT_EXCEL, engine='openpyxl')
    except Exception as e:
        print(f"ERROR: Could not create Excel writer: {e}")
        exit(1)

    # --- 3. Write Summary Sheet ---
    print("Writing summary sheet...")
    try:
        summary_df.to_excel(writer, sheet_name='Test Summary', index=False)
        # Auto-adjust column widths for summary sheet (optional but nice)
        worksheet = writer.sheets['Test Summary']
        for i, col in enumerate(summary_df.columns):
             max_len = max(summary_df[col].astype(str).map(len).max(), len(col)) + 2
             worksheet.column_dimensions[chr(65+i)].width = max_len # Adjust width
    except Exception as e:
        print(f"ERROR: Failed to write summary sheet: {e}")
        # Continue to try writing other sheets if possible

    # --- 4. Iterate Through Tests, Search LOINC, Fetch Parameters, Write Sheets ---
    start_time = time.time()
    total_tests = len(unique_tests_df)

    for i, (original_index, test_row) in enumerate(unique_tests_df.iterrows()):
        internal_test_id = test_row['test_id']
        internal_test_name = test_row['test_name']
        print(f"\n[{i + 1}/{total_tests}] Processing Test ID: {internal_test_id}, Name: '{internal_test_name}'")

        # Simple cleaning: remove " test" or " panel" if present at the end (case-insensitive)
        search_term = re.sub(r'(?i)\s+(test|panel)$', '', internal_test_name).strip()
        if not search_term: # If name was only "Test" or "Panel"
            search_term = internal_test_name # Use original

        # --- 4a. Search LOINC for potential test matches ---
        loinc_test_matches = search_loinc_tests(search_term, loinc_auth, HEADERS)
        time.sleep(0.2) # Politeness delay between different test searches

        test_sheet_data = [] # Holds data for this test's sheet

        if not loinc_test_matches:
            print(f"  No suitable LOINC test matches found or kept for '{internal_test_name}'. Adding placeholder row to sheet.")
            # Add a placeholder row indicating no matches found after filtering
            placeholder_row = {
                "search_term": search_term, "match_rank": 0,
                "loinc_test_code": "Not Found", "loinc_test_long_name": "No matching LOINC term found/kept",
                "loinc_test_status": "N/A", "loinc_test_class_type": "N/A",
                "loinc_test_component": "N/A", "loinc_test_property": "N/A",
                "loinc_test_time": "N/A", "loinc_test_system": "N/A",
                "loinc_test_scale": "N/A", "loinc_test_method": "N/A",
                "loinc_test_class": "N/A", "loinc_test_short_name": "N/A",
                "loinc_test_url": "N/A",
                "loinc_parameter_codes": "N/A", "loinc_parameter_names": "N/A"
            }
            test_sheet_data.append(placeholder_row)

        else:
            # --- 4b. For each potential match, get its parameters via FHIR API ---
            for test_match in loinc_test_matches:
                loinc_code = test_match['loinc_test_code']
                param_codes, param_names = get_loinc_panel_parameters(loinc_code, loinc_auth, FHIR_HEADERS)

                # Combine test match info with parameter info
                row_data = test_match.copy() # Start with the test match data
                row_data["loinc_parameter_codes"] = param_codes
                row_data["loinc_parameter_names"] = param_names
                test_sheet_data.append(row_data)
                time.sleep(0.2) # Politeness delay between FHIR API calls


        # --- 4c. Write the sheet for this internal test ---
        if test_sheet_data:
            sheet_name = internal_test_name[:30] # Limit to 30 chars for Excel sheet name
            print(f"  Writing sheet: '{sheet_name}'")
            try:
                test_df = pd.DataFrame(test_sheet_data)
                # Define column order for clarity
                cols_order = [
                    "loinc_test_code", "search_term", "loinc_test_long_name", "loinc_test_short_name", "loinc_test_url",
                    "loinc_parameter_names", "loinc_parameter_codes",
                    "loinc_test_status", "loinc_test_class_type", "loinc_test_class", "loinc_test_component",
                    "loinc_test_property", "loinc_test_time", "loinc_test_system", "loinc_test_scale",
                    "loinc_test_method", "match_rank"
                ]
                test_df = test_df[cols_order] # Reorder/select columns
                test_df.to_excel(writer, sheet_name=sheet_name, index=False)

                # Auto-adjust column widths for test sheet (optional)
                worksheet = writer.sheets[sheet_name]
                for i, col in enumerate(test_df.columns):
                     # Estimate max width needed, handle multiline parameter cells
                     if col in ["loinc_parameter_codes", "loinc_parameter_names"]:
                         max_len = max(test_df[col].astype(str).map(lambda x: max(len(line) for line in x.split('\n'))).max(), len(col)) + 2
                     else:
                         max_len = max(test_df[col].astype(str).map(len).max(), len(col)) + 2
                     worksheet.column_dimensions[chr(65+i)].width = min(max_len, 60) # Limit max width

            except Exception as e:
                print(f"ERROR: Failed to write sheet '{sheet_name}': {e}")
        else:
             print(f"  No data generated for test '{internal_test_name}' (ID: {internal_test_id}). Skipping sheet creation.")


    # --- 5. Save and Close Excel File ---
    print("\nSaving Excel file...")
    try:
        writer.close() # Use close() for pandas >= 1.4 with openpyxl
        print(f"Successfully saved results to {OUTPUT_EXCEL}")
    except Exception as e:
        print(f"ERROR: Failed to save Excel file: {e}")

    end_time = time.time()
    print(f"\nScript finished in {end_time - start_time:.2f} seconds.")