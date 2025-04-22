import requests
import csv
import time
import os
import json

# --- Configuration ---
LOINC_USERNAME = os.getenv("LOINC_USERNAME")
LOINC_PASSWORD = os.getenv("LOINC_PASSWORD")

OUTPUT_CSV_TESTS = "loinc_tests_detailed.csv"
OUTPUT_CSV_PARAMETERS = "loinc_parameters_detailed.csv"
API_ENDPOINT = "https://loinc.regenstrief.org/searchapi/loincs"
HEADERS = {'User-Agent': 'LIMSMappingScript/1.2 (Contact: your-email@example.com)'}

# --- Pre-filtering Configuration ---
ENABLE_PRE_FILTERING = True # Master switch for all filters below

# --- Filter Criteria (Only applied if ENABLE_PRE_FILTERING is True) ---
# Keep results ONLY if they meet ALL enabled filter conditions
FILTER_ON_STATUS = True
FILTER_STATUS_KEEP = 'ACTIVE' # Status to keep (e.g., 'ACTIVE')

FILTER_ON_CLASSTYPE = True
FILTER_CLASSTYPE_KEEP = 1     # Class Type to keep (1=Lab)

FILTER_ON_SCALE = True
FILTER_SCALE_EXCLUDE = 'Doc' # Scale Type to EXCLUDE (e.g., 'Doc')
# --- End Filter Criteria ---

# --- Input Data ---
# (Keep your test_names and parameter_names lists)
test_names = [
    "LIVER FUNCTION TEST", "URIC ACID", "ALKALINE PHOSPHATE", "RA FACTOR",
    "TOTAL PROTEIN", "GLUCOSE - FASTING", "GLUCOSE - RBS", "HEMOGLOBIN",
    "AMYLASE", "GLUCOSE - PP", "UREA", "SGPT", "SGOT", "CRP", "ASO",
    "CHOLESTEROL", "LDL", "KIDNEY FUNCTION TEST", "HDL", "LIPID PROFILE",
    "CBC", "TOTAL CALCIUM", "TRIGLYCERIDES", "CREATININE"
]

parameter_names = [
    "Polymorphs", "Serum Beta HCG", "Plasma Glucose, PP (2 Hr.)", "BLEEDING TIME",
    "MCV (Mean Cell Volume )", "Insulin Fasting", "S. Paratyphi 'BH'", "Fluid Protein",
    "24 Hours Microalbumin", "APTT -Test", "Serum Iron", "Salmonella Para typhi 'B','H' (BH)",
    "Haemoglobin (Hb%)", "CREATININE", "Plasma Glucose, (60 Min)", "Serum Bilirubin, Indirect",
    "Serum Ionic Calcium", "CLOTTING TIME", "Serum Acid Phosphatse (Total)",
    "Haemoglobin (Hb%)", "RDW", "PCT", "Volume", "Malaria Parasite, Identification",
    "Active Motile", "Serum AFP", "Serum Albumin", "Fasting Plasma Glucose (FPG)",
    "PCV / Hct.", "Lymphocytes", "Duration of Abstinence", "Serum Lactate",
    "VLDL Cholesterol", "HCT", "MCHC (Mear Corpus. Hb Conc.)", "Serum Amylase",
    "Serum Urea", "Total Sperm Count", "Total Iron Binding Capacity", "Candida",
    "T. Cholesterol", "Glycosylated Hemoglobin (HbA1c)", "Serum Free T4",
    "A1c-AREA (HbA1C)", "Basophils", "Total Leucocyte Count ( TLC )", "PDW",
    "Serum Globulins", "S. Typhi 'O'", "Serum A/G Ratio", "RBCs", "Serum Cholesterol",
    "Dengue specific Antibodies, IgG", "VDRL TEST", "Serum Potassium (K+)",
    "Serum CA-19.9", "Abnormal Cells", "Plasma Glucose Fasting",
    "MCH (Mean Corpus. Haemoglobin)", "CHOL/HDL", "ASO-TITRE ", "Serum Creatinine",
    "Abs. Neutrophils", "Abs. Basophils", "Glycosylated Hemoglobin (HbA1c)",
    "Hg", "Dengue-IgG Antibody ( Elisa )", "Absolute Eosinophil Count",
    "Widal Result", "Dengue specific Antibodies, IgM", "Transferrin Saturation",
    "PCV (Packed Cell Volume)", "Abs. MID", "Serum Bi-Carbonate (HCO3)",
    "Serum Copper", "Liquefication", "Blood Urea Nitrogen", "Lymphocytes",
    "Serum Estradiol", "Blood Urea Nitrogen ( BUN )", "MPV", "MID",
    "Salmonella Typhi 'H' (TH)", "S. Typhi 'H'", "Param 1", "ANTI-TPO",
    "Foetal Hemoglobin", "Dengue NS1 Antigen", "SGOT", "Serum Homocysteine (Quantitative)",
    "C-Reactive Protein", "Transferrin Saturation", "Serum Total Protein",
    "Serum Sodium (Na+)", "Serum Calcium, Total", "Serum T3", "Abs. Eosinophils",
    "Serum TSH", "Serum Free PSA", "TSH", "(PROTHROMBIN TIME) Test", "LDL/HDL",
    "SGPT", "Serum Lipase", "Serum Triglycerides", "CA- 15.3", "Serum T4",
    "ESR- 1 hr ", "CSF, Protien", "Eosinophils", "Salmonella Para typhi 'A','H' (AH)",
    "Serum CK-NAC", "Serum Uric Acid", "Abs. Differential Leucocyte Count (DLC)",
    "Serum CPK-MB", "test", "Salmonella Typhi 'O' (TO)", "Serum Testosterone",
    "HDL Cholesterol", "Serum Prolactin", "RA Factor", "Dengue-IgM Antibody (Elisa)",
    "Plasma Glucose Random", "Serum Phosphorus", "C-reactive Protein (CRP)",
    "Serum Bilirubin, Direct", "RBCs Count", "Platelet Count (Automated)",
    "Serum Alkaline Phosphatase", "Neutrophil", "Postprandial Glucose (PPG)",
    "Pus Cells", "Abs. Lymphocytes", "RDWA", "Parasite", "Blood Urea", "Serum LH",
    "Monocytes", "Serum Free T3", "Serum Vitamin B12", "Serum Calcium, Total",
    "Microalbumin", "Epithelial Cells", "LPCR", "pH", "Serum PSA", "PT INR",
    "LDL Cholesterol", "Band Cells", "S. Paratyphi 'AH'", "Serum Bilirubin, Total",
    "Reticulocyte Count", "Serum Ferritin", "Serum CEA", "Serum Chlorides ",
    "Differential Leucocyte Count (DLC)", "Test parameter", "Total RBCs",
    "Abs. Monocytes", "Urine Microalbumin Spot", "FSH"
]

# --- Helper Function to Fetch LOINC Codes ---
def fetch_loinc_codes(terms_list, auth_credentials, list_name="terms"):
    all_results = []
    processed_terms = set()
    total_unique_terms = len(set(terms_list))
    current_term_index = 0

    print(f"\n--- Starting LOINC search for {total_unique_terms} unique {list_name} ---")
    if ENABLE_PRE_FILTERING:
        filter_desc = []
        if FILTER_ON_STATUS: filter_desc.append(f"STATUS='{FILTER_STATUS_KEEP}'")
        if FILTER_ON_CLASSTYPE: filter_desc.append(f"CLASSTYPE={FILTER_CLASSTYPE_KEEP}")
        if FILTER_ON_SCALE: filter_desc.append(f"SCALE_TYP!='{FILTER_SCALE_EXCLUDE}'")
        print(f"--- Pre-filtering ENABLED: Keeping results WHERE {' AND '.join(filter_desc)} ---")
    else:
        print("--- Pre-filtering DISABLED ---")

    for term in terms_list:
        if term in processed_terms:
           print(f"[Skipping duplicate input term: {term}]")
           continue

        processed_terms.add(term)
        current_term_index += 1
        print(f"[{current_term_index}/{total_unique_terms}] Searching for: '{term}'")
        results_found_for_term = 0
        results_kept_for_term = 0

        try:
            response = requests.get(
                API_ENDPOINT, params={"query": term}, auth=auth_credentials,
                headers=HEADERS, timeout=45
            )
            response.raise_for_status()
            data = response.json()
            loinc_results = data.get("Results", [])
            results_found_for_term = len(loinc_results)

            if not loinc_results:
                print(f"  -> No LOINC results found.")
                # Add placeholder only if filtering is off
                if not ENABLE_PRE_FILTERING:
                     all_results.append({
                        "search_term": term, "match_rank": 0, "loinc": "Not Found",
                        "long_common_name": "N/A", "status": "Not Found in DB", "class_type": "N/A",
                        "component": "N/A", "property": "N/A", "time_aspect": "N/A",
                        "system": "N/A", "scale_type": "N/A", "method_type": "N/A",
                        "example_units": "N/A", "class": "N/A", "short_name": "N/A",
                        "loinc_url": "N/A"
                    })
            else:
                for i, hit in enumerate(loinc_results):
                    loinc_num = hit.get("LOINC_NUM", "Parse Error")
                    loinc_url = f"https://loinc.org/{loinc_num}" if loinc_num != "Parse Error" else "N/A"
                    scale_type = hit.get("SCALE_TYP", "N/A") # Get scale type

                    result_entry = {
                        "search_term": term, "match_rank": i + 1, "loinc": loinc_num,
                        "long_common_name": hit.get("LONG_COMMON_NAME", "N/A"),
                        "status": hit.get("STATUS", "N/A"),
                        "class_type": hit.get("CLASSTYPE", None),
                        "component": hit.get("COMPONENT", "N/A"),
                        "property": hit.get("PROPERTY", "N/A"),
                        "time_aspect": hit.get("TIME_ASPCT", "N/A"),
                        "system": hit.get("SYSTEM", "N/A"),
                        "scale_type": scale_type, # Store the scale type
                        "method_type": hit.get("METHOD_TYP", "N/A"),
                        "example_units": hit.get("EXAMPLE_UNITS", "N/A"),
                        "class": hit.get("CLASS", "N/A"),
                        "short_name": hit.get("SHORTNAME", "N/A"),
                        "loinc_url": loinc_url
                    }

                    # --- Apply Pre-filtering ---
                    passes_filter = True
                    if ENABLE_PRE_FILTERING:
                        # Check Status filter
                        if FILTER_ON_STATUS and result_entry['status'] != FILTER_STATUS_KEEP:
                            passes_filter = False
                        # Check Class Type filter (only if Status passed)
                        if passes_filter and FILTER_ON_CLASSTYPE and result_entry['class_type'] != FILTER_CLASSTYPE_KEEP:
                            passes_filter = False
                        # Check Scale Type Exclude filter (only if previous passed)
                        if passes_filter and FILTER_ON_SCALE and result_entry['scale_type'] == FILTER_SCALE_EXCLUDE:
                            passes_filter = False

                    if passes_filter:
                        all_results.append(result_entry)
                        results_kept_for_term += 1
                    # --- End Pre-filtering ---

                print(f"  -> Found {results_found_for_term} results. Kept {results_kept_for_term} after filtering.")

        # (Keep the existing except blocks)
        except requests.exceptions.HTTPError as http_err:
             print(f"  -> HTTP error: {http_err} (Status: {response.status_code})")
             # Simplified error row creation
             error_row = {"search_term": term, "match_rank": 0, "loinc": f"HTTP Error {response.status_code}", "long_common_name": str(http_err), "status": "Error", "loinc_url": "Error"}
             all_results.append({**{k: "Error" for k in fieldnames if k not in error_row}, **error_row}) # Fill remaining fields
        except requests.exceptions.RequestException as req_err: # Catch other request errors (conn, timeout)
             print(f"  -> Request error: {req_err}")
             error_row = {"search_term": term, "match_rank": 0, "loinc": "Request Error", "long_common_name": str(req_err), "status": "Error", "loinc_url": "Error"}
             all_results.append({**{k: "Error" for k in fieldnames if k not in error_row}, **error_row})
        except json.JSONDecodeError as json_err:
             print(f"  -> JSON decoding error: {json_err}")
             error_row = {"search_term": term, "match_rank": 0, "loinc": "JSON Error", "long_common_name": str(json_err), "status": "Error", "loinc_url": "Error"}
             all_results.append({**{k: "Error" for k in fieldnames if k not in error_row}, **error_row})
        except Exception as e:
            print(f"  -> Unexpected error: {e}")
            error_row = {"search_term": term, "match_rank": 0, "loinc": "Unexpected Error", "long_common_name": str(e), "status": "Error", "loinc_url": "Error"}
            # Define fieldnames here or pass it to make this work robustly
            fieldnames_for_error = ["search_term", "match_rank", "loinc", "loinc_url", "status", "long_common_name", "short_name", "class_type", "component", "property", "time_aspect", "system", "scale_type", "method_type", "example_units", "class"]
            all_results.append({**{k: "Error" for k in fieldnames_for_error if k not in error_row}, **error_row})

        time.sleep(0.1)

    print(f"--- Finished LOINC search for {list_name} ---")
    return all_results

# --- Helper Function to Save Results to CSV ---
# Define fieldnames globally or pass it to the function if needed for error handling above
fieldnames = [
    "search_term", "match_rank", "loinc", "loinc_url", "status",
    "long_common_name", "short_name", "class_type", "component",
    "property", "time_aspect", "system", "scale_type", "method_type",
    "example_units", "class",
]

def save_to_csv(results_list, filename):
    """Saves a list of result dictionaries to a CSV file."""
    if not results_list:
        print(f"No results to save for {filename} (possibly due to filtering).")
        return

    print(f"Saving {len(results_list)} results/rows to {filename}...")
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(results_list)
        print(f"Successfully saved results to {filename}.")
    except IOError as e:
        print(f"Error saving CSV file {filename}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during CSV writing for {filename}: {e}")


# --- Main Execution ---
if __name__ == "__main__":
    if not LOINC_USERNAME or not LOINC_PASSWORD:
        print("ERROR: LOINC_USERNAME and LOINC_PASSWORD environment variables must be set.")
        exit(1)

    loinc_auth = (LOINC_USERNAME, LOINC_PASSWORD)
    start_time = time.time()

    test_results = fetch_loinc_codes(test_names, loinc_auth, list_name="Test Names")
    save_to_csv(test_results, OUTPUT_CSV_TESTS)

    parameter_results = fetch_loinc_codes(parameter_names, loinc_auth, list_name="Parameter Names")
    save_to_csv(parameter_results, OUTPUT_CSV_PARAMETERS)

    end_time = time.time()
    total_results = len(test_results) + len(parameter_results)
    print(f"\nScript finished in {end_time - start_time:.2f} seconds.")
    print(f"Total rows written to CSV files: {total_results}")