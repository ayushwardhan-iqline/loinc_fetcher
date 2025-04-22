import requests
import csv
import time
import os

# --- Configuration ---
# IMPORTANT: Replace with your actual LOINC credentials
# You can get these by registering for a free account at https://loinc.org/
LOINC_USERNAME = os.environ['LOINC_USERNAME']
LOINC_PASSWORD = os.environ['LOINC_PASSWORD']

# Alternative: Use environment variables for better security
# LOINC_USERNAME = os.getenv("LOINC_USERNAME", "YOUR_LOINC_USERNAME")
# LOINC_PASSWORD = os.getenv("LOINC_PASSWORD", "YOUR_PASSWORD")

OUTPUT_CSV_TESTS = "loinc_tests.csv"
OUTPUT_CSV_PARAMETERS = "loinc_parameters.csv"
API_ENDPOINT = "https://loinc.regenstrief.org/searchapi/loincs"

# --- Input Data ---
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
    """
    Fetches LOINC codes for a given list of terms using the LOINC API.

    Args:
        terms_list (list): A list of strings (test or parameter names).
        auth_credentials (tuple): A tuple containing (LOINC_USERNAME, LOINC_PASSWORD).
        list_name (str): A descriptive name for the list being processed (for logging).

    Returns:
        list: A list of dictionaries, each containing the search term,
              found LOINC code, name, and status.
    """
    results = []
    total_terms = len(terms_list)
    print(f"\n--- Starting LOINC search for {total_terms} {list_name} ---")

    for i, term in enumerate(terms_list):
        # Deduping within the list being processed (optional but efficient)
        # If you absolutely need duplicates processed, remove this check
        # if term in [r['search_term'] for r in results]:
        #      print(f"[{i+1}/{total_terms}] Skipping duplicate term: {term}")
        #      # Optionally copy the result from the first occurrence
        #      first_occurrence = next((r for r in results if r['search_term'] == term), None)
        #      if first_occurrence:
        #          results.append(first_occurrence.copy()) # Append a copy
        #      continue # Move to next term

        print(f"[{i+1}/{total_terms}] Searching for: {term}")
        result_entry = {
            "search_term": term,
            "loinc": "Not Found",
            "name": "N/A",
            "status": "Not Found",
            "unit": "N/A"
        }
        try:
            response = requests.get(
                API_ENDPOINT,
                params={"query": term},
                auth=auth_credentials,
                timeout=30 # Add a timeout
            )
            response.raise_for_status() # Check for HTTP errors

            data = response.json()
            # print(data)
            loinc_results = data.get("Results", [])
            preferred_result = None

            # Check if any result has EXAMPLE_UNITS == "mg/dL"
            for this_sample in loinc_results:
                units = this_sample.get("EXAMPLE_UNITS")
                if isinstance(units, str) and units.strip().lower() == "mg/dl":
                    preferred_result = this_sample
                    break

            # If none found with mg/dL, fall back to the first result
            if not preferred_result and loinc_results:
                preferred_result = loinc_results[0]
            
            if preferred_result:
                top_hit = preferred_result
                result_entry["loinc"] = top_hit.get("LOINC_NUM", "Error Parsing LOINC")
                result_entry["name"] = top_hit.get("LONG_COMMON_NAME", "Error Parsing Name")
                result_entry["unit"] = top_hit.get("EXAMPLE_UNITS", "Error Parsing Units")
                result_entry["status"] = "Found"
                print(f"  -> Found: {result_entry['loinc']} - {result_entry['name']}")
            else:
                print(f"  -> Not Found in LOINC database.")
                result_entry["status"] = "Not Found in DB"

        except requests.exceptions.HTTPError as http_err:
            print(f"  -> HTTP error occurred: {http_err} (Status code: {response.status_code})")
            result_entry["status"] = f"HTTP Error {response.status_code}"
            result_entry["name"] = str(http_err)
        except requests.exceptions.ConnectionError as conn_err:
            print(f"  -> Connection error occurred: {conn_err}")
            result_entry["status"] = "Connection Error"
            result_entry["name"] = str(conn_err)
        except requests.exceptions.Timeout as timeout_err:
            print(f"  -> Timeout error occurred: {timeout_err}")
            result_entry["status"] = "Timeout Error"
            result_entry["name"] = str(timeout_err)
        except requests.exceptions.RequestException as req_err:
            print(f"  -> An ambiguous request error occurred: {req_err}")
            result_entry["status"] = "Request Error"
            result_entry["name"] = str(req_err)
        except Exception as e:
            print(f"  -> An unexpected error occurred: {e}")
            result_entry["status"] = "Unexpected Error"
            result_entry["name"] = str(e)

        results.append(result_entry)
        # Optional: Add a small delay
        # time.sleep(0.1)

    print(f"--- Finished LOINC search for {list_name} ---")
    return results

# --- Helper Function to Save Results to CSV ---
def save_to_csv(results_list, filename):
    """Saves a list of result dictionaries to a CSV file."""
    if not results_list:
        print(f"No results to save for {filename}.")
        return

    print(f"Saving results to {filename}...")
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            # Define the header row based on the keys of the first dictionary
            fieldnames = results_list[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(results_list)
        print(f"Successfully saved results to {filename}.")
    except IOError as e:
        print(f"Error saving CSV file {filename}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during CSV writing for {filename}: {e}")

# --- Main Execution ---
if __name__ == "__main__":
    # Check if credentials are placeholders
    if LOINC_USERNAME == "YOUR_LOINC_USERNAME" or LOINC_PASSWORD == "YOUR_PASSWORD":
        print("ERROR: Please replace placeholder LOINC credentials in the script.")
        exit() # Stop execution if credentials are not set

    loinc_auth = (LOINC_USERNAME, LOINC_PASSWORD)

    # --- Process Test Names ---
    test_results = fetch_loinc_codes(test_names, loinc_auth, list_name="Test Names")
    save_to_csv(test_results, OUTPUT_CSV_TESTS)

    # --- Process Parameter Names ---
    parameter_results = fetch_loinc_codes(parameter_names, loinc_auth, list_name="Parameter Names")
    save_to_csv(parameter_results, OUTPUT_CSV_PARAMETERS)

    print("\nScript finished.")