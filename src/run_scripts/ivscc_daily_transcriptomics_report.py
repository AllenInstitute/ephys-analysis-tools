"""
-------------------------------------------------------------------------
File name: ivscc_daily_transcriptomics_report.py
Maintainer: Ramkumar Rajanbabu
-------------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 05/27/2021
Description: Generate IVSCC daily transcriptomics report (excel document)
-------------------------------------------------------------------------
"""


#-----Imports-----#
# File imports
from functions.transcriptomics_functions import generate_daily_report
# Test imports
#import time # To measure program execution time


#-----Test Secondary Functions-----#

"""
# Test retrograde_labeling project has positive for status reporter
# Test Picrotoxin Validation project has PX tubes
jem_df["jem-test_projects"] = np.where((jem_df["jem-project_name"] == "retrograde_labeling") & (jem_df["jem-status_reporter"] == "Positive"), "Correct - Retrograde Labeling",
                              np.where((jem_df["jem-project_name"] == "retrograde_labeling") & (jem_df["jem-status_reporter"] != "Positive"), "Incorrect - Retrograde Labeling",
                              np.where((jem_df["jem-project_name"] == "Picrotoxin Validation") & (jem_df["jem-id_patched_cell_container"].str.startswith("PXS4")), "Correct - Picrotoxin Validation",
                              np.where((jem_df["jem-project_name"] == "Picrotoxin Validation") & (~jem_df["jem-id_patched_cell_container"].str.startswith("PXS4")), "Incorrect - Picrotoxin Validation", "Not Applicable"))))

test_jem_proj = jem_df[jem_df["jem-test_projects"].str.startswith("Incorrect")]

def test_jem_projects(test_jem_proj):
    if (len(test_jem_proj) > 0):
        # Row numbering
        test_jem_proj_num = 1
        # Tests for incorrect jem project details
        if len(test_jem_proj) > 0:
            print("#-----Incorrect JEM Project Details-----#")
            print("Description:")
            print("In the JEM form, please correct the project details based on the JEM Project!")
            print()
            for index, row in test_jem_proj.iterrows():
                print(f"{test_jem_proj_num}) JEM Specimen ID: {row['jem-id_cell_specimen']} | JEM Patch Tube: {row['jem-id_patched_cell_container']}")
                print(f"   - JEM Project: {row['jem-project_name']}")
                print(f"   - JEM Reporter Status: {row['jem-status_reporter']}")
                test_jem_proj_num+=1


# Test hIVSCC-METc project codes have PC tubes
lims_df["test-project_code"] = np.where((lims_df["lims-id_project_code"] == "hIVSCC-METc")&(lims_df["lims-id_patched_cell_container"].str.startswith("PCS4")), "Correct - Human Culture",
                               np.where((lims_df["lims-id_project_code"] == "hIVSCC-METc")&(~lims_df["lims-id_patched_cell_container"].str.startswith("PCS4")), "Incorrect - Human Culture", "Not Applicable"))

test_lims_pc = lims_df[lims_df["test-project_code"] == "Incorrect - Human Culture"]

def test_lims_project_codes(test_lims_pc):
    if (len(test_lims_pc) > 0):
        # Row numbering
        test_lims_pc_num = 1
        # Tests for incorrect lims project details
        if len(test_lims_pc) > 0:
            print("#-----Incorrect LIMS Project Code Details-----#")
            print("Description:")
            print("Please use the LIMS Specimen ID to change the JEM Patch Tube and LIMS Patch Tube to PC!")
            print()
            for index, row in test_lims_pc.iterrows():
                print(f"{test_lims_pc_num}) Project code: {row['lims-id_project_code']}| LIMS Specimen ID: {row['lims-id_cell_specimen']}| LIMS Patch Tube: {row['lims-id_patched_cell_container']}")
                test_lims_pc_num+=1
"""


if __name__ == "__main__":
    #start = time.time()
    generate_daily_report("ivscc")
    #print("\nThe program was executed in", round(time.time()-start, 2), "seconds.")
