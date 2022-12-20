"""
-----------------------------------------------------------------------
File name: generate_jem_lims_metadata.py
Maintainer: Ramkumar Rajanbabu
-----------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 10/01/2021
Description: Template for generating generate_jem_lims_metadata.csv
-----------------------------------------------------------------------
"""


#-----Imports-----#
# General imports
import numpy as np
import os
import pandas as pd
# File imports
from functions.file_functions import load_data_variables
from functions.jem_functions import clean_date_field, clean_time_field, clean_num_field, clean_roi_field, \
replace_value, add_jem_patch_tube_field, add_jem_species_field, add_jem_post_patch_status_field, get_project_channel, \
fix_jem_versions
from functions.lims_functions import get_lims
# Test imports
import time # To measure program execution time


#-----Functions-----#
def main():
	"""
	Main function to create jem_lims_metadata.csv and jem_lims_metadata.xlsx

	Parameters:
		None

	Returns:
		jem_lims_metadata.csv (csv file)
		jem_lims_metadata.xlsx (excel file)
	"""

	# Load json file
	data_variables = load_data_variables()

	# Directories
	path_output = "//allen/programs/celltypes/workgroups/279/Patch-Seq/ivscc-data-warehouse/data-sources"
	#path_output_view = "//allen/programs/celltypes/workgroups/279/Patch-Seq/ivscc-data-warehouse/view-data-sources"

	# Generate jem_df
	jem_df = generate_jem_df()
	# Rename columns based on jem_dictionary
	jem_df.rename(columns=data_variables["jem_dictionary"], inplace=True)
	# Filter dataframe to only IVSCC Group 2017-Present
	jem_df = jem_df[jem_df["jem-id_rig_user"].isin(data_variables["ivscc_rig_users_list"])]
	jem_df = jem_df[jem_df["jem-id_rig_number"].isin(data_variables["ivscc_rig_numbers_list"])]

	# Fix jem versions
	jem_df = fix_jem_versions(jem_df)
	# Clean and add date_fields
	jem_df = clean_date_field(jem_df)
	# Clean time and add duration fields
	jem_df = clean_time_field(jem_df)
	# Clean numerical fields
	jem_df = clean_num_field(jem_df)
	# Clean and add roi fields
	jem_df = clean_roi_field(jem_df)
	# Clean up project_level_nucleus
	jem_df["jem-project_level_nucleus"] = jem_df.apply(get_project_channel, axis=1)
	# Replace value in fields
	jem_df = replace_value(jem_df)
	# Add patch tube field
	jem_df = add_jem_patch_tube_field(jem_df)
	# Add species field
	jem_df = add_jem_species_field(jem_df)
	# Add post patch status field
	jem_df = add_jem_post_patch_status_field(jem_df)

	# Drop columns
	jem_df.drop(columns=data_variables["drop_list"], inplace=True)

	# Add lims_df
	lims_df = get_lims()
	# Rename columns based on jem_dictionary
	lims_df.rename(columns=data_variables["lims_dictionary"], inplace=True)

	# Merge jem_df and lims_df
	jem_lims_df = pd.merge(left=jem_df, right=lims_df, left_on="jem-id_patched_cell_container", right_on="lims-id_patched_cell_container", how="left")
	#jem_lims_df = pd.merge(left=jem_df, right=lims_df, left_on="jem-id_cell_specimen", right_on="lims-id_cell_specimen", how="left")
	# Sort columns
	jem_lims_df = jem_lims_df.reindex(columns=data_variables["column_order_list"])
	# Create a new column (test-jem_lims) for jem_lims_df 
	jem_lims_df["test-jem_lims_specimen_id_congruency"] = np.where((jem_lims_df["jem-id_cell_specimen"] == jem_lims_df["lims-id_cell_specimen"])&(jem_lims_df["jem-id_patched_cell_container"] == jem_lims_df["lims-id_patched_cell_container"]), "Matching JEM & LIMS Information",
																 np.where((jem_lims_df["jem-id_cell_specimen"].isnull()), "Missing JEM Cell Specimen ID",
																 np.where((jem_lims_df["lims-id_cell_specimen"].isnull()), "Missing LIMS Cell Specimen ID",
																 np.where((jem_lims_df["jem-id_cell_specimen"] != jem_lims_df["lims-id_cell_specimen"]), "Mismatching JEM & LIMS Cell Specimen ID", "Unsure"))))
	# Create a new column (test-jem_lims) for jem_lims_df 
	#jem_lims_df["test-jem_lims_patch_tube_congruency"] = np.where((jem_lims_df["jem-id_cell_specimen"] == jem_lims_df["lims-id_cell_specimen"])&(jem_lims_df["jem-id_patched_cell_container"] == jem_lims_df["lims-id_patched_cell_container"]), "Matching JEM & LIMS Patch Tube",
	#															np.where((jem_lims_df["jem-id_cell_specimen"].isnull()), "Missing JEM Cell Specimen ID",
	#															np.where((jem_lims_df["jem-id_cell_specimen"].isnull()), "Missing JEM Patch Tube",
	#															np.where((jem_lims_df["lims-id_cell_specimen"].isnull()), "Missing LIMS Patch Tube",
	#															np.where((jem_lims_df["jem-id_patched_cell_container"] != jem_lims_df["lims-id_patched_cell_container"]), "Mismatching JEM & LIMS Patch Tube", "Unsure")))))
	# Sort values
	jem_lims_df.sort_values(by=["jem-date_patch_y-m-d", "jem-id_slice_specimen", "jem-id_cell_specimen"], ascending=[False, True, True], inplace=True)
	# Dataframe to csvs and excel
	jem_lims_df.to_csv(path_or_buf=os.path.join(path_output, "jem_lims_metadata.csv"), index=False)
	#jem_lims_df.to_excel(excel_writer=os.path.join(path_output, "jem_lims_metadata.xlsx"), index=False)


def generate_jem_df():
	"""
	Generates a formatted version of JEM metadata by using csv files from compiled-jem-data. 

	Parameters:
		None

	Returns:
		jem_df (dataframe): a pandas dataframe.
	"""

	# Directories
	jem_raw_data_dir = "//allen/programs/celltypes/workgroups/279/Patch-Seq/ivscc-data-warehouse/data-sources/jem-raw-data-sources"
	# Read all jem dataframes
	jem_df = pd.read_csv(os.path.join(jem_raw_data_dir, "jem_metadata.csv"), low_memory=False)
	jem_na_df = pd.read_csv(os.path.join(jem_raw_data_dir, "NA_jem_metadata.csv"), low_memory=False)
	jem_fail_df = pd.read_csv(os.path.join(jem_raw_data_dir, "jem_metadata_wFAILURE.csv"), low_memory=False)
	# Replace status values
	jem_df["status"] = jem_df["status"].replace({"SUCCESS (high confidence)": "SUCCESS"})
	jem_fail_df["status"] = jem_fail_df["status"].replace({"SUCCESS (high confidence)": "SUCCESS", "NO ATTEMPTS": "FAILURE", "Failure": "FAILURE"})
	# Filter dataframe to only FAILURE
	jem_fail_df = jem_fail_df[jem_fail_df["status"] == "FAILURE"]
	# Filter tubes and NAs
	jem_df = jem_df[(jem_df["status"] == "SUCCESS")&(~jem_df["container"].isnull())]
	jem_na_df = jem_na_df[(jem_na_df["status"] == "SUCCESS")&(jem_na_df["container"].isnull())]
	# Replace experiments without tubes with NA
	jem_df["container"] = jem_df["container"].replace({np.nan: "NA"})
	jem_na_df["container"] = jem_na_df["container"].replace({np.nan: "NA"})
	# Merge all jem dataframes
	jem_df = pd.concat([jem_df, jem_na_df, jem_fail_df], ignore_index=True, sort=False)

	return jem_df


if __name__ == "__main__":
    start = time.time()
    main()
    print("\nThe program was executed in", round(((time.time()-start)/60), 2), "minutes.")
