"""
---------------------------------------------------------------------
File name: jem_template.py
Maintainer: Ramkumar Rajanbabu
---------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 10/01/2021
Description: Template for creating master_jem.csv and master_jem.xlsx
---------------------------------------------------------------------
"""


# Imports
import json
import pandas as pd
import os
import numpy as np
from functions.jem_functions import clean_date_field, clean_time_field, clean_num_field, clean_roi_field, \
replace_value, add_jem_patch_tube_field, add_jem_species_field, add_jem_post_patch_status_field


# Read json data from file to import jem_dictionary
with open("C:/Users/ramr/Documents/Github/ai_repos/ephys_analysis_tools/src/constants/data_variables.json") as json_file:
    data_variables = json.load(json_file)

# compiled-jem-data input and output directory
path_input = "//allen/programs/celltypes/workgroups/279/Patch-Seq/compiled-jem-data/raw_data"
path_output = "//allen/programs/celltypes/workgroups/279/Patch-Seq/compiled-jem-data/formatted_data"

# JEM csv files
path_jem = os.path.join(path_input, "jem_metadata.csv")
path_jem_na = os.path.join(path_input, "NA_jem_metadata.csv")
path_jem_fail = os.path.join(path_input, "jem_metadata_wFAILURE.csv")

# Read all jem dataframes
jem_df = pd.read_csv(path_jem, low_memory=False)
jem_na_df = pd.read_csv(path_jem_na, low_memory=False)
jem_fail_df = pd.read_csv(path_jem_fail, low_memory=False)

# Replace status values
jem_df["status"] = jem_df["status"].replace({"SUCCESS (high confidence)": "SUCCESS"})
jem_fail_df["status"] = jem_fail_df["status"].replace({"SUCCESS (high confidence)": "SUCCESS", "NO ATTEMPTS": "FAILURE", "Failure": "FAILURE"})

# Filter dataframe to only FAILURE
jem_fail_df = jem_fail_df[jem_fail_df["status"] == "FAILURE"]

# Filter tubes and NAs
jem_df = jem_df[(jem_df["status"] == "SUCCESS")&(~jem_df["container"].isnull())]
jem_na_df = jem_na_df[(jem_na_df["status"] == "SUCCESS")&(jem_na_df["container"].isnull())]

# Replace container values
jem_na_df["container"] = jem_na_df["container"].replace({np.nan: "Not Applicable"})

# Merge all jem dataframes
master_jem_df = pd.concat([jem_df, jem_na_df, jem_fail_df], ignore_index=True, sort=False)

# Drop columns
master_jem_df.drop(columns=["organism_name", "name","specimen_ID","full_genotype","cell_depth"], inplace=True)

# Rename columns based on jem_dictionary
master_jem_df.rename(columns=data_variables["jem_dictionary"], inplace=True)

# Filter dataframe to only IVSCC Group 2017-Current
master_jem_df = master_jem_df[master_jem_df["jem-id_rig_user"].isin(data_variables["ivscc_rig_users_list"])]
master_jem_df = master_jem_df[master_jem_df["jem-id_rig_number"].isin(data_variables["ivscc_rig_numbers_list"])]

#Fix depth/time column and combining into one column
jem_version_109_df = master_jem_df[master_jem_df["formVersion"] == "1.0.9"]
jem_version_other_df = master_jem_df[master_jem_df["formVersion"] != "1.0.9"]
# Drop necessary columns for concat
jem_version_109_df.drop(columns=["jem-depth", "jem-time_exp_retraction_end"], inplace=True)
jem_version_other_df.drop(columns=["jem-depth_old", "jem-time_exp_retraction_end_old"], inplace=True)
# Rename necessary columns for concat
jem_version_109_df.rename(columns={"jem-depth_old": "jem-depth", "jem-time_exp_retraction_end_old": "jem-time_exp_retraction_end"}, inplace=True)
# Concat df
master_jem_df = pd.concat([jem_version_109_df, jem_version_other_df], sort=True)

# Clean and add date_fields
master_jem_df = clean_date_field(master_jem_df)
# Clean time and add duration fields
master_jem_df = clean_time_field(master_jem_df)
# Clean numerical fields
master_jem_df = clean_num_field(master_jem_df)
# Clean and add roi fields
master_jem_df = clean_roi_field(master_jem_df)
# Replace value in fields
master_jem_df = replace_value(master_jem_df)
# Add patch tube field
master_jem_df = add_jem_patch_tube_field(master_jem_df)
# Add species field
master_jem_df = add_jem_species_field(master_jem_df)
# Add post patch status field
master_jem_df = add_jem_post_patch_status_field(master_jem_df)

# Drop columns
master_jem_df.drop(columns=data_variables["drop_list"], inplace=True)

# Sort columns
master_jem_df = master_jem_df.reindex(columns=data_variables["column_order_list"])
master_jem_df.sort_values(by=["jem-date_patch_y-m-d", "jem-id_slice_specimen", "jem-id_cell_specimen", "jem-status_attempt"], inplace=True)

# Dataframe to csvs and excel
master_jem_df.to_csv(path_or_buf=os.path.join(path_output, "master_jem.csv"), index=False)
master_jem_df.to_excel(excel_writer=os.path.join(path_output, "master_jem.xlsx"), index=False)
