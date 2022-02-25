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


# Read json data from file to import jem_dictionary
with open("C:/Users/ramr/Documents/Github/ai_repos/ephys_analysis_tools/src/jem_templates/python_files/data_variables.json") as json_file:
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

# Rename columns based on jem_dictionary
master_jem_df.rename(columns=data_variables["jem_dictionary"], inplace=True)

# Filter dataframe to only IVSCC Group 2017-Current
master_jem_df = master_jem_df[master_jem_df["jem-id_rig_user"].isin(data_variables["ivscc_rig_users_list"])]
master_jem_df = master_jem_df[master_jem_df["jem-id_rig_number"].isin(data_variables["ivscc_rig_numbers_list"])]

#Fix depth/time column and combining into one column
v_109_df = master_jem_df[master_jem_df["formVersion"] == "1.0.9"]
v_other_df = master_jem_df[master_jem_df["formVersion"] != "1.0.9"]
v_109_df.rename(columns={"jem-depth_old": "jem-depth", "jem-time_exp_end_old": "jem-time_exp_retraction_end"}, inplace=True)
v_other_df.rename(columns={"jem-depth_current": "jem-depth", "jem-time_exp_retraction_end_current": "jem-time_exp_retraction_end"}, inplace=True)
master_jem_df = pd.concat([v_109_df, v_other_df], sort=True)

# Fix datetime column to only date column
split_date = master_jem_df["jem-date_patch"].str.split(" ", n=1, expand=True) # Splitting date and time into 2 columns
master_jem_df["jem-date_patch"] = split_date[0] # Choosing column with only the dates

# Remove timezones from time columns 
for col in data_variables["columns_time_list"]:
    split_timezone = master_jem_df[col].str.split(" ", n=1, expand=True) # Splitting time and timezone into 2 columns
    master_jem_df[col] = split_timezone[0] # Choosing column with only the time

# Add in year, month, day columns
master_jem_df["jem-date_patch_y-m-d"] = master_jem_df["jem-date_patch"]
split_date = master_jem_df["jem-date_patch_y-m-d"].str.split("-", n=2, expand=True) # Splitting year, month and day
master_jem_df["jem-date_patch_y"] = split_date[0] # Choosing column with years
master_jem_df["jem-date_patch_m"] = split_date[1] # Choosing column with months
master_jem_df["jem-date_patch_d"] = split_date[2] # Choosing column with days

# Change date columns to a datetime
master_jem_df["jem-date_acsf"] = pd.to_datetime(master_jem_df["jem-date_acsf"])
master_jem_df["jem-date_blank"] = pd.to_datetime(master_jem_df["jem-date_blank"])
master_jem_df["jem-date_internal"] = pd.to_datetime(master_jem_df["jem-date_internal"])
master_jem_df["jem-date_patch"] = pd.to_datetime(master_jem_df["jem-date_patch"])

# Change date column format to MM/DD/YYYY
master_jem_df["jem-date_acsf"] = master_jem_df["jem-date_acsf"].dt.strftime("%m/%d/%Y")
master_jem_df["jem-date_blank"] = master_jem_df["jem-date_blank"].dt.strftime("%m/%d/%Y")
master_jem_df["jem-date_internal"] = master_jem_df["jem-date_internal"].dt.strftime("%m/%d/%Y")
master_jem_df["jem-date_patch"] = master_jem_df["jem-date_patch"].dt.strftime("%m/%d/%Y")

# Convert string column to float column and apply absolute value to columns
master_jem_df["jem-depth"] = pd.to_numeric(master_jem_df["jem-depth"], errors='coerce').abs()
master_jem_df["lims-depth"] = pd.to_numeric(master_jem_df["lims-depth"], errors='coerce').abs()
master_jem_df["jem-pressure_extraction"] = pd.to_numeric(master_jem_df["jem-pressure_extraction"], errors='coerce').abs()
master_jem_df["jem-pressure_retraction"] = pd.to_numeric(master_jem_df["jem-pressure_retraction"], errors='coerce').abs()
master_jem_df["jem-res_initial_seal"] = pd.to_numeric(master_jem_df["jem-res_initial_seal"], errors='coerce').abs()
master_jem_df["jem-res_final_seal"] = pd.to_numeric(master_jem_df["jem-res_final_seal"], errors='coerce').abs()

# Create duration columns
master_jem_df["jem-time_duration_experiment"] = pd.to_datetime(master_jem_df["jem-time_exp_extraction_start"]) - pd.to_datetime(master_jem_df["jem-time_exp_whole_cell_start"])
master_jem_df["jem-time_duration_extraction"] = pd.to_datetime(master_jem_df["jem-time_exp_extraction_end"]) - pd.to_datetime(master_jem_df["jem-time_exp_extraction_start"])
master_jem_df["jem-time_duration_retraction"] = pd.to_datetime(master_jem_df["jem-time_exp_retraction_end"]) - pd.to_datetime(master_jem_df["jem-time_exp_extraction_end"])
master_jem_df["jem-time_duration_experiment"] = (master_jem_df["jem-time_duration_experiment"].astype('timedelta64[s]'))/60
master_jem_df["jem-time_duration_extraction"] = (master_jem_df["jem-time_duration_extraction"].astype('timedelta64[s]'))/60
master_jem_df["jem-time_duration_retraction"] = (master_jem_df["jem-time_duration_retraction"].astype('timedelta64[s]'))/60

# Convert to float
master_jem_df["jem-time_duration_experiment"] = master_jem_df["jem-time_duration_experiment"].astype(float)
master_jem_df["jem-time_duration_extraction"] = master_jem_df["jem-time_duration_extraction"].astype(float)
master_jem_df["jem-time_duration_retraction"] = master_jem_df["jem-time_duration_retraction"].astype(float)

# Round decimal places to 2
master_jem_df["jem-time_duration_experiment"] = master_jem_df["jem-time_duration_experiment"].round(2)
master_jem_df["jem-time_duration_extraction"] = master_jem_df["jem-time_duration_extraction"].round(2)
master_jem_df["jem-time_duration_retraction"] = master_jem_df["jem-time_duration_retraction"].round(2)

# Test columns
master_jem_df["test-mismatch_depth"] = master_jem_df["jem-depth"] == master_jem_df["lims-depth"]
master_jem_df["test-mismatch_id_cell_specimen"] = master_jem_df["jem-id_cell_specimen"] == master_jem_df["lims-id_cell_specimen"]

# Replace values in column (roi-major_minor)
master_jem_df["jem-roi_major_minor"] = master_jem_df["jem-roi_major_minor"].replace({"layer ": "L"}, regex=True)
master_jem_df["jem-roi_major_minor"] = master_jem_df["jem-roi_major_minor"].replace({"/": "-"}, regex=True)
master_jem_df["jem-roi_major_minor"] = master_jem_df["jem-roi_major_minor"].replace({"MH": "EPIMH",
                                                                                     "LH": "EPILH",
                                                                                     "HIPCA1": "HIP_CA1",
                                                                                     "HIPDG-mo": "HIP_DG-mo",
                                                                                     "HIPDG-sg": "HIP_DG-sg",
                                                                                     "RSP1": "RSP_L1",
                                                                                     "RSP2-3": "RSP_L2-3",
                                                                                     "RSP5": "RSP_L5",
                                                                                     "RSP6a": "RSP_L6a",
                                                                                     "RSP6b": "RSP_L6b",
                                                                                     "TCx, L2": "TCx2",
                                                                                     "TCx, L2-3": "TCx2-3",
                                                                                     "TCx, L3": "TCx3",
                                                                                     "TCx, L5": "TCx5",
                                                                                     "CB": "CBXmo"}, regex=False)
master_jem_df["jem-roi_major_minor"] = master_jem_df["jem-roi_major_minor"].replace(data_variables["roi_dictionary"], regex=True)

# Creating jem-roi_major and jem-roi_minor columns
roi = master_jem_df["jem-roi_major_minor"].str.split("_", n=1, expand=True) # Splitting roi_major and roi_minor
master_jem_df["jem-roi_major"] = roi[0] # Choosing column with roi_major
master_jem_df["jem-roi_minor"] = roi[1] # Choosing column with roi_minor

# Creating jem-roi_super column
master_jem_df["jem-roi_super"] = master_jem_df["jem-roi_major"].replace({roi_cor: "Cortical" for roi_cor in data_variables["cortical_list"]}, regex=True)
master_jem_df["jem-roi_super"] = master_jem_df["jem-roi_super"].replace({roi_sub: "Subcortical" for roi_sub in data_variables["subcortical_list"]}, regex=True)
master_jem_df["jem-roi_super"] = master_jem_df["jem-roi_super"].replace({"NA": "Unknown"}, regex=True)

# Replace values in columns
master_jem_df["jem-health_cell"] = master_jem_df["jem-health_cell"].replace({"None": np.nan})
master_jem_df["jem-project_name"] = master_jem_df["jem-project_name"].replace({np.nan: "None"})
master_jem_df["jem-health_slice_initial"] = master_jem_df["jem-health_slice_initial"].replace({"Damaged": "Damage (Tissue Processing)", "Good": "Healthy","Wave of Death": "Wave of Death (after 30 min)", "'Wave of Death'": "Wave of Death (after 30 min)"})
master_jem_df["jem-status_reporter"] = master_jem_df["jem-status_reporter"].replace({"Cre+": "Positive", "Cre-": "Negative", "human": np.nan, "None": np.nan})

# Convert column to integer column
master_jem_df["jem-health_cell"] = master_jem_df["jem-health_cell"].fillna(value=0)
master_jem_df["jem-status_attempt"] = master_jem_df["jem-status_attempt"].fillna(value=0)
master_jem_df["jem-health_cell"] = master_jem_df["jem-health_cell"].astype(int)
master_jem_df["jem-status_attempt"] = master_jem_df["jem-status_attempt"].astype(int)
master_jem_df["jem-id_rig_number"] = master_jem_df["jem-id_rig_number"].astype(int)

# Add new columns
master_jem_df["jem-nucleus_post_patch_detail"] = np.where(((master_jem_df["jem-nucleus_post_patch"]=="nucleus_present")|(master_jem_df["jem-nucleus_post_patch"]=="entire_cell"))&(master_jem_df["jem-res_final_seal"]>=1000), "Nuc-giga-seal",
                                                 np.where(((master_jem_df["jem-nucleus_post_patch"]=="nucleus_present")|(master_jem_df["jem-nucleus_post_patch"]=="entire_cell"))&(master_jem_df["jem-res_final_seal"]<1000), "Nuc-low-seal",
                                                 np.where(master_jem_df["jem-nucleus_post_patch"]=="nucleus_absent", "No-seal",
                                                 np.where(master_jem_df["jem-nucleus_post_patch"]=="unknown", "Unknown", "Not applicable"))))
master_jem_df["jem-id_species"] = np.where(master_jem_df["jem-id_slice_specimen"].str.startswith(tuple(["H1", "H2"])), "Human",
                                  np.where(master_jem_df["jem-id_slice_specimen"].str.startswith(tuple(["Q20.26.007", "Q21.26.003", "Q21.26.017", "Q21.26.019", "Q21.26.023"])), "NHP (Macaca mulatta)",
                                  np.where(master_jem_df["jem-id_slice_specimen"].str.startswith(tuple(["QN", "Q19", "Q20.26.001", "Q20.26.009", "Q21.26.005", "Q21.26.006", "Q21.26.008", "Q21.26.009", "Q21.26.013", "Q21.26.015", "Q21.26.021"])), "NHP (Macaca nemestrina)",
                                  np.where(master_jem_df["jem-id_slice_specimen"].str.startswith("SC2"), "NHP (Saimiri sciureus)", "Mouse"))))

# Drop columns
master_jem_df.drop(columns=data_variables["drop_list"], inplace=True)

# Sort columns
master_jem_df = master_jem_df.reindex(columns=data_variables["column_order_list"])
master_jem_df.sort_values(by=["jem-date_patch_y-m-d", "jem-id_slice_specimen", "jem-id_cell_specimen", "jem-status_attempt"], inplace=True)

# Dataframe to csvs and excel
master_jem_df.to_csv(path_or_buf=os.path.join(path_output, "master_jem.csv"), index=False)
master_jem_df.to_excel(excel_writer=os.path.join(path_output, "master_jem.xlsx"), index=False)
