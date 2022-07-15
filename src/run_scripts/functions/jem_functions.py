"""
---------------------------------------------------------------------
File name: jem_functions.py
Maintainer: Ramkumar Rajanbabu
---------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 04/07/2022
Description: JEM related functions
---------------------------------------------------------------------
"""


#-----Imports-----#
# General imports
import json
import numpy as np
import os
import pandas as pd
from datetime import datetime, date, timedelta
# File imports
from functions.file_functions import get_jsons, load_data_variables
from functions.jem_data_set import JemDataSet


# Load json file
data_variables = load_data_variables()


def generate_jem_df(group, filter_tubes=None):
	"""
	Generates a jem metadata dataframe with the previous 30 days of information.
	Specifically, used for daily and weekly transcriptomics reports.

	Parameters:
		group (string): "ivscc" or "hct".
		filter_tubes (string): None (default) or "only_patch_tubes" to filter dataframe to only patched cell containers.

	Returns:
		jem_df (dataframe): a pandas dataframe.
	"""

	# Date of today
	dt_today = datetime.today()
	date_today = dt_today.date()
	day_today = date_today.strftime("%y%m%d") # "YYMMDD"
	# Date of the previous 30 days from date of today
	date_prev_30d = date_today - timedelta(days=30)
	day_prev_30d = date_prev_30d.strftime("%y%m%d") # "YYMMDD"

	# Directories
	if group == "ivscc":
		json_dir  = "//allen/programs/celltypes/workgroups/279/Patch-Seq/all-metadata-files"
	if group == "hct":
		json_dir  = "//allen/programs/celltypes/workgroups/hct/HCT_Ephys_Data/JEM_forms"

	delta_mod_date = (date_today - date_prev_30d).days + 3
	jem_paths = get_jsons(dirname=json_dir, expt="PS", delta_days=delta_mod_date)
	# Flatten JSON files (previous 30 day information) to pandas dataframe 9jem_df)
	jem_df = flatten_jem_data(jem_paths, day_prev_30d, day_today)

	# Rename columns based on jem_dictionary
	jem_df.rename(columns=data_variables["jem_dictionary"], inplace=True)

	if group == "ivscc":
		# Filter dataframe to only IVSCC Group 2017-Current
		jem_df = jem_df[jem_df["jem-id_rig_user"].isin(data_variables["ivscc_rig_users_list"])]
		jem_df = jem_df[jem_df["jem-id_rig_number"].isin(data_variables["ivscc_rig_numbers_list"])]
	if group == "hct":
		# Filter dataframe to only HCT Group 
		jem_df = jem_df[jem_df["jem-id_rig_user"].isin(data_variables["hct_rig_users_list"])]
		jem_df = jem_df[jem_df["jem-id_rig_number"].isin(data_variables["hct_rig_numbers_list"])]

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

	# Filter to only successful experiments
	jem_df = jem_df[(jem_df["jem-status_success_failure"] == "SUCCESS")]
	# Filters dataframe to only patched cell containers
	if filter_tubes == "only_patch_tubes":
		jem_df = jem_df[(jem_df["jem-status_patch_tube"] == "Patch Tube")]

	return jem_df


#-----Clean-up JEM fields-----#
def clean_date_field(df):
	"""
	Cleans up date fields in JEM metadata.

	Parameters: 
		df (dataframe): a pandas dataframe.

	Returns:
		df (dataframe): a pandas dataframe.
	"""

	# Split datetime field into date and time field
	split_date_time = df["jem-date_patch"].str.split(" ", n=1, expand=True) # Splitting date and time into 2 columns
	df["jem-date_patch"] = split_date_time[0] # Choosing column with only the dates
	df["jem-time_patch"] = split_date_time[1] # Choosing column with only the times
	# Remove timezones from time field 
	#for time_value in df["jem-time_patch"]:
	#	split_timezone = df["jem-time_patch"].str.split(" ", n=1, expand=True) # Splitting time and timezone into 2 columns
	#	df["jem-time_patch"] = split_timezone[0] # Choosing column with only the time
	# Duplicate date field
	df["jem-date_patch_y-m-d"] = df["jem-date_patch"]
	# Split date field and add in year, month, day fields
	split_date = df["jem-date_patch_y-m-d"].str.split("-", n=2, expand=True) # Splitting year, month and day
	df["jem-date_patch_y"] = split_date[0] # Choosing column with years
	df["jem-date_patch_m"] = split_date[1] # Choosing column with months
	df["jem-date_patch_d"] = split_date[2] # Choosing column with days
	# Change date fields to a datetime
	df["jem-date_acsf"] = pd.to_datetime(df["jem-date_acsf"])
	df["jem-date_blank"] = pd.to_datetime(df["jem-date_blank"])
	df["jem-date_internal"] = pd.to_datetime(df["jem-date_internal"])
	df["jem-date_patch"] = pd.to_datetime(df["jem-date_patch"])
	# Change date fields format to MM/DD/YYYY
	df["jem-date_acsf"] = df["jem-date_acsf"].dt.strftime("%m/%d/%Y")
	df["jem-date_blank"] = df["jem-date_blank"].dt.strftime("%m/%d/%Y")
	df["jem-date_internal"] = df["jem-date_internal"].dt.strftime("%m/%d/%Y")
	df["jem-date_patch"] = df["jem-date_patch"].dt.strftime("%m/%d/%Y")

	return df


def clean_roi_field(df):
	"""
	Cleans up roi fields and adds new roi fields in JEM metadata.

	Parameters: 
		df (dataframe): a pandas dataframe.

	Returns:
		df (dataframe): a pandas dataframe.
	"""

	# Replace values in column (roi-major_minor)
	df["jem-roi_major_minor"] = df["jem-roi_major_minor"].replace({"layer ": "L", "/": "-"}, regex=True)
	df["jem-roi_major_minor"] = df["jem-roi_major_minor"].replace(data_variables["roi_dictionary_regex_false"], regex=False)
	df["jem-roi_major_minor"] = df["jem-roi_major_minor"].replace(data_variables["roi_dictionary"], regex=True)
	# Creating roi_major and roi_minor columns
	roi = df["jem-roi_major_minor"].str.split("_", n=1, expand=True) # Splitting roi_major and roi_minor
	df["jem-roi_major"] = roi[0] # Choosing column with roi_major
	df["jem-roi_minor"] = roi[1] # Choosing column with roi_minor
	# Creating roi_super column
	df["jem-roi_super"] = df["jem-roi_major"].replace({roi_cor: "Cortical" for roi_cor in data_variables["cortical_list"]}, regex=True)
	df["jem-roi_super"] = df["jem-roi_super"].replace({roi_sub: "Subcortical" for roi_sub in data_variables["subcortical_list"]}, regex=True)
	df["jem-roi_super"] = df["jem-roi_super"].replace({"NA": "Unknown"}, regex=True)

	return df


def clean_time_field(df):
	"""
	Cleans up time fields and adds new duration fields in JEM metadata.

	Parameters: 
		df (dataframe): a pandas dataframe.

	Returns:
		df (dataframe): a pandas dataframe.
	"""

	# Remove timezones from time columns 
	#for col in data_variables["columns_time_list"]:
	#    split_time_zone = df[col].str.split(" ", n=1, expand=True) # Splitting time and timezone into 2 columns
	#    df[col] = split_time_zone[0] # Choosing column with only the time
	# Removing timezones with string slicing
	df["jem-time_patch"] = df["jem-time_patch"].str[0:8]
	df["jem-in_bath_time_start"] = df["jem-in_bath_time_start"].str[0:8]
	df["jem-break_in_time_end"] = df["jem-break_in_time_end"].str[0:8]
	df["jem-time_exp_extraction_start"] = df["jem-time_exp_extraction_start"].str[0:8]
	df["jem-time_exp_extraction_end"] = df["jem-time_exp_extraction_end"].str[0:8]
	df["jem-time_exp_retraction_end"] = df["jem-time_exp_retraction_end"].str[0:8]
	if "jem-time_exp_channel_end" in df.columns:
		df["jem-time_exp_channel_end"] = df["jem-time_exp_channel_end"].str[0:8]
    # Create duration fields
	df["jem-time_duration_experiment"] = pd.to_datetime(df["jem-time_exp_extraction_start"]) - pd.to_datetime(df["jem-break_in_time_end"])
	df["jem-time_duration_extraction"] = pd.to_datetime(df["jem-time_exp_extraction_end"]) - pd.to_datetime(df["jem-time_exp_extraction_start"])
	df["jem-time_duration_retraction"] = pd.to_datetime(df["jem-time_exp_retraction_end"]) - pd.to_datetime(df["jem-time_exp_extraction_end"])
	# Change to seconds
	df["jem-time_duration_experiment"] = (df["jem-time_duration_experiment"].astype("timedelta64[s]"))/60
	df["jem-time_duration_extraction"] = (df["jem-time_duration_extraction"].astype("timedelta64[s]"))/60
	df["jem-time_duration_retraction"] = (df["jem-time_duration_retraction"].astype("timedelta64[s]"))/60
	# Convert to float
	df["jem-time_duration_experiment"] = df["jem-time_duration_experiment"].astype(float)
	df["jem-time_duration_extraction"] = df["jem-time_duration_extraction"].astype(float)
	df["jem-time_duration_retraction"] = df["jem-time_duration_retraction"].astype(float)
	# Round decimal places to 2
	df["jem-time_duration_experiment"] = df["jem-time_duration_experiment"].round(2)
	df["jem-time_duration_extraction"] = df["jem-time_duration_extraction"].round(2)
	df["jem-time_duration_retraction"] = df["jem-time_duration_retraction"].round(2)

	return df


def clean_num_field(df):
	"""
	Cleans up numerical fields in JEM metadata.

	Parameters: 
		df (dataframe): a pandas dataframe.

	Returns:
		df (dataframe): a pandas dataframe.
	"""

	# Convert field to integer field
	#df["jem-id_rig_number"] = df["jem-id_rig_number"].astype(int)
	#df["jem-status_attempt"] = df["jem-status_attempt"].astype(int)
	# Convert string field to float field and apply absolute value to fields
	df["jem-depth"] = pd.to_numeric(df["jem-depth"], errors='coerce').abs()
	df["jem-pressure_extraction"] = pd.to_numeric(df["jem-pressure_extraction"], errors='coerce').abs()
	df["jem-pressure_retraction"] = pd.to_numeric(df["jem-pressure_retraction"], errors='coerce').abs()
	df["jem-in_bath_resistance"] = pd.to_numeric(df["jem-in_bath_resistance"], errors='coerce').abs()
	df["jem-res_final_seal"] = pd.to_numeric(df["jem-res_final_seal"], errors='coerce').abs()
	# Convert to float
	df["jem-depth"] = df["jem-depth"].astype(float)
	df["jem-pressure_extraction"] = df["jem-pressure_extraction"].astype(float)
	df["jem-pressure_retraction"] = df["jem-pressure_retraction"].astype(float)
	df["jem-in_bath_resistance"] = df["jem-in_bath_resistance"].astype(float)
	df["jem-res_final_seal"] = df["jem-res_final_seal"].astype(float)
	# Round decimal places to 1
	df["jem-depth"] = df["jem-depth"].round(1)
	df["jem-pressure_extraction"] = df["jem-pressure_extraction"].round(1)
	df["jem-pressure_retraction"] = df["jem-pressure_retraction"].round(1)
	df["jem-in_bath_resistance"] = df["jem-in_bath_resistance"].round(1)
	df["jem-res_final_seal"] = df["jem-res_final_seal"].round(1)

	return df


def get_project_channel(row):
    """
    Merges old and new channel recording project fields into one field.

	Parameters: 
		row: the row of a pandas dataframe.

	Returns:
		Channel_Recording (string)
		None (string)
	"""
    
    project_name = row["jem-project_name"]
    project_nucleus = row["jem-project_level_nucleus"]
    if project_nucleus == "Channel_Recording":
        return project_nucleus
    if project_name == "Channel_Recording":
        return "Channel_Recording"
    else:
        return "None"


def replace_value(df):
	"""
	Replaces the values of fields in JEM metadata.

	Parameters: 
		df (dataframe): a pandas dataframe.

	Returns:
		df (dataframe): a pandas dataframe.
	"""

	# Replace values in columns
	if "jem-status_misinformation" in df.columns:
		df["jem-status_misinformation"] = df["jem-status_misinformation"].replace({np.nan: "No"})
	if "jem-status_success_failure" in df.columns:
		df["jem-status_success_failure"] = df["jem-status_success_failure"].replace({"SUCCESS (high confidence)": "SUCCESS", "NO ATTEMPTS": "FAILURE", "Failure": "FAILURE"})
	if "jem-status_reporter" in df.columns:
		df["jem-status_reporter"] = df["jem-status_reporter"].replace({"Cre+": "Positive", "Cre-": "Negative", "human": np.nan, "None": np.nan})
	if "jem-virus_enhancer" in df.columns:
		df["jem-virus_enhancer"] = df["jem-virus_enhancer"].replace({np.nan: "None"})
	if "jem-project_level_nucleus" in df.columns:
		df["jem-project_level_nucleus"] = df["jem-project_level_nucleus"].replace({np.nan: "None"})
	if "jem-project_name" in df.columns:
		df["jem-project_name"] = df["jem-project_name"].replace({"Channel_Recording": "None", np.nan: "None"})
	if "jem-project_icv_injection_fluorescent_roi" in df.columns:
		df["jem-project_icv_injection_fluorescent_roi"] = df["jem-project_icv_injection_fluorescent_roi"].replace({np.nan: "None"})
	if "jem-project_retrograde_labeling_hemisphere" in df.columns:
		df["jem-project_retrograde_labeling_hemisphere"] = df["jem-project_retrograde_labeling_hemisphere"].replace({np.nan: "None"})
	if "jem-project_retrograde_labeling_region" in df.columns:
		df["jem-project_retrograde_labeling_region"] = df["jem-project_retrograde_labeling_region"].replace({np.nan: "None"})
	if "jem-project_retrograde_labeling_exp" in df.columns:
		df["jem-project_retrograde_labeling_exp"] = df["jem-project_retrograde_labeling_exp"].replace({np.nan: "None"})
	
	return df


#-----Add new JEM fields-----#
def add_jem_patch_tube_field(df):
	"""
	Adds a patch tube field in JEM metadata.

	Parameters: 
		df (dataframe): a pandas dataframe.

	Returns:
		df (dataframe): a pandas dataframe.
	"""

	df["jem-status_patch_tube"] = np.where((df["jem-id_patched_cell_container"].str.startswith("P"))&(df["jem-status_success_failure"] == "SUCCESS"), "Patch Tube",
								  np.where((df["jem-id_patched_cell_container"] == "NA")&(df["jem-status_success_failure"] == "SUCCESS"), "No Tube", "No Experiment"))

	return df


def add_jem_species_field(df):
	"""
	Adds a species field in JEM metadata.

	Parameters: 
		df (dataframe): a pandas dataframe.

	Returns:
		df (dataframe): a pandas dataframe.
	"""

	df["jem-id_species"] = np.where(df["jem-id_slice_specimen"].str.startswith(tuple(["H1", "H2"])), "Human",
						   np.where(df["jem-id_slice_specimen"].str.startswith(tuple(["QM", "Q20.26.007", "Q21.26.003", "Q21.26.017", "Q21.26.019", "Q21.26.023"])), "NHP-Macaca mulatta",
	                       np.where(df["jem-id_slice_specimen"].str.startswith(tuple(["QN", "Q19", "Q20.26.001", "Q20.26.009", "Q21.26.005", "Q21.26.006", "Q21.26.008", "Q21.26.009", "Q21.26.013", "Q21.26.015", "Q21.26.021"])), "NHP-Macaca nemestrina",
	                       np.where(df["jem-id_slice_specimen"].str.startswith(tuple(["SC2", "Q21.26.020"])), "NHP-Saimiri sciureus", "Mouse"))))

	return df


def add_jem_post_patch_status_field(df):
	"""
	Adds a post patch status field in JEM metadata.

	Parameters: 
		df (dataframe): a pandas dataframe.

	Returns:
		df (dataframe): a pandas dataframe.
	"""

	df["jem-nucleus_post_patch_detail"] = np.where(((df["jem-nucleus_post_patch"]=="nucleus_present")|(df["jem-nucleus_post_patch"]=="entire_cell"))&(df["jem-res_final_seal"]>=1000), "Nuc-giga-seal",
                                          np.where(((df["jem-nucleus_post_patch"]=="nucleus_present")|(df["jem-nucleus_post_patch"]=="entire_cell"))&(df["jem-res_final_seal"]<1000), "Nuc-low-seal",
                                          np.where(df["jem-nucleus_post_patch"]=="nucleus_absent", "No-seal",
                                          np.where(df["jem-nucleus_post_patch"]=="unknown", "Unknown", "Not applicable"))))

	return df


#-----Fix JEM version issues-----#
def fix_jem_versions(df):
	"""
	Fixes jem versions in JEM metadata.

	Parameters: 
		df (dataframe): a pandas dataframe.

	Returns:
		df (dataframe): a pandas dataframe.
	"""

	# Lists
	jem_version_109_list = ["1.0.9"]
	jem_version_210_list = ["1.0.9", "2.0.0", "2.0.1", "2.0.2", "2.0.3", "2.0.5", "2.0.6", "2.0.7", "2.0.8", "2.1.0"]
	jem_version_212_list = ["1.0.9", "2.0.0", "2.0.1", "2.0.2", "2.0.3", "2.0.5", "2.0.6", "2.0.7", "2.0.8", "2.1.0", "2.1.1", "2.1.2"]

	# Fix depth and time fields (jem version 1.1.0 and onwards))
	df_cur = df[~df["jem-version_jem_form"].isin(jem_version_109_list)].copy()
	df_old = df[df["jem-version_jem_form"].isin(jem_version_109_list)].copy()
	# Drop necessary fields for concatenating dataframes
	if "jem-depth_old" in df.columns:
		df_cur.drop(columns=["jem-depth_old", "jem-time_exp_retraction_end_old"], inplace=True)
		df_old.drop(columns=["jem-depth", "jem-time_exp_retraction_end"], inplace=True)
	# Rename necessary fields for concatenating dataframes
	df_old = df_old.rename(columns={"jem-depth_old": "jem-depth", "jem-time_exp_retraction_end_old": "jem-time_exp_retraction_end"})
	# Concatenate dataframes
	df = pd.concat([df_cur, df_old], sort=True)
	
	# Fix the blank date fields (jem version 2.1.1 and onwards)
	df_cur = df[~df["jem-version_jem_form"].isin(jem_version_210_list)].copy()
	df_old = df[df["jem-version_jem_form"].isin(jem_version_210_list)].copy()
	# Drop necessary fields for concatenating dataframes
	if "jem-date_blank_old" in df.columns:
		df_cur.drop(columns=["jem-date_blank_old"], inplace=True)
		df_old.drop(columns=["jem-date_blank"], inplace=True)
	# Rename necessary fields for concatenating dataframes
	df_old = df_old.rename(columns={"jem-date_blank_old": "jem-date_blank"})
	# Concatenate dataframes
	df = pd.concat([df_cur, df_old], sort=True)

	# Fix experiment section (jem version 2.1.3 and onwards)
	df_cur = df[~df["jem-version_jem_form"].isin(jem_version_212_list)].copy()
	df_old = df[df["jem-version_jem_form"].isin(jem_version_212_list)].copy()
	# Drop necessary fields for concatenating dataframes
	if "jem-in_bath_time_start_old" in df.columns:
		df_cur.drop(columns=["jem-in_bath_time_start_old", "jem-in_bath_resistance_old", "jem-break_in_time_end_old"], inplace=True)
		df_old.drop(columns=["jem-in_bath_time_start", "jem-in_bath_resistance", "jem-break_in_time_end"], inplace=True)
	# Rename necessary fields for concatenating dataframes
	df_old = df_old.rename(columns={"jem-in_bath_time_start_old": "jem-in_bath_time_start", "jem-in_bath_resistance_old": "jem-in_bath_resistance", "jem-break_in_time_end_old": "jem-break_in_time_end"})
	# Concatenate dataframes
	df = pd.concat([df_cur, df_old], sort=True)

	return df


#-----Agata's code-----#
def flatten_jem_data(jem_paths, start_day_str, end_day_str):
	"""
	Compiles JEM files from paths, returning a pandas dataframe.

	Parameters:
		jem_paths : list of strings
		start_day_str : string
		end_day_str : string

	Returns:
		jem_df (dataframe): a pandas dataframe.
	"""
	start_day = datetime.strptime(start_day_str, "%y%m%d").date()
	end_day = datetime.strptime(end_day_str, "%y%m%d").date()

	jem_df = pd.DataFrame()
	for jem_path in jem_paths:
	    jem = JemDataSet(jem_path)
	    expt_date = jem.get_experiment_date()
	    if (expt_date >= start_day.strftime("%Y-%m-%d")) and (expt_date <= end_day.strftime("%Y-%m-%d")):
	        slice_data = jem.get_data()
	        jem_df = pd.concat([jem_df, slice_data], axis=0, sort=True)
	jem_df.reset_index(drop=True, inplace=True)

	if len(jem_df) == 0:
	    print("No JEM data found for experiments between %s and %s" %(start_day_str, end_day_str))
	    #jem_df = pd.DataFrame(columns=output_cols)

	return jem_df
