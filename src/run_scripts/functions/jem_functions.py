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


# Imports
import json
import pandas as pd
import os
import numpy as np

from datetime import datetime, date, timedelta
from functions.jem_data_set import JemDataSet


# Read json data from file to import jem_dictionary
with open("C:/Users/ramr/Documents/Github/ai_repos/ephys_analysis_tools/src/constants/data_variables.json") as json_file:
    data_variables = json.load(json_file)


#-----Clean-up JEM fields-----#
def clean_date_field(df):
	"""
	1) Cleans up date fields in JEM metadata. 

	df: a pandas dataframe (default=jem_df)
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
    1) Cleans up roi field in JEM metadata.
    2) Creates roi_super, roi_major, roi_minor fields.

    df: a pandas dataframe (default=jem_df)
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
    1) Cleans up time fields in JEM metadata.
    2) Creates duration fields

    df: a pandas dataframe (default=jem_df)
    """

	# Remove timezones from time columns 
	#for col in data_variables["columns_time_list"]:
	#    split_time_zone = df[col].str.split(" ", n=1, expand=True) # Splitting time and timezone into 2 columns
	#    df[col] = split_time_zone[0] # Choosing column with only the time
	# Removing timezones with string slicing
	df["jem-time_patch"] = df["jem-time_patch"].str[0:8]
	df["jem-time_exp_approach_start"] = df["jem-time_exp_approach_start"].str[0:8]
	df["jem-time_exp_whole_cell_start"] = df["jem-time_exp_whole_cell_start"].str[0:8]
	df["jem-time_exp_extraction_start"] = df["jem-time_exp_extraction_start"].str[0:8]
	df["jem-time_exp_extraction_end"] = df["jem-time_exp_extraction_end"].str[0:8]
	df["jem-time_exp_retraction_end"] = df["jem-time_exp_retraction_end"].str[0:8]
	df["jem-time_exp_channel_end"] = df["jem-time_exp_channel_end"].str[0:8]
    # Create duration fields
	df["jem-time_duration_experiment"] = pd.to_datetime(df["jem-time_exp_extraction_start"]) - pd.to_datetime(df["jem-time_exp_whole_cell_start"])
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
	Clean numerical fields.
	"""

	# Convert field to integer field
	df["jem-id_rig_number"] = df["jem-id_rig_number"].astype(int)
	#df["jem-health_cell"] = df["jem-health_cell"].astype(int)
	#df["jem-status_attempt"] = df["jem-status_attempt"].astype(int)
	# Convert string field to float field and apply absolute value to fields
	df["jem-depth"] = pd.to_numeric(df["jem-depth"], errors='coerce').abs()
	df["jem-pressure_extraction"] = pd.to_numeric(df["jem-pressure_extraction"], errors='coerce').abs()
	df["jem-pressure_retraction"] = pd.to_numeric(df["jem-pressure_retraction"], errors='coerce').abs()
	df["jem-res_initial_seal"] = pd.to_numeric(df["jem-res_initial_seal"], errors='coerce').abs()
	df["jem-res_final_seal"] = pd.to_numeric(df["jem-res_final_seal"], errors='coerce').abs()
	# Convert to float
	df["jem-depth"] = df["jem-depth"].astype(float)
	df["jem-pressure_extraction"] = df["jem-pressure_extraction"].astype(float)
	df["jem-pressure_retraction"] = df["jem-pressure_retraction"].astype(float)
	df["jem-res_initial_seal"] = df["jem-res_initial_seal"].astype(float)
	df["jem-res_final_seal"] = df["jem-res_final_seal"].astype(float)
	# Round decimal places to 1
	df["jem-depth"] = df["jem-depth"].round(1)
	df["jem-pressure_extraction"] = df["jem-pressure_extraction"].round(1)
	df["jem-pressure_retraction"] = df["jem-pressure_retraction"].round(1)
	df["jem-res_initial_seal"] = df["jem-res_initial_seal"].round(1)
	df["jem-res_final_seal"] = df["jem-res_final_seal"].round(1)

	return df


def replace_value(df):
	"""
	Replace values in fields.	
	"""

	# Replace values in columns
	df["jem-status_misinformation"] = df["jem-status_misinformation"].replace({np.nan: "No"})

	df["jem-health_cell"] = df["jem-health_cell"].replace({"None": np.nan})

	df["jem-status_success_failure"] = df["jem-status_success_failure"].replace({"SUCCESS (high confidence)": "SUCCESS", "NO ATTEMPTS": "FAILURE", "Failure": "FAILURE"})
	df["jem-status_reporter"] = df["jem-status_reporter"].replace({"Cre+": "Positive", "Cre-": "Negative", "human": np.nan, "None": np.nan})

	df["jem-virus_enhancer"] = df["jem-virus_enhancer"].replace({np.nan: "None"})
	df["jem-project_level_nucleus"] = df["jem-project_level_nucleus"].replace({np.nan: "None"})
	df["jem-project_name"] = df["jem-project_name"].replace({np.nan: "None"})
	df["jem-project_retrograde_labeling_hemisphere"] = df["jem-project_retrograde_labeling_hemisphere"].replace({np.nan: "None"})
	df["jem-project_retrograde_labeling_region"] = df["jem-project_retrograde_labeling_region"].replace({np.nan: "None"})
	df["jem-project_retrograde_labeling_exp"] = df["jem-project_retrograde_labeling_exp"].replace({np.nan: "None"})
	
	return df


#-----Add new JEM fields-----#
def add_jem_patch_tube_field(df):
	"""
	1) Creates a patch tube field in JEM metadata.

	df: a pandas dataframe
	"""

	df["jem-status_patch_tube"] = np.where((df["jem-id_patched_cell_container"].str.startswith("P"))&(df["jem-status_success_failure"] == "SUCCESS"), "Patch Tube",
								  np.where((df["jem-id_patched_cell_container"] == "NA")&(df["jem-status_success_failure"] == "SUCCESS"), "No Tube", "No Experiment"))

	return df


def add_jem_species_field(df):
	"""
	1) Creates a species field in JEM metadata.

	df: a pandas dataframe
	"""

	df["jem-id_species"] = np.where(df["jem-id_slice_specimen"].str.startswith(tuple(["H1", "H2"])), "Human",
						   np.where(df["jem-id_slice_specimen"].str.startswith(tuple(["QM", "Q20.26.007", "Q21.26.003", "Q21.26.017", "Q21.26.019", "Q21.26.023"])), "NHP-Macaca mulatta",
	                       np.where(df["jem-id_slice_specimen"].str.startswith(tuple(["QN", "Q19", "Q20.26.001", "Q20.26.009", "Q21.26.005", "Q21.26.006", "Q21.26.008", "Q21.26.009", "Q21.26.013", "Q21.26.015", "Q21.26.021"])), "NHP-Macaca nemestrina",
	                       np.where(df["jem-id_slice_specimen"].str.startswith("SC2"), "NHP-Saimiri sciureus", "Mouse"))))

	return df


def add_jem_post_patch_status_field(df):
	"""
	1) Creates a post patch status field in JEM metadata.

	df: a pandas dataframe
	"""

	df["jem-nucleus_post_patch_detail"] = np.where(((df["jem-nucleus_post_patch"]=="nucleus_present")|(df["jem-nucleus_post_patch"]=="entire_cell"))&(df["jem-res_final_seal"]>=1000), "Nuc-giga-seal",
                                          np.where(((df["jem-nucleus_post_patch"]=="nucleus_present")|(df["jem-nucleus_post_patch"]=="entire_cell"))&(df["jem-res_final_seal"]<1000), "Nuc-low-seal",
                                          np.where(df["jem-nucleus_post_patch"]=="nucleus_absent", "No-seal",
                                          np.where(df["jem-nucleus_post_patch"]=="unknown", "Unknown", "Not applicable"))))

	return df


#-----Agata's code-----#
def flatten_jem_data(jem_paths, start_day_str, end_day_str):
    """Compiles JEM files from paths, returning a pandas dataframe.
    
    Parameters
    ----------
    jem_paths : list of strings

    start_day_str : string

    end_day_str : string

    Returns
    -------
    jem_df : pandas dataframe
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