"""
---------------------------------------------------------------------
File name: jem_metadata_clean_up.py
Maintainer: Ramkumar Rajanbabu
---------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 04/02/2022
Description: Template for cleaning up jem metadata dataframe
---------------------------------------------------------------------
"""

# Imports
import json
import pandas as pd
import os
import numpy as np

from datetime import datetime, date, timedelta
from funcs.jem_data_set import JemDataSet


# Read json data from file to import jem_dictionary
with open("C:/Users/ramr/Documents/Github/ai_repos/ephys_analysis_tools/src/constants/data_variables.json") as json_file:
    data_variables = json.load(json_file)


#*****Clean-up current fields*****#

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
	for time_value in df["jem-time_patch"]:
		split_timezone = df["jem-time_patch"].str.split(" ", n=1, expand=True) # Splitting time and timezone into 2 columns
		df["jem-time_patch"] = split_timezone[0] # Choosing column with only the time

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


def clean_time_field(df):
	"""
    1) Cleans up time fields in JEM metadata.
    2) Creates duration fields

    df: a pandas dataframe (default=jem_df)
    """

    # Create duration fields
	df["jem-time_duration_experiment"] = pd.to_datetime(df["jem-time_exp_extraction_start"]) - pd.to_datetime(df["jem-time_exp_whole_cell_start"])
	df["jem-time_duration_extraction"] = pd.to_datetime(df["jem-time_exp_extraction_end"]) - pd.to_datetime(df["jem-time_exp_extraction_start"])
	df["jem-time_duration_retraction"] = pd.to_datetime(df["jem-time_exp_retraction_end"]) - pd.to_datetime(df["jem-time_exp_extraction_end"])
	
	# Change to seconds
	df["jem-time_duration_experiment"] = (df["jem-time_duration_experiment"].astype('timedelta64[s]'))/60
	df["jem-time_duration_extraction"] = (df["jem-time_duration_extraction"].astype('timedelta64[s]'))/60
	df["jem-time_duration_retraction"] = (df["jem-time_duration_retraction"].astype('timedelta64[s]'))/60

	# Convert to float
	df["jem-time_duration_experiment"] = df["jem-time_duration_experiment"].astype(float)
	df["jem-time_duration_extraction"] = df["jem-time_duration_extraction"].astype(float)
	df["jem-time_duration_retraction"] = df["jem-time_duration_retraction"].astype(float)

	# Round decimal places to 2
	df["jem-time_duration_experiment"] = df["jem-time_duration_experiment"].round(2)
	df["jem-time_duration_extraction"] = df["jem-time_duration_extraction"].round(2)
	df["jem-time_duration_retraction"] = df["jem-time_duration_retraction"].round(2)

	return df


def clean_roi_field(df):
    """
    1) Cleans up roi field in JEM metadata.
    2) Creates roi_super, roi_major, roi_minor fields.

    df: a pandas dataframe (default=jem_df)
    """

    # Replace values in column (roi-major_minor)
    df["jem-roi_major_minor"] = df["jem-roi_major_minor"].replace({"layer ": "L"}, regex=True)
    df["jem-roi_major_minor"] = df["jem-roi_major_minor"].replace({"/": "-"}, regex=True)
    df["jem-roi_major_minor"] = df["jem-roi_major_minor"].replace({"MH": "EPIMH", "LH": "EPILH",
                                   "HIPCA1": "HIP_CA1", "HIPDG-mo": "HIP_DG-mo", "HIPDG-sg": "HIP_DG-sg",
                                   "RSP1": "RSP_L1", "RSP2-3": "RSP_L2-3", "RSP5": "RSP_L5", "RSP6a": "RSP_L6a", "RSP6b": "RSP_L6b",
                                   "VISp, layer 1": "VISp1", "VISp, layer 2/3": "VISp2/3", "VISp, layer 4": "VISp4", "VISp, layer 5": "VISp5", "VISp, layer 6a": "VISp6a", "VISp, layer 6b": "VISp6b",
                                   "FCx, layer 1": "FCx1", "FCx, layer 2": "FCx2", "FCx, layer 3": "FCx3", "FCx, layer 4": "FCx4", "FCx, layer 5": "FCx5",
                                   "TCx, L2": "TCx2", "TCx, L2-3": "TCx2-3", "TCx, L3": "TCx3", "TCx, L5": "TCx5",
                                   "CB": "CBXmo"}, regex=False)
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


#*****Add new fields*****#

def add_species_field(df):
	"""
	1) Creates a species field in JEM metadata.

	df: a pandas dataframe (default=jem_df)
	"""

	df["jem-id_species"] = np.where(df["jem-id_slice_specimen"].str.startswith(tuple(["H1", "H2"])), "Human",
						   np.where(df["jem-id_slice_specimen"].str.startswith(tuple(["Q20.26.007", "Q21.26.003", "Q21.26.017", "Q21.26.019", "Q21.26.023"])), "NHP (Macaca mulatta)",
	                       np.where(df["jem-id_slice_specimen"].str.startswith(tuple(["QN", "Q19", "Q20.26.001", "Q20.26.009", "Q21.26.005", "Q21.26.006", "Q21.26.008", "Q21.26.009", "Q21.26.013", "Q21.26.015", "Q21.26.021"])), "NHP (Macaca nemestrina)",
	                       np.where(df["jem-id_slice_specimen"].str.startswith("SC2"), "NHP (Saimiri sciureus)", "Mouse"))))

	return df


def add_post_patch_status_field(df):
	"""
	1) Creates a post patch status field in JEM metadata.

	df: a pandas dataframe (default=jem_df)
	"""

	df["jem-nucleus_post_patch_detail"] = np.where(((df["jem-nucleus_post_patch"]=="nucleus_present")|(df["jem-nucleus_post_patch"]=="entire_cell"))&(df["jem-res_final_seal"]>=1000), "Nuc-giga-seal",
                                                 np.where(((df["jem-nucleus_post_patch"]=="nucleus_present")|(df["jem-nucleus_post_patch"]=="entire_cell"))&(df["jem-res_final_seal"]<1000), "Nuc-low-seal",
                                                 np.where(df["jem-nucleus_post_patch"]=="nucleus_absent", "No-seal",
                                                 np.where(df["jem-nucleus_post_patch"]=="unknown", "Unknown", "Not applicable"))))

	return df


# Agata's code

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



