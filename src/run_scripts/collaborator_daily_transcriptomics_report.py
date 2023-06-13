"""
---------------------------------------------------------------------
File name: collaborator_daily_transcriptomics_report.py
Maintainer: Ramkumar Rajanbabu
---------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 05/19/2023
Description: Generate Collaborator daily transcriptomics report (excel document)
---------------------------------------------------------------------
"""


#-----Imports-----#
# General imports
import json
import numpy as np
import os
import pandas as pd
from datetime import datetime, date, timedelta
from pandas.tseries.offsets import CustomBusinessDay
from pandas.tseries.holiday import USFederalHolidayCalendar
from pathlib import Path, PureWindowsPath
from tkinter.filedialog import askdirectory
# File imports
from functions.file_functions import get_jsons, load_data_variables
from functions.jem_data_set import JemDataSet
from functions.io_functions import validated_input, validated_date_input,save_xlsx, get_jsons_walk
from functions.jem_functions import generate_jem_df


#-----General Information-----#
"""
project_dictionary details: New project codes (2021-present)
- 102-01-045-10: CTY IVSCC (Mouse/NHP) # IVSCC
- 102-01-061-20.3 : CTY BICAN Human and NHP Atlas # IVSCC
- 122-01-002-20.2.1 : AIND Thalamus U19 # IVSCC
- 102-04-006-20 : MSP Measuring Consciousness Ph2 (TBD) # HCT
- 102-01-051-20: CTY Neuromodulation of NHP Cell Types # HCT

project_dictionary details: Old project codes
- 102-01-020-20: CTY BRAIN Human Cell Types (Human Acute/Culture, U01 shipping pilot) # IVSCC (10/01/2017 - 6/03/2022) 
- 102-04-009-10: CTY SR: Targeted CNS Gene Therapy (Dravet pilot) # IVSCC (Dates?)
"""

#-----Variables-----#
# Load json file
data_variables = load_data_variables()
# Ask user for input on file directory location
json_dir = askdirectory()

# Compiling all JSON files from user chosen file directory
jem_paths = get_jsons_walk(dirname=json_dir, expt="PS")
jem_df = pd.DataFrame()
for jem_path in jem_paths:
    jem = JemDataSet(jem_path)
    slice_data = jem.get_data()
    if slice_data is None:
        continue
    success_slice_data = slice_data[slice_data["status"].str.contains("SUCCESS")]
    jem_df = pd.concat([jem_df, slice_data], axis=0, sort=True)
jem_df.reset_index(drop=True, inplace=True)

# Rename columns based on jem_dictionary
jem_df.rename(columns=data_variables["jem_dictionary"], inplace=True)

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


# Add patch tube field
jem_df = add_jem_patch_tube_field(jem_df)
# Filters dataframe to only patched cell containers
filter_tubes = "only_patch_tubes"
if filter_tubes == "only_patch_tubes":
	jem_df = jem_df[(jem_df["jem-status_patch_tube"] == "Patch Tube")]


def fix_jem_versions_collab(df):
	"""
	Fixes jem versions in JEM metadata.

	Parameters: 
		df (dataframe): a pandas dataframe.

	Returns:
		df (dataframe): a pandas dataframe.
	"""

	# Lists
	jem_version_208_list = ["2.0.8"]

	# Rename necessary fields for concatenating dataframes
	df = df.rename(columns={"jem-date_blank_old": "jem-date_blank"})

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


	# Replace values in column (roi-major_minor)
	df["jem-roi_major_minor"] = df["jem-roi_major_minor"].replace({"layer ": "L", "/": "-"}, regex=True)
	df["jem-roi_major_minor"] = df["jem-roi_major_minor"].replace(data_variables["roi_dictionary_regex_false"], regex=False)
	df["jem-roi_major_minor"] = df["jem-roi_major_minor"].replace(data_variables["roi_dictionary_regex_true"], regex=True)
	# Creating roi_major and roi_minor columns
	roi = df["jem-roi_major_minor"].str.split("_", n=1, expand=True) # Splitting roi_major and roi_minor
	df["jem-roi_major"] = roi[0] # Choosing column with roi_major
	df["jem-roi_minor"] = roi[1] # Choosing column with roi_minor
	# Creating roi_super column
	df["jem-roi_super"] = df["jem-roi_major"].replace({roi_cor: "Cortical" for roi_cor in data_variables["cortical_list"]}, regex=True)
	df["jem-roi_super"] = df["jem-roi_super"].replace({roi_sub: "Subcortical" for roi_sub in data_variables["subcortical_list"]}, regex=True)
	df["jem-roi_super"] = df["jem-roi_super"].replace({roi_bs: "Brainstem" for roi_bs in data_variables["brainstem_list"]}, regex=True)
	df["jem-roi_super"] = df["jem-roi_super"].replace({"NA": "Unknown"}, regex=True)

	return df

jem_df = fix_jem_versions_collab(jem_df)

# Lists
jem_fields = ["jem-date_patch", "jem-date_blank", "jem-id_rig_user", "jem-id_cell_specimen",
                "jem-id_patched_cell_container", "jem-roi_major", "jem-roi_minor",
                "jem-nucleus_post_patch",
                "jem-project_name", "jem-status_reporter", "prep_type"]

# Filter dataframe to specified fields
jem_df = jem_df[jem_fields]
# Sort by jem-id_patched_cell_container in ascending order
jem_df.sort_values(by=["jem-date_patch", "jem-id_patched_cell_container"], inplace=True)

# Checking df
jem_df.to_csv(os.path.join("C:/Users/ramr/Documents/Github/ai_repos/ephys-analysis-tools/trial_1.csv"), index=False)


# Renaming columns names
jem_df.rename(columns = data_variables["collab_daily_tx_report_dictionary"], inplace=True)
jem_df = jem_df[data_variables["collab_daily_tx_report_dictionary"].values()]
jem_df.insert(loc=3, column="Library Prep Day1 Date", value="")
jem_df.sort_values(by="Patch Tube Name", inplace=True)
jem_df.to_csv("C:/Users/ramr/Documents/Github/ai_repos/ephys-analysis-tools/Final.csv")



# Generate lims_df
# lims_df = generate_collab_lims_df(date_report)

#----------Merge jem_df and lims_df----------#
# Merge dataframes by outer join based on specimen id 
# jem_lims_name_df = pd.merge(left=jem_df, right=lims_df, left_on="jem-id_cell_specimen", right_on="lims-id_cell_specimen", how="outer")