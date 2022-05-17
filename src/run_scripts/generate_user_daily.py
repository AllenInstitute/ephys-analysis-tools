"""
-----------------------------------------------------------------------
File name: generate_user_daily.py
Maintainer: Ramkumar Rajanbabu
-----------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 5/13/2022
Description: Template for generating user_daily.xlsx
-----------------------------------------------------------------------
"""


#-----Imports-----#
# General imports
import json
import numpy as np
import os
import pandas as pd
from datetime import datetime, date, timedelta
# File imports
from functions.jem_functions import generate_jem_df, flatten_jem_data, \
fix_jem_versions, fix_jem_blank_date, \
clean_date_field, clean_time_field, clean_num_field, clean_roi_field, \
replace_value, add_jem_patch_tube_field, add_jem_species_field, add_jem_post_patch_status_field, \
get_project_channel
from functions.lims_functions import get_lims

from functions.file_functions import get_jsons
from functions.jem_data_set import JemDataSet
# Test imports
#import time # To measure program execution time


#-----Functions-----#
def main():
	"""
	Main function to create user_daily.xlsx.

	Parameters:
		None

	Returns:
		user_daily.xlsx (excel file)
	"""

	output_fields = ["jem-id_rig_user", "jem-date_patch", "jem-time_patch" , "jem-date_blank",
					 "jem-id_species", "lims-id_project_code",
					 "jem-id_cell_specimen", "lims-id_cell_specimen",
					 "jem-id_patched_cell_container", "lims-id_patched_cell_container",
					 "jem-nucleus_post_patch",
					 "jem-status_reporter", "jem-roi_major_minor", "jem-roi_major", "jem-roi_minor",
					 "jem-virus_enhancer", "jem-project_name",
					 "jem-project_retrograde_labeling_hemisphere", "jem-project_retrograde_labeling_region", "jem-project_retrograde_labeling_exp"]


	# Read json data from file to import dictionaries/list
	with open("C:/Users/ramr/Documents/Github/ai_repos/ephys_analysis_tools/src/constants/data_variables.json") as json_file:
		data_variables = json.load(json_file)

	# Directories
	path_output = "//allen/programs/celltypes/workgroups/279/Patch-Seq/compiled-jem-data/formatted_data"

	# Get today's date
	dt_today, day_today = generate_today()

	# Generate jem_df
	jem_df = generate_jem_df()
	# Generate jem_df in daily transcriptomics report format
	jem_df = generate_daily(jem_df, dt_today) # datetime(2022, 5, 12)

	# Add lims_df
	lims_df = get_lims()
	# Rename columns based on jem_dictionary
	lims_df.rename(columns=data_variables["lims_dictionary"], inplace=True)

	# Merge jem_df and lims_df
	jem_lims_df = pd.merge(left=jem_df, right=lims_df, left_on="jem-id_patched_cell_container", right_on="lims-id_patched_cell_container", how="left")
	
	# Filter dataframe to specified fields
	jem_lims_df = jem_lims_df[output_fields].copy()

	# Sort by jem-id_patched_cell_container in ascending order
	jem_lims_df.sort_values(by=["jem-time_patch", "jem-id_cell_specimen", "jem-id_patched_cell_container"], inplace=True)

	# Convert the dataframe to an XlsxWriter Excel object.
	jem_lims_df.to_excel(os.path.join(path_output, "user_daily.xlsx"), engine='xlsxwriter', index=False) 


def generate_today():
	"""
	Generates the date of today.

	Parameters:
	    None

	Returns:
	    day_today (string): a date of today in the format of YYMMDD.
	"""

	dt_today = datetime.today().date() # datetime.datetime
	day_today = dt_today.strftime("%y%m%d")

	return dt_today, day_today


def generate_daily(df, date):
    """
    Generates a jem metadata dataframe based on the specified date.

    Parameters:
        df (dataframe): a pandas dataframe.
        date (datetime): 

    Returns:
        df (dataframe): a pandas dataframe.
    """

    # Filters dataframe to user specified date
    df = df[df["jem-date_patch"].str.contains(date.strftime("%m/%d/%Y"))]
    
    return df


# Main
if __name__ == "__main__":
	#start = time.time()
	main()
	#print("\nThe program was executed in", round(((time.time()-start)/60), 2), "minutes.")
