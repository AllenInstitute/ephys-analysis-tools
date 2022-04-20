"""
-----------------------------------
File name: save_jem_data.py
Maintainer: Ramkumar Rajanbabu
-----------------------------------
Author: Agata
Date/time created: 3/11/2022
Description: Save jem metdata as a csv
-----------------------------------
"""


# Imports
import json
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time # To measure program execution time

from functions.jem_functions import flatten_jem_data, clean_date_field, clean_roi_field, clean_time_field, clean_num_field, replace_value, add_jem_patch_tube_field, add_jem_post_patch_status_field, add_jem_species_field
from functions.file_functions import get_jsons
from functions.lims_functions import get_lims


#-----Dates-----#
# Date of today
dt_today = datetime.today()
date_today = dt_today.date()
day_today = date_today.strftime("%y%m%d") # "YYMMDD"
# Date of the previous 30 days from date of today
date_prev_30d = date_today - timedelta(days=30)
day_prev_30d = date_prev_30d.strftime("%y%m%d") # "YYMMDD"
# Start of IVSCC data for Patch-Seq
date_start_ivscc = datetime(2017, 10, 1).date()
day_start_ivscc = date_start_ivscc.strftime("%y%m%d") # "YYMMDD"
# First day of this year
date_start_year = datetime(dt_today.year, 1, 1).date()
day_start_year = date_start_year.strftime("%y%m%d") # "YYMMDD"
# First day of this year
date_end_custom = datetime(2017, 12, 31).date()
day_end_custom = date_end_custom.strftime("%y%m%d") # "YYMMDD"


# Read json data from file to import jem_dictionary
with open("C:/Users/ramr/Documents/Github/ai_repos/ephys_analysis_tools/src/constants/data_variables.json") as json_file:
    data_variables = json.load(json_file)


#-----Ram's code-----#
def make_metadata_csv(day_start, day_end):
    """
    Parameters:
    day_start (str): date format YYMMDD
    day_end (str): date format YYMMDD

    Return:
    jem_df, jem_success_df, jem_failure_df
    """

    # Directories
    json_dir  = "//allen/programs/celltypes/workgroups/279/Patch-Seq/all-metadata-files"
    output_dir = "//allen/programs/celltypes/workgroups/279/Patch-Seq/ivscc_data/test_data"

    # Date
    date_start = datetime.strptime(day_start, "%y%m%d").date()
    date_end = datetime.strptime(day_end, "%y%m%d").date()
    
    delta_mod_date = (date_end - date_start).days + 3
    jem_paths = get_jsons(dirname=json_dir, expt="PS", delta_days=delta_mod_date)
    
    # Flatten JSON files (previous 30 day information) to pandas dataframe jem_df)
    jem_df = flatten_jem_data(jem_paths, day_start, day_end)
    
    # Rename columns based on jem_dictionary
    jem_df.rename(columns=data_variables["jem_dictionary"], inplace=True)
    
    # Filter dataframe to only IVSCC Group 2017-Current
    #jem_df = jem_df[jem_df["jem-id_rig_user"].isin(data_variables["ivscc_rig_users_list"])]
    #jem_df = jem_df[jem_df["jem-id_rig_number"].isin(data_variables["ivscc_rig_numbers_list"])]

    # Clean and add date_fields
    jem_df = clean_date_field(jem_df)
    # Clean and add roi fields
    jem_df = clean_roi_field(jem_df)
    # Clean time and add duration fields
    jem_df = clean_time_field(jem_df)
    # Clean numerical fields
    jem_df = clean_num_field(jem_df)
    # Replace value in fields
    jem_df = replace_value(jem_df)
    # Add patch tube field
    jem_df = add_jem_patch_tube_field(jem_df)
    # Add species field
    jem_df = add_jem_species_field(jem_df)
    # Add post patch field
    jem_df = add_jem_post_patch_status_field(jem_df)

    # Sort columns
    #jem_df = jem_df.reindex(columns=data_variables["column_order_list"])

    # Add lims_df
    lims_df = get_lims()
    # Merge jem_df and lims_df
    jem_df = pd.merge(left=jem_df, right=lims_df, left_on="jem-id_patched_cell_container", right_on="patched_cell_container", how="left")

    # Sort
    jem_df.sort_values(by=["jem-date_patch_y-m-d", "jem-id_slice_specimen", "jem-id_cell_specimen", "jem-status_attempt"], inplace=True)

    jem_success_df = jem_df[jem_df["jem-status_success_failure"] == "SUCCESS"]
    jem_failure_df = jem_df[jem_df["jem-status_success_failure"] == "FAILURE"]

    if len(jem_df) > 0:
        try:
            jem_df.to_csv(os.path.join(output_dir, "jem_metadata.csv"), encoding='utf-8-sig', index=False, date_format="%Y-%m-%d")
            print(len(jem_df))
            jem_success_df.to_csv(os.path.join(output_dir, "jem_metadata-success.csv"), encoding='utf-8-sig', index=False, date_format="%Y-%m-%d")
            jem_failure_df.to_csv(os.path.join(output_dir, "jem_metadata-failure.csv"), encoding='utf-8-sig', index=False, date_format="%Y-%m-%d")
        except IOError:
            print("\nOh no! Unable to save spreadsheet :(\nMake sure you don't already have a file with the same name opened.")

    return jem_df, jem_success_df, jem_failure_df


# Main
if __name__ == '__main__':
    start = time.time()
    print("Running program now...")
    _, _, _ = make_metadata_csv(day_start=day_start_year, day_end=day_today)
    print("Generated 3 JEM csvs!")
    print("\nThe program was executed in", round(((time.time()-start)/60), 2), "minutes.")
