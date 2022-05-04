"""
---------------------------------------------------------------------
File name: ivscc_daily_transcriptomics_report.py
Maintainer: Ramkumar Rajanbabu
---------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 05/27/2021
Description: Generate daily transcriptomics report (excel document)
---------------------------------------------------------------------
"""


#-----Imports-----#
import json
import csv
import os
import sys
import pandas as pd
import numpy as np
from pandas.tseries.offsets import CustomBusinessDay
from pandas.tseries.holiday import USFederalHolidayCalendar
from datetime import datetime, date, timedelta
from pathlib import Path, PureWindowsPath
# File imports
from functions.file_functions import get_jsons
from functions.jem_functions import flatten_jem_data, clean_date_field, clean_time_field, clean_num_field, clean_roi_field, \
replace_value, add_jem_patch_tube_field, add_jem_species_field, add_jem_post_patch_status_field, \
fix_jem_versions, fix_jem_blank_date
from functions.io_functions import validated_input, validated_date_input,save_xlsx
from functions.internal_functions import get_lims, get_specimen_id, get_modification_date
#import time # To measure program execution time


#-----Variables-----#
# Read json data from file to import jem_dictionary
with open("C:/Users/ramr/Documents/Github/ai_repos/ephys_analysis_tools/src/constants/data_variables.json") as json_file:
    data_variables = json.load(json_file)

# File name 
name_report = "ps_transcriptomics_report"

# Directories
dir_temporary_report =  "//allen/programs/celltypes/workgroups/279/Patch-Seq/transcriptomics_reports/daily_transcriptomics_reports/temporary"
dir_saved_report = PureWindowsPath(Path("//allen/programs/celltypes/workgroups/279/Patch-Seq/transcriptomics_reports/daily_transcriptomics_reports/temporary"))
dir_submit_report = PureWindowsPath(Path("//aibsfileprint/public/MolBio/RNASeq-SingleCell/PatchSeq/Ephys_daily_reports"))

# Excel document details 
norm_d = {"font_name":"Arial", "font_size":10, "align":"left", "bold": False, "num_format":"0.00"}
head_d = norm_d.copy()
head_d["bg_color"] = "#ABCF8F"

# Dictionaries
dictionary_report = {"jem-id_patched_cell_container":"Patch Tube Name", "jem-date_blank":"Blank Fill Date", "jem-date_patch":"Patch Date", "id_species":"Species", "id_cell_specimen_id":"Specimen ID",
                     "id_slice_genotype":"Cell Line", "jem-roi_major":"ROI Major", "jem-roi_minor":"ROI Minor", "jem-nucleus_post_patch": "Comments", "project_code":"Project", "jem-id_cell_specimen":"Cell Specimen ID"}
dictionary_test = {"jem-date_patch": "JEM Patch Date", "jem-date_blank": "JEM Blank Fill Date", "jem-id_cell_specimen": "JEM Specimen ID", "jem-id_patched_cell_container": "JEM Patch Tube Name",
                   "name": "LIMS Specimen ID", "patched_cell_container": "LIMS Patch Tube Name",
                   "test-jem_lims": "LIMS/JEM Check"}
# New project codes (2021-present)
dictionary_project = {"mouse_nhp": "102-01-045-10",  # 102-01-045-10: CTY IVSCC (Mouse/NHP)
                      "dravet": "102-04-009-10",     # 102-04-009-10: CTY SR: Targeted CNS Gene Therapy (Dravet pilot)
                      "human_u01": "102-01-020-20"}  # 102-01-020-20: CTY BRAIN Human Cell Types (Human Acute/Culture, U01 shipping pilot)


#-----Functions-----#
def generate_daily_report():
    """
    Generates the daily transcriptomics report.

    Output: An excel sheet with a daily transcriptomics report based on a user specified date.
    """

    # Get last business day
    last_bday, last_bday_str = generate_last_business_day()

    # User prompts
    date_report, dt_report = user_prompts(last_bday, last_bday_str)
    # Create daily transcriptomics report name
    date_name_report = "%s_%s.xlsx" %(date_report, name_report)

    # Generate jem_df
    jem_df = generate_jem_df()
    # Generate jem_df in daily transcriptomics report format
    jem_df = generate_daily_jem_df(jem_df, dt_report)

    # Generate lims_df
    lims_df = generate_lims_df(date_report)

    #----------Merge jem_df and lims_df----------#
    # Merge dataframes by outer join based on specimen id 
    jem_lims_name_df = pd.merge(left=jem_df, right=lims_df, left_on="jem-id_cell_specimen", right_on="name", how="outer")
    # Merge dataframes by outer join based on patch tube 
    jem_lims_tube_df = pd.merge(left=jem_df, right=lims_df, left_on="jem-id_patched_cell_container", right_on="patched_cell_container", how="outer")

    #----------Create conditional column----------#
    # Create a new column (test-jem_lims)
    jem_lims_name_df["test-jem_lims"] = np.where((jem_lims_name_df["jem-id_cell_specimen"] == jem_lims_name_df["name"])&(jem_lims_name_df["jem-id_patched_cell_container"] == jem_lims_name_df["patched_cell_container"]), "Both",
                                        np.where((jem_lims_name_df["jem-id_cell_specimen"].isnull()), "LIMS",
                                        np.where((jem_lims_name_df["name"].isnull()), "JEM",
                                        np.where((jem_lims_name_df["jem-id_patched_cell_container"] != jem_lims_name_df["patched_cell_container"]), "Mismatched Patch Tube", "Unsure"))))
    # Create a new column (test-jem_lims)
    jem_lims_tube_df["test-jem_lims"] = np.where((jem_lims_tube_df["jem-id_cell_specimen"] == jem_lims_tube_df["name"])&(jem_lims_tube_df["jem-id_patched_cell_container"] == jem_lims_tube_df["patched_cell_container"]), "Both",
                                        np.where((jem_lims_tube_df["jem-id_cell_specimen"].isnull()), "LIMS",
                                        np.where((jem_lims_tube_df["name"].isnull()), "JEM",
                                        np.where((jem_lims_tube_df["jem-id_cell_specimen"] != jem_lims_tube_df["name"]), "Mismatched Specimen ID", "Unsure"))))

    if (len(jem_df) > 0) & (len(lims_df) > 0):
        # Adding new column with project codes
        jem_lims_name_df["project_code"] = np.where((jem_lims_name_df["id_species"] == "Human"), dictionary_project["human_u01"], dictionary_project["mouse_nhp"])
        # Agata's method:  jem_lims_name_df.loc[:,"project_code"] = jem_lims_name_df.apply(lambda x: assign_project(x["jem-id_patched_cell_container"], x["id_species"], x["jem-roi_major"]), axis=1)

        # Create a date check for jem 
        jem_lims_name_df["jem-date_container"] = jem_lims_name_df["jem-id_patched_cell_container"].str[5:11]
        jem_lims_name_df["jem-date_container"] = pd.to_datetime(jem_lims_name_df["jem-date_container"], format='%y%m%d').dt.strftime("%m/%d/%Y")
        jem_lims_name_df["jem-date_check"] = jem_lims_name_df["jem-date_patch"] == jem_lims_name_df["jem-date_container"]
        # Use pdate_df to check for mismatched dates and patched cell container dates
        pdate_df = jem_lims_name_df[["jem-date_check", "jem-date_patch", "jem-id_cell_specimen", "jem-id_patched_cell_container", "name", "patched_cell_container"]].copy()
        pdate_df.dropna(subset=["jem-date_patch"], inplace=True)
        pdate_df.rename(columns=dictionary_test, inplace=True)
        pdate_df.sort_values(by="LIMS Patch Tube Name", inplace=True)

        # Create a test_df
        test_name_df = generate_test_df(jem_lims_name_df)
        test_tube_df = generate_test_df(jem_lims_tube_df)

        # Test dataframes
        pdate_df = pdate_df[pdate_df["jem-date_check"] == False]
        check_jem_df = test_name_df[test_name_df["LIMS/JEM Check"] == "JEM"]
        check_lims_df = test_name_df[test_name_df["LIMS/JEM Check"] == "LIMS"]
        check_mismatch_specimen_df = test_tube_df[test_tube_df["LIMS/JEM Check"] == "Mismatched Specimen ID"]
        check_mismatch_container_df = test_name_df[test_name_df["LIMS/JEM Check"] == "Mismatched Patch Tube"]

        if (len(pdate_df) > 0) or (len(check_jem_df) > 0) or (len(check_lims_df) > 0) or (len(check_mismatch_specimen_df) > 0) or (len(check_mismatch_container_df) > 0):
            # Row numbering
            pdate_num, check_jem_num, check_lims_num, check_mismatch_specimen_num, check_mismatch_container_num = 1, 1, 1, 1, 1
            # Check for mismatched dates
            if len(pdate_df) > 0:
                print("\nMismatched information between Patch Date and Patch Tube!")
                for index, row in pdate_df.iterrows():
                    print(f"{pdate_num}) JEM Patch Date: {row['JEM Patch Date']}| JEM Specimen ID: {row['JEM Specimen ID']}| JEM Patch Tube: {row['JEM Patch Tube Name']}")
                    pdate_num+=1
            # Check for JEM information
            if (len(check_jem_df) > 0) & (len(check_mismatch_specimen_df) == 0) & (len(check_mismatch_container_df) == 0):
                print("\nOnly JEM information is found! Please use the JEM Specimen ID and JEM Patch Tube to update LIMS!")
                for index, row in check_jem_df.iterrows():
                    print(f"{check_jem_num}) JEM Specimen ID: {row['JEM Specimen ID']}| JEM Patch Tube: {row['JEM Patch Tube Name']}")
                    check_jem_num+=1
            # Check for LIMS information
            if (len(check_lims_df) > 0) & (len(check_mismatch_specimen_df) == 0) & (len(check_mismatch_container_df) == 0):
                print("\nOnly LIMS information is found! Please use the LIMS Specimen ID and LIMS Patch Tube to create a JEM form!")
                for index, row in check_lims_df.iterrows():
                    print(f"{check_lims_num}) LIMS Specimen ID: {row['LIMS Specimen ID']}| LIMS Patch Tube: {row['LIMS Patch Tube Name']}")
                    check_lims_num+=1
            # Check for mismatched specimen id information
            if len(check_mismatch_specimen_df) > 0:
                print("\nMismatched information between JEM and LIMS Specimen ID!")
                for index, row in check_mismatch_specimen_df.iterrows():
                    print(f"{check_mismatch_specimen_num}) JEM Patch Tube: {row['JEM Patch Tube Name']}| JEM Specimen ID: {row['JEM Specimen ID']}| LIMS Specimen ID: {row['LIMS Specimen ID']}")
                    check_mismatch_specimen_num+=1        
            # Check for mismatched patch tube information
            if len(check_mismatch_container_df) > 0:
                print("\nMismatched information between JEM and LIMS Patch Tube!")
                for index, row in check_mismatch_container_df.iterrows():
                    print(f"{check_mismatch_container_num}) JEM Specimen ID: {row['JEM Specimen ID']}| JEM Patch Tube: {row['JEM Patch Tube Name']}| LIMS Patch Tube: {row['LIMS Patch Tube Name']}")
                    check_mismatch_container_num+=1

        # Check for JEM and LIMS information
        else:
            try:
                # Renaming columns names
                jem_lims_name_df.rename(columns=dictionary_report, inplace=True)
                jem_lims_name_df = jem_lims_name_df[dictionary_report.values()]
                jem_lims_name_df.insert(loc=3, column="Library Prep Day1 Date", value="")
                jem_lims_name_df.sort_values(by="Patch Tube Name", inplace=True)
                # Save dataframe as an excel sheet    
                save_xlsx(jem_lims_name_df, dir_temporary_report, date_name_report, norm_d, head_d)
                # Message in the terminal
                terminal_message(dir_saved_report, dir_submit_report, jem_lims_name_df)
            except IOError:
                    print("\nUnable to save the excel sheet. \nMake sure you don't already have a file with the same name opened.")
    else:
        sys.exit(f"""No JSON data for successful experiments on {dt_report.strftime("%m/%d/%Y")}.""")


def generate_last_business_day():
    """
    Get last business day
    """
    
    bday_us = CustomBusinessDay(calendar=USFederalHolidayCalendar())
    last_bday = (datetime.today() - bday_us).date()
    last_bday_str = last_bday.strftime("%y%m%d")
    
    return last_bday, last_bday_str


def user_prompts(last_bday, last_bday_str):
    """
    User prompts for date entry
    """

    str_prompt1 = "\nWould you like to report on samples from %s? (y / n): "  %last_bday_str
    valid_vals = ["y", "n"]
    str_prompt2 = "Please enter date to report on (YYMMDD): "
    response1 = "\nPlease try again..."
    response2 = "\nPlease try again... date should be YYMMDD"

    # Ask for user input
    last_bday_state = validated_input(str_prompt1, response1, valid_vals)
    if last_bday_state == "n":
        date_report = validated_date_input(str_prompt2, response2, valid_options=None)
        dt_report = datetime.strptime(date_report, "%y%m%d").date()
    else:
        date_report = last_bday_str
        dt_report = last_bday
    
    return date_report, dt_report


def generate_jem_df():
    """
    Generate a jem metadata dataframe
    
    date: dt_report (datetime?)
    """

    # Date of today
    dt_today = datetime.today()
    date_today = dt_today.date()
    day_today = date_today.strftime("%y%m%d") # "YYMMDD"
    # Date of the previous 30 days from date of today
    date_prev_30d = date_today - timedelta(days=30)
    day_prev_30d = date_prev_30d.strftime("%y%m%d") # "YYMMDD"

    # Directories
    json_dir  = "//allen/programs/celltypes/workgroups/279/Patch-Seq/all-metadata-files"
    
    delta_mod_date = (date_today - date_prev_30d).days + 3
    jem_paths = get_jsons(dirname=json_dir, expt="PS", delta_days=delta_mod_date)
    
    # Flatten JSON files (previous 30 day information) to pandas dataframe 9jem_df)
    jem_df = flatten_jem_data(jem_paths, day_prev_30d, day_today)
    
    # Rename columns based on jem_dictionary
    jem_df.rename(columns=data_variables["jem_dictionary"], inplace=True)
    
    # Filter dataframe to only IVSCC Group 2017-Current
    jem_df = jem_df[jem_df["jem-id_rig_user"].isin(data_variables["ivscc_rig_users_list"])]
    jem_df = jem_df[jem_df["jem-id_rig_number"].isin(data_variables["ivscc_rig_numbers_list"])]
    
    # Replace status values
    jem_df["jem-status_success_failure"] = jem_df["jem-status_success_failure"].replace({"SUCCESS (high confidence)": "SUCCESS",
                                                                                         "NO ATTEMPTS": "FAILURE",
                                                                                         "Failure": "FAILURE"})
    # Replace tube values
    jem_df["jem-id_patched_cell_container"] = jem_df["jem-id_patched_cell_container"].replace({"NA": "Not Applicable"})
    # Replace reporter status values 
    jem_df["jem-status_reporter"] = jem_df["jem-status_reporter"].replace({"Cre+": "Positive", "Cre-": "Negative", "human": np.nan, "NA": np.nan})

    # Filter to only successful experiments
    jem_df = jem_df[(jem_df["jem-status_success_failure"] == "SUCCESS")]
    # Drop na values
    jem_df.dropna(subset=["jem-id_patched_cell_container"], inplace=True)
    # Filters dataframe to only patched cell containers
    jem_df = jem_df[(jem_df["jem-id_patched_cell_container"] != "Not Applicable")]

    # Fix jem versions
    jem_df = fix_jem_versions(jem_df)
    # Fix jem blank date
    jem_df = fix_jem_blank_date(jem_df)
    # Clean and add date_fields
    jem_df = clean_date_field(jem_df)
    # Clean time and add duration fields
    jem_df = clean_time_field(jem_df)
    # Clean numerical fields
    jem_df = clean_num_field(jem_df)
    # Clean and add roi fields
    jem_df = clean_roi_field(jem_df)
    # Replace value in fields
    jem_df = replace_value(jem_df)
    # Add patch tube field
    jem_df = add_jem_patch_tube_field(jem_df)
    # Add species field
    jem_df = add_jem_species_field(jem_df)
    # Add post patch status field
    jem_df = add_jem_post_patch_status_field(jem_df)

    return jem_df


def generate_daily_jem_df(df, date):
    """
    Generate a jem metadata dataframe
    
    date: dt_report (datetime?)
    """

    # Lists
    jem_fields = ["jem-date_patch", "jem-date_blank", "jem-id_rig_user", "jem-id_cell_specimen",
                  "jem-id_patched_cell_container", "jem-roi_major", "jem-roi_minor",
                  "jem-nucleus_post_patch"]

    # Filter dataframe to specified fields
    df = df[jem_fields]
    # Filters dataframe to user specified date
    df = df[df["jem-date_patch"].str.contains(date.strftime("%m/%d/%Y"))]
    # Sort by jem-id_patched_cell_container in ascending order
    df.sort_values(by="jem-id_patched_cell_container", inplace=True)
    
    return df


def generate_lims_df(date):
    """
    Generate a lims dataframe
    """

    # Lists
    hct_jt = [str(x) for x in range(101, 151, 1)] # Jonathan(101-150)
    hct_cr = [str(x) for x in range(225, 251, 1)] # Cristina(225-250)
    hct_bk_mk = [str(x) for x in range(301, 351, 1)] # Brian K(301-350), Meanhwan(325-350)
    hct_ln = [str(x) for x in range(351, 401, 1)] # Lindsay(351-400)
    hct_user_tube_num_list = hct_jt + hct_cr + hct_bk_mk + hct_ln

    #collab = PG/PH & 400?

    lims_df = get_lims()
    # Filters dataframe to user specified date
    lims_df = lims_df[lims_df["patched_cell_container"].str.contains(date)]
    # Only run if patched cell containers were collected
    if len(lims_df) > 0:
        # Exclude Collaborator containers
        lims_df = lims_df[(~lims_df["patched_cell_container"].str.startswith("PGS4")) & (~lims_df["patched_cell_container"].str.startswith("PHS4"))]
        # Exclude HCT containers (Ex. column output: 301)
        lims_df["exclude_container"] = lims_df["patched_cell_container"].str.slice(-7, -4)
        lims_df = lims_df[~lims_df["exclude_container"].str.contains("|".join(hct_user_tube_num_list))]
        # Replace values
        lims_df["id_species"].replace({"Homo Sapiens":"Human", "Mus musculus":"Mouse"}, inplace=True)
        lims_df["id_slice_genotype"].replace({None:""}, inplace=True)
        # Apply specimen id
        lims_df["id_cell_specimen_id"] = lims_df.apply(get_specimen_id, axis=1)
        # Sort by patched_cell_container in ascending order
        lims_df.sort_values(by="patched_cell_container", inplace=True)

    return lims_df

  
def generate_test_df(df):
    """
    Generate a test dataframe
    """
    
    test_df = df[["test-jem_lims", "jem-id_cell_specimen", "jem-id_patched_cell_container", "name", "patched_cell_container"]].copy()
    test_df.rename(columns=dictionary_test, inplace=True)
    test_df.sort_values(by="LIMS Patch Tube Name", inplace=True)
    
    return test_df


def check_project_retrograde():
    """
    Docstring
    """
    
    df["jem-project_name"] = df[df["jem-project_name"] == "retrograde_labeling"]
    df["jem-status_reporter"] = df[df["jem-status_reporter"] == "Positive"]

    return df



def terminal_message(saved_location, report_location, df):
    """
    Message in terminal
    """
    
    print()
    print(f"Saved report location: {saved_location}")
    print(f"Submit report location: {report_location}")
    print(f"Total Patch Tubes: {len(df)}")
    print()
    print("Before submitting the report, please manually compare the total number of patch tubes in Slack(# rotations_team) with the total listed above.")
    print("Also, check the Blank Date is correct in the report.")
    print()
    print("If all present information is correct, please create a copy from the saved report location and submit the report in the submit report location.")


if __name__ == "__main__":
    #start = time.time()
    generate_daily_report()
    #print("\nThe program was executed in", round(time.time()-start, 2), "seconds.")
