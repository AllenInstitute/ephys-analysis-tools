"""
---------------------------------------------------------------------
File name: transcriptomics_functions.py
Maintainer: Ramkumar Rajanbabu
---------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 05/19/2022
Description: Transcriptomics related functions
---------------------------------------------------------------------
"""


#-----Imports-----#
# General imports
import json
import numpy as np
import os
import pandas as pd
import sys
from datetime import datetime, date, timedelta
from pandas.tseries.offsets import CustomBusinessDay
from pandas.tseries.holiday import USFederalHolidayCalendar
from pathlib import Path, PureWindowsPath
# File imports
from functions.file_functions import load_data_variables
from functions.io_functions import validated_input, validated_date_input,save_xlsx
from functions.jem_functions import generate_jem_df
from functions.lims_functions import generate_lims_df


#-----General Information-----#
"""
project_dictionary details: New project codes (2021-present)
- 102-01-045-10: CTY IVSCC (Mouse/NHP) # IVSCC (Dates?)
- 102-04-006-20 : MSP Measuring Consciousness Ph2 (TBD) # HCT
- 102-01-051-20: CTY Neuromodulation of NHP Cell Types # HCT

project_dictionary details: Old project codes
- 102-01-020-20: CTY BRAIN Human Cell Types (Human Acute/Culture, U01 shipping pilot) # IVSCC (10/01/2017 - 6/03/2022) 
- 102-04-009-10: CTY SR: Targeted CNS Gene Therapy (Dravet pilot) # IVSCC (Dates?)
"""


#-----Variables-----#
# Load json file
data_variables = load_data_variables()


#-----Daily Transcriptomics Functions-----#
def generate_daily_report(group):
    """
    Generates the daily transcriptomics report.

    Parameters:
        group (string): "ivscc" or "hct".

    Returns:
        An excel file with a daily transcriptomics report based on a user specified date.
    """

    # Excel document details 
    norm_d = {"font_name":"Arial", "font_size":10, "align":"left", "bold": False, "num_format":"0.00"}
    head_d = norm_d.copy()
    head_d["bg_color"] = "#ABCF8F"

    # Directories
    submit_report_dir = PureWindowsPath(Path("//aibsfileprint/public/MolBio/RNASeq-SingleCell/PatchSeq/Ephys_daily_reports"))

    if group == "ivscc":
        # Directories
        ivscc_temporary_report_dir = "//allen/programs/celltypes/workgroups/279/Patch-Seq/transcriptomics_reports/ivscc_transcriptomics_reports/daily_transcriptomics_reports/temporary"
        ivscc_saved_report_dir = PureWindowsPath(Path(ivscc_temporary_report_dir))
        # File name 
        name_report = "ps_transcriptomics_report"
    if group == "hct":
        # Directories
        hct_temporary_report_dir = "//allen/programs/celltypes/workgroups/279/Patch-Seq/transcriptomics_reports/hct_transcriptomics_reports/daily_transcriptomics_reports/temporary"
        hct_saved_report_dir = PureWindowsPath(Path(hct_temporary_report_dir))
        # File name 
        name_report = "ps_transcriptomics_report_HCT"

    # Get last business day
    last_bday, last_bday_str = generate_last_business_day()

    # User prompts
    date_report, dt_report = user_prompts_daily(last_bday, last_bday_str)
    # Create daily transcriptomics report name
    date_name_report = "%s_%s.xlsx" %(date_report, name_report)

    # Generate lims_df and jem_df with only patch tubes
    if group == "ivscc":
        lims_df = generate_lims_df("ivscc", date_report)
        jem_df = generate_jem_df("ivscc", "only_patch_tubes")
    if group == "hct":
        lims_df = generate_lims_df("hct", date_report)
        jem_df = generate_jem_df("hct", "only_patch_tubes")
    # Generate jem_df in daily transcriptomics report format
    jem_df = generate_daily_jem_df(jem_df, dt_report)

    #----------Merge jem_df and lims_df----------#
    # Merge dataframes by outer join based on specimen id 
    jem_lims_name_df = pd.merge(left=jem_df, right=lims_df, left_on="jem-id_cell_specimen", right_on="lims-id_cell_specimen", how="outer")
    # Merge dataframes by outer join based on patch tube 
    jem_lims_tube_df = pd.merge(left=jem_df, right=lims_df, left_on="jem-id_patched_cell_container", right_on="lims-id_patched_cell_container", how="outer")

    #----------Create conditional column----------#
    # Create a new column (test-jem_lims) for jem_lims_name_df 
    jem_lims_name_df["test-jem_lims"] = np.where((jem_lims_name_df["jem-id_cell_specimen"] == jem_lims_name_df["lims-id_cell_specimen"])&(jem_lims_name_df["jem-id_patched_cell_container"] == jem_lims_name_df["lims-id_patched_cell_container"]), "Both",
                                        np.where((jem_lims_name_df["jem-id_cell_specimen"].isnull()), "LIMS",
                                        np.where((jem_lims_name_df["lims-id_cell_specimen"].isnull()), "JEM",
                                        np.where((jem_lims_name_df["jem-id_patched_cell_container"] != jem_lims_name_df["lims-id_patched_cell_container"]), "Mismatched Patch Tube", "Unsure"))))
    # Create a new column (test-jem_lims) for jem_lims_tube_df
    jem_lims_tube_df["test-jem_lims"] = np.where((jem_lims_tube_df["jem-id_cell_specimen"] == jem_lims_tube_df["lims-id_cell_specimen"])&(jem_lims_tube_df["jem-id_patched_cell_container"] == jem_lims_tube_df["lims-id_patched_cell_container"]), "Both",
                                        np.where((jem_lims_tube_df["jem-id_cell_specimen"].isnull()), "LIMS",
                                        np.where((jem_lims_tube_df["lims-id_cell_specimen"].isnull()), "JEM",
                                        np.where((jem_lims_tube_df["jem-id_cell_specimen"] != jem_lims_tube_df["lims-id_cell_specimen"]), "Mismatched Specimen ID", "Unsure"))))

    if (len(jem_df) > 0) & (len(lims_df) > 0):
        # Adding new column with project codes
        if group == "ivscc":
            jem_lims_name_df["project_code"] = data_variables["project_dictionary"]["mouse_nhp"]
        if group == "hct":
            jem_lims_name_df["project_code"] = np.where((jem_lims_name_df["jem-id_patched_cell_container"].str.startswith("PYS4")) & (jem_lims_name_df["lims-id_patched_cell_container"].str.startswith("PYS4")), data_variables["project_dictionary"]["psilocybin"],
                                               np.where((jem_lims_name_df["jem-id_patched_cell_container"].str.startswith("P7S4")) & (jem_lims_name_df["lims-id_patched_cell_container"].str.startswith("P7S4")) & (jem_lims_name_df["lims-id_project_code"] == "MET-NM"), data_variables["project_dictionary"]["neuromodulation"],
                                               np.where((jem_lims_name_df["jem-id_patched_cell_container"].str.startswith("PCS4")) & (jem_lims_name_df["lims-id_patched_cell_container"].str.startswith("PCS4")) & (jem_lims_name_df["lims-id_project_code"] == "MET-NM"), data_variables["project_dictionary"]["neuromodulation"], data_variables["project_dictionary"]["mouse_nhp"])))
        
        # Create a date check for jem 
        jem_lims_name_df["jem-date_container"] = jem_lims_name_df["jem-id_patched_cell_container"].str[5:11]
        jem_lims_name_df["jem-date_container"] = pd.to_datetime(jem_lims_name_df["jem-date_container"], format='%y%m%d').dt.strftime("%m/%d/%Y")
        jem_lims_name_df["jem-date_check"] = jem_lims_name_df["jem-date_patch"] == jem_lims_name_df["jem-date_container"]
        # Use test_patch_date_df to check for mismatched dates and patched cell container dates
        test_patch_date_df = jem_lims_name_df[["jem-date_check", "jem-date_patch", "jem-id_cell_specimen", "jem-id_patched_cell_container", "lims-id_cell_specimen", "lims-id_patched_cell_container"]].copy()
        test_patch_date_df.dropna(subset=["jem-date_patch"], inplace=True)
        test_patch_date_df.sort_values(by="lims-id_patched_cell_container", inplace=True)

        # Create a test_df
        test_name_df = generate_test_df(jem_lims_name_df)
        test_tube_df = generate_test_df(jem_lims_tube_df)

        # Test dataframes
        test_patch_date_df = test_patch_date_df[test_patch_date_df["jem-date_check"] == False]
        test_jem_df = test_name_df[test_name_df["test-jem_lims"] == "JEM"]
        test_lims_df = test_name_df[test_name_df["test-jem_lims"] == "LIMS"]
        test_mismatch_specimen_df = test_tube_df[test_tube_df["test-jem_lims"] == "Mismatched Specimen ID"]
        test_mismatch_container_df = test_name_df[test_name_df["test-jem_lims"] == "Mismatched Patch Tube"]

        if (len(test_patch_date_df) > 0) or (len(test_jem_df) > 0) or (len(test_lims_df) > 0) or (len(test_mismatch_specimen_df) > 0) or (len(test_mismatch_container_df) > 0):
            # Tests for mismatched jem patch date and jem patch tube date
            test_mismatch_patch_date(test_patch_date_df)
            # Tests for missing LIMS information
            test_miss_lims(test_jem_df, test_mismatch_specimen_df, test_mismatch_container_df)
            # Tests for missing JEM information
            test_miss_jem(test_lims_df, test_mismatch_specimen_df, test_mismatch_container_df)
            # Tests for mismatched jem/lims specimen id information
            test_mismatch_specimen(test_mismatch_specimen_df)   
            # Tests for mismatched jem/lims patch tube information
            test_mismatch_patch_tube(test_mismatch_container_df)

            if group == "ivscc":
                # Tests for PROJECTS
                test_jem_projects(jem_df)
        
        else:
            try:
                # Renaming columns names
                jem_lims_name_df.rename(columns=data_variables["daily_tx_report_dictionary"], inplace=True)
                jem_lims_name_df = jem_lims_name_df[data_variables["daily_tx_report_dictionary"].values()]
                jem_lims_name_df.insert(loc=3, column="Library Prep Day1 Date", value="")
                jem_lims_name_df.sort_values(by="Patch Tube Name", inplace=True)
                if group == "ivscc":
                    # Save dataframe as an excel sheet    
                    save_xlsx(jem_lims_name_df, ivscc_temporary_report_dir, date_name_report, norm_d, head_d)
                    # Message in the terminal
                    terminal_message_daily(ivscc_saved_report_dir, submit_report_dir, jem_lims_name_df, "ivscc")
                if group == "hct":
                    # Save dataframe as an excel sheet    
                    save_xlsx(jem_lims_name_df, hct_temporary_report_dir, date_name_report, norm_d, head_d)
                    # Message in the terminal
                    terminal_message_daily(hct_saved_report_dir, submit_report_dir, jem_lims_name_df)
            except IOError:
                    print("\nUnable to save the excel sheet. \nMake sure you don't already have a file with the same name opened.")
    else:
        sys.exit(f"""\nNo data present for successful experiments with patch tubes collected on {dt_report.strftime("%m/%d/%Y")}.""")


def generate_last_business_day():
    """
    Generates the previous business day.

    Parameters:
        None

    Returns:
        last_bday (datetime.date)
        last_bday_str (string)
    """
    
    bday_us = CustomBusinessDay(calendar=USFederalHolidayCalendar())
    last_bday = (datetime.today() - bday_us).date()
    last_bday_str = last_bday.strftime("%y%m%d")
    
    return last_bday, last_bday_str


def user_prompts_daily(last_bday, last_bday_str):
    """
    Prompts the user to enter the specified date.

    Parameters:
        last_bday (datetime.date)
        last_bday_str (string)

    Returns:
        date_report (string)
        dt_report (datetime)
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


def generate_daily_jem_df(df, date):
    """
    Generates a jem metadata dataframe based on the specified date.

    Parameters:
        df (dataframe): a pandas dataframe.
        date (datetime): 

    Returns:
        df (dataframe): a pandas dataframe.
    """

    # Lists
    jem_fields = ["jem-date_patch", "jem-date_blank", "jem-id_rig_user", "jem-id_cell_specimen",
                  "jem-id_patched_cell_container", "jem-roi_major", "jem-roi_minor",
                  "jem-nucleus_post_patch",
                  "jem-project_name", "jem-status_reporter"]

    # Filter dataframe to specified fields
    df = df[jem_fields]
    # Filters dataframe to user specified date
    df = df[df["jem-date_patch"].str.contains(date.strftime("%m/%d/%Y"))]
    # Sort by jem-id_patched_cell_container in ascending order
    df.sort_values(by="jem-id_patched_cell_container", inplace=True)
    
    return df


def generate_test_df(df):
    """
    Generates a test dataframe based on the specified date.

    Parameters:
        df (dataframe): a pandas dataframe.

    Returns:
        df (dataframe): a pandas dataframe.
    """
    
    test_fields = ["test-jem_lims",
                   "jem-id_cell_specimen", "jem-id_patched_cell_container",
                   "lims-id_cell_specimen", "lims-id_patched_cell_container"]

    test_df = df[test_fields].copy()
    test_df.sort_values(by="lims-id_patched_cell_container", inplace=True)
    
    return test_df


def terminal_message_daily(saved_location, report_location, df, group=None):
    """
    Generates a message in the anaconda command prompt terminal for the user.

    Parameters:
        saved_location: a location of the directory with the copies of generated daily transcriptomics reports.
        report_location: a location of the directory to submit the generated daily transcriptomics report.
        df (dataframe): a pandas dataframe.
        group (string): None (default) or "ivscc" to add more print statements.

    Returns:
        print statement (string)
    """

    print()
    print(f"Saved report location: {saved_location}")
    print(f"Submit report location: {report_location}")
    print(f"Total Patch Tubes: {len(df)}")
    if group == "ivscc":
        print()
        print("#-----Manual Check-----#")
        print("- Compare the total number of patch tubes in Slack(# rotations_team) with the total listed above")
        print("- Blank Date")
        print("- Virus/Enhancer")
        print("- Slice/Cell Level Projects")
    print()
    print("If all present information is correct, please create a copy from the saved report location and submit the report in the submit report location.")


#-----Daily Transcriptomics - Test Primary Functions-----#
def test_mismatch_patch_date(test_patch_date_df):
    """
    Tests for mismatched jem patch date and patch tube date.

    Parameters:
        test_patch_date_df (dataframe): a pandas dataframe.

    Returns:
        print statement (string)
    """

    # Row numbering
    num = 1

    if (len(test_patch_date_df) > 0):
        print("\n#-----Mismatched JEM Patch Date & JEM Patch Tube Date-----#")
        print("Description:")
        print("In the JEM form, please correct the JEM Patch Date and Patch Tube!")
        print()
        for index, row in test_patch_date_df.iterrows():
            print(f"{num}) JEM Specimen ID: {row['jem-id_cell_specimen']}")
            print(f"   - JEM Patch Date: {row['jem-date_patch']}")
            print(f"   - JEM Patch Tube: {row['jem-id_patched_cell_container']}")
            num+=1


def test_miss_lims(test_jem_df, test_mismatch_specimen_df, test_mismatch_container_df):
    """
    Tests for missing lims information.

    Parameters:
        test_jem_df (dataframe): a pandas dataframe.
        test_mismatch_specimen_df (dataframe): a pandas dataframe.
        test_mismatch_container_df (dataframe): a pandas dataframe.

    Returns:
        print statement (string)
    """
    
    # Row numbering
    num = 1

    if (len(test_jem_df) > 0) & (len(test_mismatch_specimen_df) == 0) & (len(test_mismatch_container_df) == 0):
        print("\n#-----Missing LIMS Information-----#")
        print("Description:")
        print("Please use the JEM Specimen ID and JEM Patch Tube to update LIMS!")
        print()
        for index, row in test_jem_df.iterrows():
            print(f"{num}) JEM Specimen ID: {row['jem-id_cell_specimen']}")
            print(f"   - JEM Patch Tube: {row['jem-id_patched_cell_container']}")
            num+=1


def test_miss_jem(test_lims_df, test_mismatch_specimen_df, test_mismatch_container_df):
    """
    Tests for missing jem information.

    Parameters:
        test_lims_df (dataframe): a pandas dataframe.
        test_mismatch_specimen_df (dataframe): a pandas dataframe.
        test_mismatch_container_df (dataframe): a pandas dataframe.

    Returns:
        print statement (string)
    """
    
    # Row numbering
    num = 1

    if (len(test_lims_df) > 0) & (len(test_mismatch_specimen_df) == 0) & (len(test_mismatch_container_df) == 0):
        print("\n#-----Missing JEM Information-----#")
        print("Description:")
        print("Please use the LIMS Specimen ID and LIMS Patch Tube to create a JEM form!")
        print()
        for index, row in test_lims_df.iterrows():
            print(f"{num}) LIMS Specimen ID: {row['lims-id_cell_specimen']}")
            print(f"   - LIMS Patch Tube: {row['lims-id_patched_cell_container']}")
            num+=1


def test_mismatch_specimen(test_mismatch_specimen_df):
    """
    Tests for mismatched jem and lims specimen id.

    Parameters:
        test_mismatch_specimen_df (dataframe): a pandas dataframe.

    Returns:
        print statement (string)
    """
    
    # Row numbering
    num = 1

    if len(test_mismatch_specimen_df) > 0:
        print("\n#-----Mismatched JEM/LIMS Specimen ID Information-----#")
        print("Description:")
        print("Please use the JEM Patch Tube to identify the mismatched information between the JEM Specimen ID and LIMS Specimen ID!")
        print()
        for index, row in test_mismatch_specimen_df.iterrows():
            print(f"{num}) JEM Patch Tube: {row['jem-id_patched_cell_container']}")
            print(f"   - JEM Specimen ID: {row['jem-id_cell_specimen']}")
            print(f"   - LIMS Specimen ID: {row['lims-id_cell_specimen']}")
            num+=1


def test_mismatch_patch_tube(test_mismatch_container_df):
    """
    Tests for mismatched jem and lims patch tube.

    Parameters:
        test_mismatch_container_df (dataframe): a pandas dataframe.

    Returns:
        print statement (string)
    """
    
    # Row numbering
    num = 1

    if len(test_mismatch_container_df) > 0:
        print("\n#-----Mismatched JEM/LIMS Specimen Patch Tube Information-----#")
        print("Description:")
        print("Please use the JEM Specimen ID to identify the mismatched information between the JEM Patch Tube and LIMS Patch Tube!")
        print()
        for index, row in test_mismatch_container_df.iterrows():
            print(f"{num}) JEM Specimen ID: {row['jem-id_cell_specimen']}")
            print(f"   - JEM Patch Tube: {row['jem-id_patched_cell_container']}")
            print(f"   - LIMS Patch Tube: {row['lims-id_patched_cell_container']}")
            num+=1


def test_jem_projects(jem_df):
    """
    Tests for JEM project details. 

    Parameters:
        jem_df (dataframe): a pandas dataframe.

    Returns:
        print statement (string)
    """

    jem_df["jem-test_projects"] = np.where((jem_df["jem-project_name"] == "retrograde_labeling") & (jem_df["jem-status_reporter"] == "Positive"), "Correct - Retrograde Labeling",
                                  np.where((jem_df["jem-project_name"] == "retrograde_labeling") & (jem_df["jem-status_reporter"] != "Positive"), "Incorrect - Retrograde Labeling",
                                  np.where((jem_df["jem-project_name"] == "1% Biocytin Pilot") & (jem_df["jem-id_patched_cell_container"].str.startswith("PXS4")), "Correct - 1% Biocytin",
                                  np.where((jem_df["jem-project_name"] == "1% Biocytin Pilot") & (~jem_df["jem-id_patched_cell_container"].str.startswith("PXS4")), "Incorrect - 1% Biocytin",  
                                  np.where((jem_df["jem-project_name"] == "Extended PBS Morphology Pilot") & (jem_df["jem-id_patched_cell_container"].str.startswith("PXS4")), "Correct - Extended PBS Morphology",
                                  np.where((jem_df["jem-project_name"] == "Extended PBS Morphology Pilot") & (~jem_df["jem-id_patched_cell_container"].str.startswith("PXS4")), "Incorrect - Extended PBS Morphology",
                                  np.where((jem_df["jem-project_name"] == "1% Biocytin + Extended PBS Morphology Pilot") & (jem_df["jem-id_patched_cell_container"].str.startswith("PXS4")), "Correct - 1% Biocytin + Extended PBS Morphology",
                                  np.where((jem_df["jem-project_name"] == "1% Biocytin + Extended PBS Morphology Pilot") & (~jem_df["jem-id_patched_cell_container"].str.startswith("PXS4")), "Incorrect - 1% Biocytin + Extended PBS Morphology",  
                                  np.where((jem_df["jem-project_name"] == "Morphology Clearing") & (jem_df["jem-id_patched_cell_container"].str.startswith("PXS4")), "Correct - Morphology Clearing",
                                  np.where((jem_df["jem-project_name"] == "Morphology Clearing") & (~jem_df["jem-id_patched_cell_container"].str.startswith("PXS4")), "Incorrect - Morphology Clearing",
                                  np.where((jem_df["jem-project_name"] == "Picrotoxin Validation") & (jem_df["jem-id_patched_cell_container"].str.startswith("PXS4")), "Correct - Picrotoxin Validation",
                                  np.where((jem_df["jem-project_name"] == "Picrotoxin Validation") & (~jem_df["jem-id_patched_cell_container"].str.startswith("PXS4")), "Incorrect - Picrotoxin Validation",
                                  np.where((jem_df["jem-project_name"] == "None") & (~jem_df["jem-id_patched_cell_container"].str.startswith("PXS4")), "Correct - No Project",
                                  np.where((jem_df["jem-project_name"] == "None") & (jem_df["jem-id_patched_cell_container"].str.startswith("PXS4")), "Incorrect - No Project","Not Applicable"))))))))))))))
    test_jem_proj = jem_df[jem_df["jem-test_projects"].str.startswith("Incorrect")]

    # Row numbering
    num = 1

    if len(test_jem_proj) > 0:
        print("\n#-----Incorrect JEM Project Details-----#")
        print("Description:")
        print("In the JEM form, please correct the project details based on the JEM Project!")
        print()
        for index, row in test_jem_proj.iterrows():
            print(f"{num}) JEM Specimen ID: {row['jem-id_cell_specimen']}")
            print(f"   - JEM Project: {row['jem-project_name']}")
            print(f"   - JEM Patch Tube: {row['jem-id_patched_cell_container']}")
            print(f"   - JEM Reporter Status: {row['jem-status_reporter']}")
            num+=1


#-----Weekly Transcriptomics Functions-----#
def generate_weekly_report(group):
    """
    Generates the weekly transcriptomics report.

    Parameters:
        None

    Returns:
        An excel file with a weekly transcriptomics report based on a user specified date range.
    """

    # Excel document details 
    norm_d = {"font_name":"Calibri", "font_size":11, "align":"left", "bold": False, "num_format":"0.0"}
    head_d = norm_d.copy()

    # Directories
    submit_report_dir = PureWindowsPath(Path("//aibsfileprint/public/MolBio/RNASeq-SingleCell/PatchSeq/Ephys_weekly_reports"))

    if group == "ivscc":
        # Directories
        ivscc_temporary_report_dir = "//allen/programs/celltypes/workgroups/279/Patch-Seq/transcriptomics_reports/ivscc_transcriptomics_reports/weekly_transcriptomics_reports/temporary"
        ivscc_saved_report_dir = PureWindowsPath(Path(ivscc_temporary_report_dir))
        # File name 
        name_report = "ps_transcriptomics_report"
    if group == "hct":
        # Directories
        hct_temporary_report_dir = "//allen/programs/celltypes/workgroups/279/Patch-Seq/transcriptomics_reports/hct_transcriptomics_reports/weekly_transcriptomics_reports/temporary"
        hct_saved_report_dir = PureWindowsPath(Path(hct_temporary_report_dir))
        # File name 
        name_report = "ps_transcriptomics_report_HCT"

    # Generate date variables
    day_prev_monday, day_curr_sunday = generate_dates()
    # User prompts
    dt_start, dt_end, day_prev_monday, day_curr_sunday = user_prompts_weekly(day_prev_monday, day_curr_sunday)
    # Create weekly transcriptomics report name
    date_name_report = "%s-%s_%s.xlsx" %(day_prev_monday, day_curr_sunday, name_report)  #name_report[0:-5]


    # Generate lims_df and jem_df with only patch tubes
    if group == "ivscc":
        jem_df = generate_jem_df("ivscc", "only_patch_tubes")
    if group == "hct":
        jem_df = generate_jem_df("hct", "only_patch_tubes")
    # Generate jem_df in daily transcriptomics report format
    jem_df = generate_weekly_jem_df(jem_df, dt_start, dt_end)

    if len(jem_df) > 0:
        try:
            if group == "ivscc":
                # Save dataframe as an excel sheet    
                save_xlsx_weekly_report(jem_df, ivscc_temporary_report_dir, date_name_report, norm_d, head_d)
                # Message in the terminal
                terminal_message_weekly(day_prev_monday, day_curr_sunday, ivscc_saved_report_dir, submit_report_dir, jem_df)
            if group == "hct":
                # Save dataframe as an excel sheet    
                save_xlsx_weekly_report(jem_df, hct_temporary_report_dir, date_name_report, norm_d, head_d)
                # Message in the terminal
                terminal_message_weekly(day_prev_monday, day_curr_sunday, hct_saved_report_dir, submit_report_dir, jem_df)
        except IOError:
            print("\nUnable to save the excel sheet. \nMake sure you don't already have a file with the same name opened.")
    else:
        sys.exit(f"\nNo data present for successful experiments with patch tubes collected on {day_prev_monday} - {day_curr_sunday}.")


def generate_dates():
    """
    Generates the dates of the previous Monday and current Sunday of the week.

    Parameters:
        None

    Returns:
        day_prev_monday (string): a date of the previous Monday in the format of YYMMDD. 
        day_curr_sunday (string): a date of the current Sunday in the format of YYMMDD.
    """
    
    #-----Dates-----#
    dt_today = datetime.today() # datetime.datetime
    num_weekdays = dt_today.weekday()
    dt_prev_monday = dt_today - timedelta(days=num_weekdays, weeks=1) # datetime.datetime
    dt_curr_sunday = dt_prev_monday + timedelta(days = 6) # datetime.datetime
    day_prev_monday = dt_prev_monday.strftime("%y%m%d")
    day_curr_sunday = dt_curr_sunday.strftime("%y%m%d")
    
    return day_prev_monday, day_curr_sunday


def user_prompts_weekly(day_prev_monday, day_curr_sunday):
    """
    Prompts the user to enter the specified date range.

    Parameters:
        day_prev_monday (string): a date of the previous Monday in the format of YYMMDD. 
        day_curr_sunday (string): a date of the current Sunday in the format of YYMMDD.

    Returns:
        dt_start (datetime): a datetime of the start date.
        dt_end (datetime): a datetime of the end date.
        day_prev_monday (string): a date of the previous Monday in the format of YYMMDD. 
        day_curr_sunday (string): a date of the current Sunday in the format of YYMMDD.
    """

    # User prompts
    str_prompt1 = "\nWould you like to report on samples collected between %s and %s? (y / n): "  %(day_prev_monday, day_curr_sunday)
    valid_vals = ["y", "n"]
    str_prompt2 = "Please enter weekly report start date/Monday (YYMMDD): "
    str_prompt3 = "Please enter weekly report end date/Sunday (YYMMDD): "
    response1 = "\nPlease try again..."
    response2 = "\nPlease try again... date should be YYMMDD"

    # Ask for user input
    default_dates_state = validated_input(str_prompt1, response1, valid_vals)
    if default_dates_state == "n":
        day_prev_monday = validated_date_input(str_prompt2, response2, valid_options=None)
        day_curr_sunday = validated_date_input(str_prompt3, response2, valid_options=None)
        dt_start = datetime.strptime(day_prev_monday, "%y%m%d")
        dt_end = datetime.strptime(day_curr_sunday, "%y%m%d")
    else:
        dt_start = datetime.strptime(day_prev_monday, "%y%m%d")
        dt_end = datetime.strptime(day_curr_sunday, "%y%m%d")

    return dt_start, dt_end, day_prev_monday, day_curr_sunday


def generate_weekly_jem_df(df, dt_start, dt_end):
    """
    Generates a jem metadata dataframe based on the specified date range.

    Parameters:
        df (dataframe): a pandas dataframe.
        dt_start (datetime): a datetime of the start date.
        dt_end (datetime): a datetime of the end date.

    Returns:
        df (dataframe): a pandas dataframe.
    """
    
    output_dict = {"jem-id_patched_cell_container":"tubeID","jem-date_patch": "date", "jem-id_rig_user": "rigOperator", "jem-id_rig_number": "rigNumber", "jem-date_blank": "blankFillDate", "jem-date_internal": "internalFillDate",
                   "jem-project_name":"pilotName", "jem-status_reporter":"creCell", "jem-roi_major_minor": "manualRoi", "jem-depth": "cell_depth",
                   "jem-break_in_time_end":"timeWholeCellStart", "jem-time_exp_extraction_start":"timeExtractionStart",
                   "jem-pressure_extraction":"pressureApplied", "jem-time_exp_extraction_end":"timeExtractionEnd", "jem-pressure_retraction":"retractionPressureApplied",
                   "jem-time_exp_retraction_end":"timeRetractionEnd", "jem-nucleus_post_patch":"postPatch", "jem-res_final_seal":"endPipetteR", "jem-virus_enhancer": "virus_enhancer"}

    # Filter dataframe to user specified date
    df["jem-date_patch"] = pd.to_datetime(df["jem-date_patch"])
    df = df[df["jem-date_patch"].between(dt_start, dt_end)].copy()
    df["jem-date_patch"] = df["jem-date_patch"].dt.strftime("%m/%d/%Y")

    # Renaming columns names
    df = df.rename(columns=output_dict)
    df = df[output_dict.values()]

    # Sort by date and tubeID in ascending order
    df.sort_values(by=["date", "tubeID"], inplace=True)
    
    return df


def save_xlsx_weekly_report(df, dirname, spreadname, norm_d, head_d):
    """
    Save an excel spreadsheet from dataframe.
    
    Parameters:
        df : pandas dataframe
        dirname : string
        spreadname : string
        norm_d, head_d: dictionaries
    
    Returns:
        Saved .xlsx file with name spreadname in directory dirname.
    """

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(os.path.join(dirname, spreadname), engine="xlsxwriter", date_format="mm/dd/yyyy")
    
    # Convert the dataframe to an XlsxWriter Excel object.
    df.to_excel(writer, sheet_name='Sheet1', index=False)    
    
    # Get the xlsxwriter workbook and worksheet objects.
    workbook  = writer.book
    worksheet = writer.sheets['Sheet1']
    norm_fmt = workbook.add_format(norm_d)
    head_fmt = workbook.add_format(head_d)
    worksheet.set_column('A:S', 20, norm_fmt)
    
    # This is for weekly transcriptomics reports
    worksheet.set_column('M:M', 20, norm_fmt) # pressureApplied
    worksheet.set_column('O:O', 20, norm_fmt) # retractionPressureApplied
    worksheet.set_column('R:R', 20, norm_fmt) # endPipetteR

    # Write the column headers with the defined format.
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num, value, head_fmt)
    try:
        writer.save()
    except IOError:
        print("\nOh no! Unable to save spreadsheet :(\nMake sure you don't already have a file with the same name opened.")


def terminal_message_weekly(day_prev_monday, day_curr_sunday, saved_location, report_location, df):
    """
    Generates a message in the anaconda command prompt terminal for the user.

    Parameters:
        day_prev_monday (string): a date of the previous Monday in the format of YYMMDD. 
        day_curr_sunday (string): a date of the current Sunday in the format of YYMMDD.
        saved_location: a location of the IVSCC directory with the copies of generated weekly transcriptomics reports.
        report_location: a location of the directory to submit the generated weekly transcriptomics report.
        df (dataframe): a pandas dataframe.

    Returns:
        print statement (string)
    """

    print()
    print(f"Generated report for {day_prev_monday}-{day_curr_sunday}.")
    print()
    print(f"Saved report location: {saved_location}")
    print(f"Submit report location: {report_location}")
    print(f"Total Patch Tubes: {len(df)}")
    print()
    print("If all present information is correct, please create a copy from the saved report location and submit the report in the submit report location.")
