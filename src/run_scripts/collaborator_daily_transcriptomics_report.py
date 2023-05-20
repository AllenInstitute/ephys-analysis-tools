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
# File imports
from functions.file_functions import get_jsons, load_data_variables
from functions.jem_data_set import JemDataSet
from functions.io_functions import validated_input, validated_date_input,save_xlsx


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
- 102-01-045-10: CTY IVSCC (Mouse/NHP) # IVSCC
- 102-01-061-20.3 : CTY BICAN Human and NHP Atlas # IVSCC
- 122-01-002-20.2.1 : AIND Thalamus U19 # IVSCC
- 102-04-006-20 : MSP Measuring Consciousness Ph2 (TBD) # HCT
- 102-01-051-20: CTY Neuromodulation of NHP Cell Types # HCT

project_dictionary details: Old project codes
- 102-01-020-20: CTY BRAIN Human Cell Types (Human Acute/Culture, U01 shipping pilot) # IVSCC (10/01/2017 - 6/03/2022) 
- 102-04-009-10: CTY SR: Targeted CNS Gene Therapy (Dravet pilot) # IVSCC (Dates?)
"""


# Load json file
data_variables = load_data_variables()

group = "collab"

if group == "collab":
    jem_df = generate_jem_df("collab", "only_patch_tubes")

# Make CSV
json_dir = "//allen/programs/celltypes/workgroups/279/Patch-Seq/collab-metadata-files"
jem_df.to_csv(os.path.join(json_dir, "collab_daily.csv"), index=False)

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

    if group == "collab":
        # Directories
        collab_temporary_report_dir = "//allen/programs/celltypes/workgroups/279/Patch-Seq/collab-metadata-files"
        collab_saved_report_dir = PureWindowsPath(Path(collab_temporary_report_dir))
        # File name 
        name_report = "ps_transcriptomics_report_collab"

    # Get last business day
    last_bday, last_bday_str = generate_last_business_day()

    # User prompts
    date_report, dt_report = user_prompts_daily(last_bday, last_bday_str)
    # Create daily transcriptomics report name
    date_name_report = "%s_%s.xlsx" %(date_report, name_report)

    # Generate lims_df and jem_df with only patch tubes
    if group == "collab":
        jem_df = generate_jem_df("collab", "only_patch_tubes")
    # Generate jem_df in daily transcriptomics report format
    jem_df = generate_daily_jem_df(jem_df, dt_report)

    try:
        # Renaming columns names
        jem_df.rename(columns=data_variables["collab_daily_tx_report_dictionary"], inplace=True)
        jem_df = jem_df[data_variables["collab_daily_tx_report_dictionary"].values()]
        jem_df.insert(loc=3, column="Library Prep Day1 Date", value="")
        jem_df.sort_values(by="Patch Tube Name", inplace=True)
        if group == "collab":
            # Save dataframe as an excel sheet    
            save_xlsx(jem_df, collab_temporary_report_dir, date_name_report, norm_d, head_d)
    except IOError:
            print("\nUnable to save the excel sheet. \nMake sure you don't already have a file with the same name opened.")


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

    
if __name__ == "__main__":
    generate_daily_report("collab")
