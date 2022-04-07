"""
---------------------------------------------------------------------
File name: ivscc_weekly_transcriptomics_report.py
Maintainer: Ramkumar Rajanbabu
---------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 05/28/2021
Description: Generate weekly transcriptomics report (excel document)
---------------------------------------------------------------------
"""


#-----Imports-----#
import csv
import os
import sys
import pandas as pd
from datetime import datetime, date, timedelta
from iofuncs import validated_input, validated_date_input,save_xlsx
from ivscc_daily_transcriptomics_report import generate_jem_df
#import time # To measure program execution time


# File name 
name_report = "ps_metadata_report.xlsx"

# Directories
dir_temporary_report =  "//allen/programs/celltypes/workgroups/279/Patch-Seq/transcriptomics_reports/weekly_transcriptomics_reports/temporary"

# Excel document details 
norm_d = {"font_name":"Calibri", "font_size":11, "align":"left", "bold": False, "num_format":"0.0"}
head_d = norm_d.copy()


#-----Functions-----#
def generate_weekly_report():
    """
    Generates the weekly transcriptomics report.
    
    Output: An csv with a weekly transcriptomics report based on user specified start and end date.
    """

    # Generate date variables
    day_prev_monday, day_curr_sunday = generate_dates()
    # User prompts
    dt_start, dt_end = user_prompts(day_prev_monday, day_curr_sunday)
    # Create weekly transcriptomics report name
    date_name_report = "%s-%s_%s.xlsx" %(day_prev_monday, day_curr_sunday, name_report[0:-5]) 

    # Generate jem_df
    jem_df = generate_jem_df()
    # Generate jem_df in weekly transcriptomics report format
    jem_df = generate_weekly_jem_df(jem_df, dt_start, dt_end)

    if len(jem_df) > 0:
        try:
            # Save dataframe as an excel sheet
            save_xlsx(jem_df, dir_temporary_report, date_name_report, norm_d, head_d)
            # Message in the terminal
            terminal_message(day_prev_monday, day_curr_sunday)
        except IOError:
            print("\nUnable to save the excel sheet. \nMake sure you don't already have a file with the same name opened.")
    else:
        sys.exit(f"No JSON data for successful experiments on {last_monday_str}-{following_sunday_str}.")


def generate_dates():
    
    #-----Dates-----#
    dt_today = datetime.today() # datetime.datetime
    num_weekdays = dt_today.weekday()
    dt_prev_monday = dt_today - timedelta(days=num_weekdays, weeks=1) # datetime.datetime
    dt_curr_sunday = dt_prev_monday + timedelta(days = 6) # datetime.datetime
    day_prev_monday = dt_prev_monday.strftime("%y%m%d") # "YYMMDD"
    day_curr_sunday = dt_curr_sunday.strftime("%y%m%d") # "YYMMDD"
    
    return day_prev_monday, day_curr_sunday


def user_prompts(day_prev_monday, day_curr_sunday):
    """
    User prompts for date entry
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
        dt_start = datetime.strptime(day_prev_monday, "%y%m%d") # datetime.datetime
        dt_end = datetime.strptime(day_curr_sunday, "%y%m%d") # datetime.datetime
    else:
        dt_start = datetime.strptime(day_prev_monday, "%y%m%d") # datetime.datetime
        dt_end = datetime.strptime(day_curr_sunday, "%y%m%d") # datetime.datetime

    return dt_start, dt_end


def generate_weekly_jem_df(df, dt_start, dt_end):
    """
    Generate a jem metadata dataframe
    
    start_day:
    end_day:
    """
    
    output_dict = {"jem-id_patched_cell_container":"tubeID","jem-date_patch": "date", "jem-id_rig_user": "rigOperator", "jem-id_rig_number": "rigNumber", "jem-date_blank": "blankFillDate", "jem-date_internal": "internalFillDate",
                   "jem-project_name":"pilotName", "jem-status_reporter":"creCell", "jem-roi_major_minor": "manualRoi", "jem-depth_current": "cell_depth",
                   "jem-time_exp_whole_cell_start":"timeWholeCellStart", "jem-time_exp_extraction_start":"timeExtractionStart",
                   "jem-pressure_extraction":"pressureApplied", "jem-time_exp_extraction_end":"timeExtractionEnd", "jem-pressure_retraction":"retractionPressureApplied",
                   "jem-time_exp_retraction_end_current":"timeRetractionEnd", "jem-nucleus_post_patch":"postPatch", "jem-res_final_seal":"endPipetteR", "jem-virus_enhancer": "virus_enhancer"}

    # Change values to integers
    df["jem-id_rig_number"] = df["jem-id_rig_number"].astype(str)

    # Filter dataframe to user specified date
    df["jem-date_patch"] = pd.to_datetime(df["jem-date_patch"])
    df = df[df["jem-date_patch"].between(dt_start, dt_end)]
    df["jem-date_patch"] = df["jem-date_patch"].dt.strftime("%m/%d/%Y")

    # Renaming columns names
    df.rename(columns=output_dict, inplace=True)
    df = df[output_dict.values()]

    # Sort by date and tubeID in ascending order
    df.sort_values(by=["date", "tubeID"], inplace=True)
    
    return df

def terminal_message(day_prev_monday, day_curr_sunday):
    """
    Message in terminal
    """
    
    print(f"Generated report for {day_prev_monday}-{day_curr_sunday}.")


if __name__ == "__main__":
	#start = time.time()
	generate_weekly_report()
	#print("Program was executed in", round(time.time()-start, 2), "seconds.")
