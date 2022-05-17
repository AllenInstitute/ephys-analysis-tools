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
import os
import pandas as pd
import sys
from datetime import datetime, date, timedelta
from pathlib import Path, PureWindowsPath
# File imports
from functions.io_functions import validated_input, validated_date_input
from functions.jem_functions import generate_jem_df
# Test imports
#import time # To measure program execution time


# File name 
name_report = "ps_metadata_report.xlsx"

# Directories
dir_temporary_report =  "//allen/programs/celltypes/workgroups/279/Patch-Seq/transcriptomics_reports/weekly_transcriptomics_reports/temporary"
dir_saved_report = PureWindowsPath(Path("//allen/programs/celltypes/workgroups/279/Patch-Seq/transcriptomics_reports/weekly_transcriptomics_reports/temporary"))
dir_submit_report = PureWindowsPath(Path("//aibsfileprint/public/MolBio/RNASeq-SingleCell/PatchSeq/Ephys_daily_reports"))

# Excel document details 
norm_d = {"font_name":"Calibri", "font_size":11, "align":"left", "bold": False, "num_format":"0.0"}
head_d = norm_d.copy()


#-----Functions-----#
def generate_weekly_report():
    """
    Generates the weekly transcriptomics report.

    Parameters:
        None

    Returns:
        An excel file with a weekly transcriptomics report based on a user specified date range.
    """

    # Generate date variables
    day_prev_monday, day_curr_sunday = generate_dates()
    # User prompts
    dt_start, dt_end, day_prev_monday, day_curr_sunday = user_prompts(day_prev_monday, day_curr_sunday)
    # Create weekly transcriptomics report name
    date_name_report = "%s-%s_%s.xlsx" %(day_prev_monday, day_curr_sunday, name_report[0:-5]) 

    # Generate jem_df
    jem_df = generate_jem_df("only_patch_tubes")
    # Generate jem_df in weekly transcriptomics report format
    jem_df = generate_weekly_jem_df(jem_df, dt_start, dt_end)

    if len(jem_df) > 0:
        try:
            # Save dataframe as an excel sheet
            save_xlsx_weekly_report(jem_df, dir_temporary_report, date_name_report, norm_d, head_d)
            # Message in the terminal
            terminal_message(day_prev_monday, day_curr_sunday, dir_saved_report, dir_submit_report, jem_df)
        except IOError:
            print("\nUnable to save the excel sheet. \nMake sure you don't already have a file with the same name opened.")
    else:
        sys.exit(f"No JSON data for successful experiments on {last_monday_str}-{following_sunday_str}.")


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


def user_prompts(day_prev_monday, day_curr_sunday):
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
                   "jem-time_exp_whole_cell_start":"timeWholeCellStart", "jem-time_exp_extraction_start":"timeExtractionStart",
                   "jem-pressure_extraction":"pressureApplied", "jem-time_exp_extraction_end":"timeExtractionEnd", "jem-pressure_retraction":"retractionPressureApplied",
                   "jem-time_exp_retraction_end":"timeRetractionEnd", "jem-nucleus_post_patch":"postPatch", "jem-res_final_seal":"endPipetteR", "jem-virus_enhancer": "virus_enhancer"}

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


def terminal_message(day_prev_monday, day_curr_sunday, saved_location, report_location, df):
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


if __name__ == "__main__":
	#start = time.time()
	generate_weekly_report()
	#print("Program was executed in", round(time.time()-start, 2), "seconds.")
