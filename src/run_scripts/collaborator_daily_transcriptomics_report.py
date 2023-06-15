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
from functions.io_functions import validated_input, validated_date_input, get_jsons_walk
from functions.jem_functions import generate_jem_df, fix_jem_versions_collab, add_jem_patch_tube_field
from functions.lims_functions import generate_external_lims_df


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

def save_xlsx(df, dirname, spreadname, norm_d, head_d):
    """Save an excel spreadsheet from dataframe
    
    Parameters
    ----------
    df : pandas dataframe
    dirname : string
    spreadname : string
    norm_d, head_d: dictionaries
    
    Returns
    -------
    Saved .xlsx file with name spreadname in directory dirname.
    """
    
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(os.path.join(dirname, spreadname), engine="xlsxwriter", date_format="mm/dd/yy")
    
    # Convert the dataframe to an XlsxWriter Excel object.
    df.to_excel(writer, sheet_name='Sheet1', index=False)    
    
    # Get the xlsxwriter workbook and worksheet objects.
    workbook  = writer.book
    worksheet = writer.sheets['Sheet1']
    norm_fmt = workbook.add_format(norm_d)
    head_fmt = workbook.add_format(head_d)
    worksheet.set_column('A:N', 26, norm_fmt)

    # Write the column headers with the defined format.
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num, value, head_fmt)
    try:
        writer.save()
    except IOError:
        print("\nOh no! Unable to save spreadsheet :(\nMake sure you don't already have a file with the same name opened.")


#-----Variables-----#
# Load json file
data_variables = load_data_variables()
# Ask user for input on file directory location
json_dir = askdirectory()
# File name
name_report = "ps_transcriptomics_report.xlsx"


# Excel document details 
norm_d = {"font_name":"Arial", "font_size":10, "align":"left", "bold": False, "num_format":"0.00"}
head_d = norm_d.copy()
head_d["bg_color"] = "#ABCF8F"

# Directories
external_temporary_report_dir = "//allen/programs/celltypes/workgroups/279/Patch-Seq/collaborator-data-warehouse/transcriptomic-reports/shipment-reports/temporary"

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
print("Selected JSON directory", json_dir, "\n")

shipment_file_name = os.path.basename(os.path.normpath(json_dir))
transcriptomic_report_name = shipment_file_name + "_" + name_report

if len(jem_df) == 0:
    sys.exit("No JSON data found for successful experiments in '%s' directory." %os.path.basename(json_dir))

# Rename columns based on jem_dictionary
jem_df.rename(columns=data_variables["jem_dictionary"], inplace=True)

# Add patch tube field
jem_df = add_jem_patch_tube_field(jem_df)
# Filters dataframe to only patched cell containers
filter_tubes = "only_patch_tubes"
if filter_tubes == "only_patch_tubes":
	jem_df = jem_df[(jem_df["jem-status_patch_tube"] == "Patch Tube")]

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

# Generate lims_df
lims_df = generate_external_lims_df()

#----------Merge jem_df and lims_df----------#
# Merge dataframes by outer join based on specimen id 
jem_lims_df = pd.merge(left=jem_df, right=lims_df, left_on="jem-id_cell_specimen", right_on="lims_cell_name", how="left")

if (len(jem_df) > 0) & (len(lims_df) > 0):
    # Adding new column with project codes
    jem_lims_df["project_code"] = np.where((jem_lims_df["jem-id_patched_cell_container"].str.startswith("PDS4", "PRS4")), data_variables["project_dictionary"]["mouse_human"], 
                                            data_variables["project_dictionary"]["nhp"])

    try:
        # Renaming columns names
        jem_lims_df.rename(columns = data_variables["collab_daily_tx_report_dictionary"], inplace=True)
        jem_lims_df = jem_lims_df[data_variables["collab_daily_tx_report_dictionary"].values()]
        jem_lims_df.insert(loc=3, column="Library Prep Day1 Date", value="")
        jem_lims_df.sort_values(by=["Patch Date", "Patch Tube Name"], inplace=True)
        save_xlsx(jem_lims_df, external_temporary_report_dir, transcriptomic_report_name, norm_d, head_d)
    except IOError:
            print("\nUnable to save the excel sheet. \nMake sure you don't already have a file with the same name opened.")
