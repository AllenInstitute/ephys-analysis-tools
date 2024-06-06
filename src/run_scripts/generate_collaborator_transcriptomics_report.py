"""
---------------------------------------------------------------------
File name: generate_collaborator_transcriptomics_report.py
Maintainer: Ramkumar Rajanbabu
---------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 05/19/2023
Description: Generate Collaborator transcriptomics report (excel document)
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
from functions.io_functions import validated_input, validated_date_input, get_jsons_walk, save_xlsx
from functions.jem_functions import generate_jem_df, flatten_collab_jem_data, collab_fix_field_formatting
from functions.lims_functions import create_collab_transcriptomics_query_to_df


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
jem_df = flatten_collab_jem_data(jem_paths)
print("\n", "Selected JSON directory:", json_dir)

shipment_file_name = os.path.basename(os.path.normpath(json_dir))
transcriptomic_report_name = shipment_file_name + "_" + name_report

if len(jem_df) == 0:
    sys.exit("No JSON data found for successful experiments in '%s' directory." %os.path.basename(json_dir))

jem_df.rename(columns=data_variables["jem_dictionary"], inplace=True)
jem_df = collab_fix_field_formatting(jem_df)
# Adding new column with project codes
jem_df["project_code"] = np.where((jem_df["jem-id_patched_cell_container"].str.startswith("PDS4", "PRS4")), data_variables["project_dictionary"]["PGA"],
                                   data_variables["project_dictionary"]["HMBA"])
# Sort by jem-id_patched_cell_container in ascending order
jem_df.sort_values(by=["jem-date_patch", "jem-id_patched_cell_container"], inplace=True)

# Generate lims_df
lims_df = create_collab_transcriptomics_query_to_df()

# Merge dataframes by left join based on cell name
jem_lims_df = pd.merge(left=jem_df, right=lims_df, left_on="jem-id_cell_specimen", right_on="lims_cell_name", how="left")

if (len(jem_df) > 0) & (len(lims_df) > 0):
    try:
        # Renaming columns names
        jem_lims_df.rename(columns = data_variables["collab_daily_tx_report_dictionary"], inplace=True)
        jem_lims_df = jem_lims_df[data_variables["collab_daily_tx_report_dictionary"].values()]
        jem_lims_df.insert(loc=3, column="Library Prep Day1 Date", value="")
        jem_lims_df.sort_values(by=["Patch Date", "Patch Tube Name"], inplace=True)
        save_xlsx(jem_lims_df, external_temporary_report_dir, transcriptomic_report_name, norm_d, head_d)
        print("\n", f"Created the {transcriptomic_report_name}.")
    except IOError:
            print("\nUnable to save the excel sheet. \nMake sure you don't already have a file with the same name opened.")
