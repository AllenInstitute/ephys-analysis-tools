"""
-----------------------------------------------------------------------
File name: generate_compiled_jem.py
Maintainer: Ramkumar Rajanbabu
-----------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 11/14/2022
Description: Template for generating jem_metadata.csv,
jem_metadata_wFAILURE.csv, and NA_jem_metadata.csv
-----------------------------------------------------------------------
"""


#-----Imports-----#
# General imports
import numpy as np
import os
import pandas as pd
from datetime import datetime, date, timedelta
# File imports
from functions.file_functions import get_jsons
from functions.jem_functions import flatten_jem_data
from functions.lims_functions import get_lims
# Test imports
import time # To measure program execution time


def generate_compiled_jem():
	"""
	Generates the complied jem data raw?

	Parameters:
	    None?

	Returns:
	    3 csvs?
	final_tube_df : pandas dataframe
	    Metadata for samples with tubes
	na_df : pandas dataframe
	    Metadata for NA samples (no tube sent for amplification)
	"""

	# General Variables
	file_name = "jem_metadata"

	# Directories
	json_dir  = "//allen/programs/celltypes/workgroups/279/Patch-Seq/all-metadata-files"
	output_dir = "//allen/programs/celltypes/workgroups/279/Patch-Seq/compiled-jem-data/raw_data"

	# Generate date variables
	# Date of today
	dt_today = datetime.today()
	date_today = dt_today.date()
	day_today = date_today.strftime("%y%m%d") # "YYMMDD"
	# Date of IVSCC Pipeline Start
	dt_ivscc_pipeline_start = datetime(2017, 10, 1)
	date_ivscc_pipeline_start = dt_ivscc_pipeline_start.date()
	day_ivscc_pipeline_start = date_ivscc_pipeline_start.strftime("%y%m%d") # "YYMMDD"

	delta_mod_date = (date_today - date_ivscc_pipeline_start).days + 3
	jem_paths = get_jsons(dirname=json_dir, expt="PS", delta_days=delta_mod_date)
	# Flatten JSON files (previous 30 day information) to pandas dataframe jem_df)
	jem_df = flatten_jem_data(jem_paths, day_ivscc_pipeline_start, day_today)
	jem_df.sort_values(by="date").to_csv(os.path.join(output_dir, "%s_wFAILURE.csv" %file_name), encoding='utf-8-sig', index=False, date_format="%Y-%m-%d")
	jem_df.sort_values(by=["date"], ascending=False, inplace=True)

	success_df = jem_df[jem_df["status"].str.contains("SUCCESS")]
	success_df["container"].fillna(value="NA", inplace=True)
	tube_df = success_df[~(success_df["container"].isin(["NA", "na", "N/A", "n/a"]))]
	na_df = success_df[success_df["container"].isin(["NA", "na", "N/A", "n/a"])]

	for data, data_file_name in zip([tube_df, na_df], [file_name, "NA_jem_metadata"]):
		if len(data) > 0:
			try:
				data.sort_values(by="date").to_csv(os.path.join(output_dir, "%s.csv" %data_file_name), encoding='utf-8-sig', index=False, date_format="%Y-%m-%d")
			except IOError:
				print("\nOh no! Unable to save spreadsheet :(\nMake sure you don't already have a file with the same name opened.")

	return tube_df, na_df


if __name__ == "__main__":
	start = time.time()
	_, _ = generate_compiled_jem()
	print("\nThe program was executed in", round(((time.time()-start)/60), 2), "minutes.")
