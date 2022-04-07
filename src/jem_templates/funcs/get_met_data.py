"""
-----------------------------------
File name: get_met_data.py
Maintainer: Ramkumar Rajanbabu
-----------------------------------
Author: Agata B
Date/time created: 7/12/2017
Description: Creating jem_metadata.csv
-----------------------------------
"""


# Imports
import json
import os
import numpy as np
import pandas as pd
from datetime import datetime
from dateutil import parser
from file_funcs import get_jsons
from jem_data_set import JemDataSet


# Read json data from file to import jem_dictionary
with open("C:/Users/ramr/Documents/Github/ai_repos/ephys_analysis_tools/src/jem_templates/python_files/data_variables.json") as json_file:
    data_variables = json.load(json_file)

# Directories
report_dir = 'C:/Users/ramr/Documents/Github/ai_repos/ephys_analysis_tools/src/jem-test/'
constants_dir = os.path.join(report_dir, "constants")

json_dir  = "//allen/programs/celltypes/workgroups/279/Patch-Seq/all-metadata-files"
metadata_dir = "//allen/programs/celltypes/workgroups/279/Patch-Seq/compiled-jem-data/raw_data"
output_dir = "//allen/programs/celltypes/workgroups/279/Patch-Seq/compiled-jem-data/test_data"


def make_metadata_csv(start_day_str, fn, default_json_dir=None):
    """Returns dataframed and saves 2 .csv with JEM data since the provided date, for samples with and without tubes.
    
    Parameters
    ----------
    default_json_dir : string (default None)
        Location of JEM files. None points to:
        '//allen/programs/celltypes/workgroups/279/Patch-Seq/all-metadata-files'

    start_day_str : string (default '171001', start of IVSCC-MET pipeline)

    fn : string (default 'jem_metadata')
        Filename for output .csv file. 

    Returns
    -------
    final_tube_df : pandas dataframe
        Metadata for samples with tubes
    na_df : pandas dataframe
        Metadata for NA samples (no tube sent for amplification)
    """

    # Date variables
    start_day = datetime.strptime(start_day_str, "%y%m%d").date()
    end_day = datetime.today().date()
    end_day_str = end_day.strftime("%y%m%d")

    """Get JEM pathnames that have been created since the report start date (with a 3 day buffer)"""
    delta_mod_date = (datetime.today().date() - start_day).days + 3
    jem_paths = get_jsons(dirname=json_dir, expt="PS", delta_days=delta_mod_date)

    """Flatten data in recent JSON files and output successful experiments (with tube IDs) in a dataframe. """
    jem_df = flatten_jem_data(jem_paths, start_day_str, end_day_str)
    print("Jem data flatten to dataframe using flatten_jem_data function in get_met_data.py")
    print()
    clean_jem_df = clean_metadata_fields(jem_df)
    print("Cleaned metadata fields using jem and roi_df from clean_metadata_fields functions in get_met_data.py")
    print()
    clean_jem_df = clean_metadata_roi_field(clean_jem_df)
    print("Cleaned roi metadata fields.")
    print()
    clean_jem_df.sort_values(by="date").to_csv(os.path.join(output_dir, "%s_wFAILURE.csv" %fn), encoding='utf-8-sig', index=False, date_format="%Y-%m-%d")
    print("Finished running jem_metdata_wFAILURE.csv")
    print()
    success_df = jem_df[jem_df["status"].str.contains("SUCCESS")]

    success_df["container"].fillna(value="NA", inplace=True)
    tube_df = success_df[~(success_df["container"].isin(["NA", "na", "N/A", "n/a"]))]
    na_df = success_df[success_df["container"].isin(["NA", "na", "N/A", "n/a"])]

    for data, data_fn in zip([tube_df, na_df], [fn, "NA_jem_metadata"]):
        if len(data) > 0:
            try:
                data.sort_values(by="date").to_csv(os.path.join(output_dir, "%s.csv" %data_fn), encoding='utf-8-sig', index=False, date_format="%Y-%m-%d")
            except IOError:
                print("\nOh no! Unable to save spreadsheet :(\nMake sure you don't already have a file with the same name opened.")

    return tube_df, na_df


def flatten_jem_data(jem_paths, start_day_str, end_day_str):
    """Compiles JEM files from paths, returning a pandas dataframe.
    
    Parameters
    ----------
    jem_paths : list of strings

    start_day_str : string

    end_day_str : string

    Returns
    -------
    jem_df : pandas dataframe
    """
    start_day = datetime.strptime(start_day_str, "%y%m%d").date()
    end_day = datetime.strptime(end_day_str, "%y%m%d").date()

    jem_df = pd.DataFrame()
    for jem_path in jem_paths:
        jem = JemDataSet(jem_path)
        expt_date = jem.get_experiment_date()
        if (expt_date >= start_day.strftime("%Y-%m-%d")) and (expt_date <= end_day.strftime("%Y-%m-%d")):
            slice_data = jem.get_data()
            jem_df = pd.concat([jem_df, slice_data], axis=0, sort=True)
    jem_df.reset_index(drop=True, inplace=True)

    if len(jem_df) == 0:
        print("No JEM data found for experiments between %s and %s" %(start_day_str, end_day_str))
        #jem_df = pd.DataFrame(columns=output_cols)

    return jem_df


def clean_metadata_fields(df):
    """Cleans up some inconsistent fields in JEM data.
    
    Parameters
    ----------
    df : pandas dataframe
    roi_df : pandas dataframe

    Returns
    -------
    df : pandas dataframe
    """

    """Clean up post patch naming convention."""
    df["extraction.postPatch"].replace({
        "Nucleated":"nucleus_present", "Partial-Nucleus":"nucleus_present", 
        "No-Seal": "nucleus_absent", "Outside-Out":"nucleus_absent"}, inplace=True)

    """Parse JEM date."""
    df.loc[:,"jem_date_dt"] = df["date"].apply(lambda x: str(parser.parse(x).date()))
    df.loc[:,"jem_date_m"] = df["jem_date_dt"].apply(lambda x: "-".join([x.split("-")[0], x.split("-")[1]]))

    return df


def clean_metadata_roi_field(df):
    """Cleans up roi field in JEM metadata."""

    # Replace values in column (roi-major_minor)
    df["roi"] = df["roi"].replace({"layer ": "L"}, regex=True)
    df["roi"] = df["roi"].replace({"/": "-"}, regex=True)
    df["roi"] = df["roi"].replace({"MH": "EPIMH", "LH": "EPILH",
                                   "HIPCA1": "HIP_CA1", "HIPDG-mo": "HIP_DG-mo", "HIPDG-sg": "HIP_DG-sg",
                                   "RSP1": "RSP_L1", "RSP2-3": "RSP_L2-3", "RSP5": "RSP_L5", "RSP6a": "RSP_L6a", "RSP6b": "RSP_L6b",
                                   "VISp, layer 1": "VISp1", "VISp, layer 2/3": "VISp2/3", "VISp, layer 4": "VISp4", "VISp, layer 5": "VISp5", "VISp, layer 6a": "VISp6a", "VISp, layer 6b": "VISp6b",
                                   "FCx, layer 1": "FCx1", "FCx, layer 2": "FCx2", "FCx, layer 3": "FCx3", "FCx, layer 4": "FCx4", "FCx, layer 5": "FCx5",
                                   "TCx, L2": "TCx2", "TCx, L2-3": "TCx2-3", "TCx, L3": "TCx3", "TCx, L5": "TCx5",
                                   "CB": "CBXmo"}, regex=False)
    df["roi"] = df["roi"].replace(data_variables["roi_dictionary"], regex=True)

    # Creating roi_major and roi_minor columns
    roi = df["roi"].str.split("_", n=1, expand=True) # Splitting roi_major and roi_minor
    df["roi_major"] = roi[0] # Choosing column with roi_major
    df["roi_minor"] = roi[1] # Choosing column with roi_minor

    # Creating roi_super column
    df["roi_super"] = df["roi_major"].replace({roi_cor: "Cortical" for roi_cor in data_variables["cortical_list"]}, regex=True)
    df["roi_super"] = df["roi_super"].replace({roi_sub: "Subcortical" for roi_sub in data_variables["subcortical_list"]}, regex=True)
    df["roi_super"] = df["roi_super"].replace({"NA": "Unknown"}, regex=True)

    return df

