"""
---------------------------------------------------------------------
File name: jem_template.py
Maintainer: Ramkumar Rajanbabu
---------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 10/01/2021
Description: Template for creating master_jem.csv and master_jem.xlsx
---------------------------------------------------------------------
"""


# Imports
import pandas as pd
import os
import numpy as np


# Dictionaries
jem_dictionary = {
    # date
    "acsfProductionDate": "jem-date_acsf", "blankFillDate": "jem-date_blank", "internalFillDate": "jem-date_internal", "date": "jem-date_patch",
    # depth
    "depth": "jem-depth_current", "approach.depth": "jem-depth_old", "cell_depth": "lims-depth", #LIMS?
    # health
    "approach.cellHealth": "jem-health_cell", "extraction.fillquality": "jem-health_fill_quality", "approach.sliceHealth": "jem-health_slice",
    "sliceQuality": "jem-health_slice_initial", "sliceQualityFinal": "jem-health_slice_final",
    # id
    "pipetteSpecName": "jem-id_cell_specimen", "container": "jem-id_patched_cell_container", "limsSpecName": "jem-id_slice_specimen",
    "rigNumber": "jem-id_rig_number", "rigOperator": "jem-id_rig_user",
    "name": "lims-id_cell_specimen", "specimen_ID": "lims-id_cell_specimen_id", "full_genotype": "lims-id_slice_genotype", "organism_name": "lims-id_species", #LIMS?
    # notes
    "extraction.extractionNotes": "jem-notes_extraction", "freeFailureNotes": "jem-notes_failure", "sliceNotes": "jem-notes_overall", "qcNotes": "jem-notes_qc",
    # nucleus
    "extraction.endPipetteR": "jem-nucleus_end_seal_res", "recording.pipetteR": "jem-nucleus_pipette_res", "extraction.postPatch": "jem-nucleus_post_patch", "extraction.nucleus": "jem-nucleus_sucked",
    # options
    "attempt": "jem-options_attempt", "failureNotes": "jem-options_failure", "successNotes": "jem-options_success", "virus_enhancer": "jem-options_virus_enhancer",
    # pressure
    "extraction.pressureApplied": "jem-pressure_extraction", "extraction.retractionPressureApplied": "jem-pressure_retraction",
    # project
    "approach.pilotName": "jem-project_name", "approach.project_retrograde_labeling_hemisphere": "jem-project_retrograde_labeling_hemisphere",
    "approach.project_retrograde_labeling_region": "jem-project_retrograde_labeling_region", "approach.project_retrograde_labeling_exp": "jem-project_retrograde_labeling_exp",
    # region
    "roi": "jem-region_major_minor", "roi_major": "jem-region_major", "roi_minor": "jem-region_minor",
    # status
    "approach.creCell": "jem-status_reporter", "status": "jem-status_success_fail",
    # time
    "recording.timeStart": "jem-time_exp_approach_start", "extraction.timeChannelRecordingEnd": "jem-time_exp_channel_end", "extraction.timeEnd": "jem-time_exp_end_old", 
    "extraction.timeExtractionEnd": "jem-time_exp_extraction_end", "extraction.timeExtractionStart": "jem-time_exp_extraction_start",
    "extraction.timeRetractionEnd": "jem-time_exp_retraction_end_current", "recording.timeWholeCellStart": "jem-time_exp_whole_cell_start",
    # test
    "lims_check": "test-mismatch_jem_lims"}

# Lists
ivscc_rig_users = ["aarono", "balreetp", "brianle", "dijonh", "gabrielal", "jessicat", "katherineb", "kristenh", "lindsayn", "lisak", "ramr", "rustym", "sarav"]
ivscc_rig_numbers = ["1", "2", "3", "4", "5", "6", "7", "8"]
columns_time = ["jem-time_exp_approach_start", "jem-time_exp_channel_end", "jem-time_exp_extraction_end", "jem-time_exp_extraction_start", "jem-time_exp_retraction_end", "jem-time_exp_whole_cell_start"]  


# compiled-jem-data input and output directory
path_input = "//allen/programs/celltypes/workgroups/279/Patch-Seq/compiled-jem-data/raw_data"
path_output = "//allen/programs/celltypes/workgroups/279/Patch-Seq/compiled-jem-data/formatted_data"

# JEM csv files
path_jem = os.path.join(path_input, "jem_metadata.csv")
path_jem_na = os.path.join(path_input, "NA_jem_metadata.csv")
path_jem_fail = os.path.join(path_input, "jem_metadata_wFAILURE.csv")

# Read all jem dataframes
jem_df = pd.read_csv(path_jem, low_memory=False)
jem_na_df = pd.read_csv(path_jem_na, low_memory=False)
jem_fail_df = pd.read_csv(path_jem_fail, low_memory=False)

# Replace status values
jem_df["status"] = jem_df["status"].replace({"SUCCESS (high confidence)": "SUCCESS"})
jem_fail_df["status"] = jem_fail_df["status"].replace({"SUCCESS (high confidence)": "SUCCESS", "NO ATTEMPTS": "FAILURE", "Failure": "FAILURE"})

# Filter dataframe to only FAILURE
jem_fail_df = jem_fail_df[jem_fail_df["status"] == "FAILURE"]

# Filter tubes and NAs
jem_df = jem_df[(jem_df["status"] == "SUCCESS")&(~jem_df["container"].isnull())]
jem_na_df = jem_na_df[(jem_na_df["status"] == "SUCCESS")&(jem_na_df["container"].isnull())]

# Replace container values
jem_na_df["container"] = jem_na_df["container"].replace({np.nan: "Not Applicable"})

# Merge all jem dataframes
master_jem_df = pd.concat([jem_df, jem_na_df, jem_fail_df], ignore_index=True, sort=False)

# Rename columns based on jem_dictionary
master_jem_df.rename(columns=jem_dictionary, inplace=True)

# Filter dataframe to only IVSCC Group 2017-Current
master_jem_df = master_jem_df[master_jem_df["jem-id_rig_user"].isin(ivscc_rig_users)]
master_jem_df = master_jem_df[master_jem_df["jem-id_rig_number"].isin(ivscc_rig_numbers)]

#Fix depth/time column and combining into one column
v_109_df = master_jem_df[master_jem_df["formVersion"] == "1.0.9"]
v_other_df = master_jem_df[master_jem_df["formVersion"] != "1.0.9"]
v_109_df.rename(columns={"jem-depth_old": "jem-depth", "jem-time_exp_end_old": "jem-time_exp_retraction_end"}, inplace=True)
v_other_df.rename(columns={"jem-depth_current": "jem-depth", "jem-time_exp_retraction_end_current": "jem-time_exp_retraction_end"}, inplace=True)
master_jem_df = pd.concat([v_109_df, v_other_df], sort=True)

# Fix datetime column to only date column
split_date = master_jem_df["jem-date_patch"].str.split(" ", n=1, expand=True) # Splitting date and time into 2 columns
master_jem_df["jem-date_patch"] = split_date[0] # Choosing column with only the dates

# Remove timezones from time columns 
for col in columns_time:
    split_timezone = master_jem_df[col].str.split(" ", n=1, expand=True) # Splitting time and timezone into 2 columns
    master_jem_df[col] = split_timezone[0] # Choosing column with only the time

# Add in year, monnth, day columns
master_jem_df["jem-date_patch_y-m-d"] = master_jem_df["jem-date_patch"]
split_date = master_jem_df["jem-date_patch_y-m-d"].str.split("-", n=2, expand=True) # Splitting year, month and day
master_jem_df["jem-date_patch_y"] = split_date[0] # Choosing column with years
master_jem_df["jem-date_patch_m"] = split_date[1] # Choosing column with months
master_jem_df["jem-date_patch_d"] = split_date[2] # Choosing column with days

# Change date columns to a datetime
master_jem_df["jem-date_acsf"] = pd.to_datetime(master_jem_df["jem-date_acsf"])
master_jem_df["jem-date_blank"] = pd.to_datetime(master_jem_df["jem-date_blank"])
master_jem_df["jem-date_internal"] = pd.to_datetime(master_jem_df["jem-date_internal"])
master_jem_df["jem-date_patch"] = pd.to_datetime(master_jem_df["jem-date_patch"])

# Change date column format to MM/DD/YYYY
master_jem_df["jem-date_acsf"] = master_jem_df["jem-date_acsf"].dt.strftime("%m/%d/%Y")
master_jem_df["jem-date_blank"] = master_jem_df["jem-date_blank"].dt.strftime("%m/%d/%Y")
master_jem_df["jem-date_internal"] = master_jem_df["jem-date_internal"].dt.strftime("%m/%d/%Y")
master_jem_df["jem-date_patch"] = master_jem_df["jem-date_patch"].dt.strftime("%m/%d/%Y")

# Convert string column to float column and apply absolute value to columns
master_jem_df["jem-depth"] = pd.to_numeric(master_jem_df["jem-depth"], errors='coerce').abs()
master_jem_df["lims-depth"] = pd.to_numeric(master_jem_df["lims-depth"], errors='coerce').abs()
master_jem_df["jem-pressure_extraction"] = pd.to_numeric(master_jem_df["jem-pressure_extraction"], errors='coerce').abs()
master_jem_df["jem-pressure_retraction"] = pd.to_numeric(master_jem_df["jem-pressure_retraction"], errors='coerce').abs()
master_jem_df["jem-nucleus_pipette_res"] = pd.to_numeric(master_jem_df["jem-nucleus_pipette_res"], errors='coerce').abs()
master_jem_df["jem-nucleus_end_seal_res"] = pd.to_numeric(master_jem_df["jem-nucleus_end_seal_res"], errors='coerce').abs()

# Create duration columns
master_jem_df["jem-time_duration_exp"] = pd.to_datetime(master_jem_df["jem-time_exp_retraction_end"]) - pd.to_datetime(master_jem_df["jem-time_exp_whole_cell_start"])
master_jem_df["jem-time_duration_ext"] = pd.to_datetime(master_jem_df["jem-time_exp_extraction_end"]) - pd.to_datetime(master_jem_df["jem-time_exp_extraction_start"])
master_jem_df["jem-time_duration_ret"] = pd.to_datetime(master_jem_df["jem-time_exp_retraction_end"]) - pd.to_datetime(master_jem_df["jem-time_exp_extraction_end"])
master_jem_df["jem-time_duration_exp"] = (master_jem_df["jem-time_duration_exp"].astype('timedelta64[s]'))/60
master_jem_df["jem-time_duration_ext"] = (master_jem_df["jem-time_duration_ext"].astype('timedelta64[s]'))/60
master_jem_df["jem-time_duration_ret"] = (master_jem_df["jem-time_duration_ret"].astype('timedelta64[s]'))/60

# Test columns
master_jem_df["test-mismatch_depth"] = master_jem_df["jem-depth"] == master_jem_df["lims-depth"]
master_jem_df["test-mismatch_id_cell_specimen"] = master_jem_df["jem-id_cell_specimen"] == master_jem_df["lims-id_cell_specimen"]

# Replace values in columns
master_jem_df["jem-region_major_minor"] = master_jem_df["jem-region_major_minor"].replace(to_replace="layer ", value="L", regex=True)
master_jem_df["jem-region_major_minor"] = master_jem_df["jem-region_major_minor"].replace(to_replace="/", value="-", regex=True)
master_jem_df["jem-region_minor"] = master_jem_df["jem-region_minor"].replace(to_replace="layer ", value="L", regex=True)
master_jem_df["jem-region_minor"] = master_jem_df["jem-region_minor"].replace(to_replace="/", value="-", regex=True)
master_jem_df["jem-health_cell"] = master_jem_df["jem-health_cell"].replace({"None": np.nan})
master_jem_df["jem-project_name"] = master_jem_df["jem-project_name"].replace({np.nan: "None"})
master_jem_df["jem-health_slice_initial"] = master_jem_df["jem-health_slice_initial"].replace({"Damaged": "Damage (Tissue Processing)", "Good": "Healthy","Wave of Death": "Wave of Death (after 30 min)", "'Wave of Death'": "Wave of Death (after 30 min)"})
master_jem_df["jem-status_reporter"] = master_jem_df["jem-status_reporter"].replace({"Cre+": "Positive", "Cre-": "Negative", "human": np.nan, "None": np.nan})

# Convert column to integer column
master_jem_df["jem-health_cell"] = master_jem_df["jem-health_cell"].fillna(value=0)
master_jem_df["jem-options_attempt"] = master_jem_df["jem-options_attempt"].fillna(value=0)
master_jem_df["jem-health_cell"] = master_jem_df["jem-health_cell"].astype(int)
master_jem_df["jem-options_attempt"] = master_jem_df["jem-options_attempt"].astype(int)
master_jem_df["jem-id_rig_number"] = master_jem_df["jem-id_rig_number"].astype(int)

# Add a new column
master_jem_df["jem-nucleus_post_patch_detail"] = pd.np.where(((master_jem_df["jem-nucleus_post_patch"]=="nucleus_present")|(master_jem_df["jem-nucleus_post_patch"]=="entire_cell"))&(master_jem_df["jem-nucleus_end_seal_res"]>=1000), "Nuc-giga-seal",
                                                            pd.np.where(((master_jem_df["jem-nucleus_post_patch"]=="nucleus_present")|(master_jem_df["jem-nucleus_post_patch"]=="entire_cell"))&(master_jem_df["jem-nucleus_end_seal_res"]<1000), "Nuc-low-seal",
                                                            pd.np.where(master_jem_df["jem-nucleus_post_patch"]=="nucleus_absent", "No-seal",
                                                            pd.np.where(master_jem_df["jem-nucleus_post_patch"]=="unknown", "Unknown", "Not applicable"))))

# Drop columns
master_jem_df.drop(columns=["approach.pilotTest01", "approach.pilotTest04", "approach.pilotTest05", "approach.pilotTestYN",
                           "badSweeps", "experimentType", "extraction.extractionObservations", "extraction.sampleObservations",
                           "extraction.timeRetractionStart", "extraction.tubeID", "failureCause", "flipped", "formVersion",
                           "internalSolution.concentrationBiocytin", "internalSolution.concentrationRnaseInhibitor",
                           "internalSolution.version", "internalSolution.volume", "jem-health_slice", "recording.accessR",
                           "recording.humanCellTypePrediction", "recording.membraneV", "recording.rheobase", "jem_created",
                           "jem_date_dt", "jem_date_m", "jem-depth_current", "jem-depth_old", "jem-time_exp_end_old",
                           "jem-time_exp_retraction_end_current", "extraction.pilotNameExtra","wellID"], inplace=True)

# Sort columns alphabetically and by date
master_jem_df = master_jem_df.sort_index(axis=1)
master_jem_df.sort_values(by=["jem-date_patch_y-m-d", "jem-id_slice_specimen", "jem-id_cell_specimen", "jem-options_attempt"], inplace=True)

# Dataframe to csvs and excel
master_jem_df.to_csv(path_or_buf= os.path.join(path_output, "master_jem.csv"), index=False)
master_jem_df.to_excel(excel_writer= os.path.join(path_output, "master_jem.xlsx"), index=False)
