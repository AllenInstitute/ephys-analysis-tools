"""
---------------------------------------------------------------------
File name: jem_variables.py
Maintainer: Ramkumar Rajanbabu
---------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 10/02/2021
Description: Template for lists/dictionaries to import to jem_template.py
---------------------------------------------------------------------
"""

import json


# Lists
ivscc_rig_users_list = ["aarono", "balreetp", "brianle", "dijonh", "gabrielal", "jessicat", "katherineb", "kristenh", "lindsayn", "lisak", "ramr", "rustym", "sarav"]
ivscc_rig_numbers_list = ["1", "2", "3", "4", "5", "6", "7", "8"]
columns_time_list = ["jem-time_exp_approach_start", "jem-time_exp_whole_cell_start", "jem-time_exp_extraction_start", "jem-time_exp_extraction_end", "jem-time_exp_retraction_end", "jem-time_exp_channel_end"]
column_order_list = ["jem-date_patch", "jem-date_patch_y-m-d", "jem-date_patch_y", "jem-date_patch_m", "jem-date_patch_d", "jem-date_acsf", "jem-date_blank", "jem-date_internal",
                     "jem-id_slice_specimen", "jem-id_cell_specimen", "jem-id_patched_cell_container", "jem-id_rig_user", "jem-id_rig_number",
                     "jem-status_attempt", "jem-status_success_failure", "jem-status_success", "jem-status_failure", "jem-status_reporter",
                     "jem-region_major_minor", "jem-region_major", "jem-region_minor",
                     "jem-health_slice_initial", "jem-health_slice_final", "jem-health_cell", "jem-health_fill_quality",
                     "jem-nucleus_post_patch_detail", "jem-nucleus_post_patch", "jem-nucleus_sucked",
                     "jem-res_initial_seal", "jem-res_final_seal", "jem-depth", "jem-pressure_extraction", "jem-pressure_retraction",
                     "jem-time_duration_exp", "jem-time_duration_ext", "jem-time_duration_ret",
                     "jem-time_exp_approach_start", "jem-time_exp_whole_cell_start", "jem-time_exp_extraction_start", "jem-time_exp_extraction_end", "jem-time_exp_retraction_end", "jem-time_exp_channel_end",
                     "jem-virus_enhancer", "jem-project_name", "jem-project_retrograde_labeling_hemisphere", "jem-project_retrograde_labeling_region", "jem-project_retrograde_labeling_exp",
                     "jem-notes_overall", "jem-notes_qc", "jem-notes_extraction", "jem-notes_failure",
                     "lims-id_species", "lims-id_cell_specimen", "lims-id_cell_specimen_id", "lims-id_slice_genotype", "lims-depth",
                     "test-mismatch_jem_lims", "test-mismatch_id_cell_specimen", "test-mismatch_depth"]
drop_list = ["approach.pilotTest01", "approach.pilotTest04", "approach.pilotTest05", "approach.pilotTestYN",
             "badSweeps",
             "experimentType", "extraction.extractionObservations", "extraction.pilotNameExtra", "extraction.sampleObservations", "extraction.timeRetractionStart", "extraction.tubeID",
             "failureCause", "flipped", "formVersion",
             "internalSolution.concentrationBiocytin", "internalSolution.concentrationRnaseInhibitor", "internalSolution.version", "internalSolution.volume",
             "jem-depth_current", "jem-depth_old", "jem-health_slice", "jem-time_exp_end_old", "jem-time_exp_retraction_end_current", "jem_created", "jem_date_dt", "jem_date_m",
             "recording.accessR", "recording.humanCellTypePrediction", "recording.membraneV", "recording.rheobase",
             "wellID"]

# Dictionaries
jem_dictionary = {
    "date": "jem-date_patch",
    "acsfProductionDate": "jem-date_acsf",
    "blankFillDate": "jem-date_blank", 
    "internalFillDate": "jem-date_internal",
    "approach.sliceHealth": "jem-health_slice",
    "sliceQuality": "jem-health_slice_initial",
    "sliceQualityFinal": "jem-health_slice_final",
    "approach.cellHealth": "jem-health_cell",
    "extraction.fillquality": "jem-health_fill_quality", 
    "pipetteSpecName": "jem-id_cell_specimen",
    "container": "jem-id_patched_cell_container",
    "limsSpecName": "jem-id_slice_specimen",
    "rigNumber": "jem-id_rig_number",
    "rigOperator": "jem-id_rig_user",
    "sliceNotes": "jem-notes_overall",
    "qcNotes": "jem-notes_qc",
    "extraction.extractionNotes": "jem-notes_extraction",
    "freeFailureNotes": "jem-notes_failure", 
    "extraction.postPatch": "jem-nucleus_post_patch",
    "extraction.nucleus": "jem-nucleus_sucked",
    "recording.pipetteR": "jem-res_initial_seal",
    "extraction.endPipetteR": "jem-res_final_seal",
    "depth": "jem-depth_current",
    "approach.depth": "jem-depth_old",
    "attempt": "jem-status_attempt",
    "status": "jem-status_success_failure",
    "successNotes": "jem-status_success",
    "failureNotes": "jem-status_failure",
    "approach.creCell": "jem-status_reporter",
    "roi": "jem-region_major_minor",
    "roi_major": "jem-region_major",
    "roi_minor": "jem-region_minor",
    "extraction.pressureApplied": "jem-pressure_extraction",
    "extraction.retractionPressureApplied": "jem-pressure_retraction",
    "virus_enhancer": "jem-virus_enhancer",
    "approach.pilotName": "jem-project_name", 
    "approach.project_retrograde_labeling_hemisphere": "jem-project_retrograde_labeling_hemisphere",
    "approach.project_retrograde_labeling_region": "jem-project_retrograde_labeling_region",
    "approach.project_retrograde_labeling_exp": "jem-project_retrograde_labeling_exp",
    "recording.timeStart": "jem-time_exp_approach_start",
    "recording.timeWholeCellStart": "jem-time_exp_whole_cell_start",
    "extraction.timeExtractionStart": "jem-time_exp_extraction_start",
    "extraction.timeExtractionEnd": "jem-time_exp_extraction_end",
    "extraction.timeRetractionEnd": "jem-time_exp_retraction_end_current",
    "extraction.timeChannelRecordingEnd": "jem-time_exp_channel_end",
    "extraction.timeEnd": "jem-time_exp_end_old",
    "organism_name": "lims-id_species",
    "name": "lims-id_cell_specimen",
    "specimen_ID": "lims-id_cell_specimen_id", 
    "full_genotype": "lims-id_slice_genotype",
    "cell_depth": "lims-depth",
    "lims_check": "test-mismatch_jem_lims"
}

# Write jem_dictionary to json
with open("jem_dictionary.json", 'w') as json_file:
    json.dump(jem_dictionary, json_file, indent=4)
