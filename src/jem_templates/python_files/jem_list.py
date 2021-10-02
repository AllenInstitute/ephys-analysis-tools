"""
---------------------------------------------------------------------
File name: jem_list.py
Maintainer: Ramkumar Rajanbabu
---------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 10/02/2021
Description: Template for lists to import to jem_template.py
---------------------------------------------------------------------
"""

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
