"""
-----------------------------------------------------------------------
File name: generate_ephys_pipeline_metrics.py
Maintainer: Ramkumar Rajanbabu
-----------------------------------------------------------------------
Author: Rusty Mann
Date/time created: 11/10/2022
Description: Template for generating ephys_pipeline_metrics.csv
-----------------------------------------------------------------------
"""


#-----Imports-----#
# General imports
import pg8000          
import pandas as pd
import os
import shutil
import json
import numpy as np
import pathlib
from datetime import date, datetime, timedelta
# File imports
from functions.lims_functions import get_lims_ephys, get_lims_sweep
# import zmq
# Test imports
import time # To measure program execution time


program_start_time = time.time()

# Directories
shiny_visp_mouse_path = '//allen/programs/celltypes/workgroups/rnaseqanalysis/shiny/patch_seq/star/mouse_patchseq_VISp_current/'
shiny_mtg_human_path = '//allen/programs/celltypes/workgroups/rnaseqanalysis/shiny/patch_seq/star/human/human_patchseq_MTG_current/'
data_path = "//allen/programs/celltypes/workgroups/279/Patch-Seq/ivscc-data-warehouse/data-sources"

today = date.today()
yesterday = today - timedelta(days=1)
yesterday

start_date = '2017-10-01'
end_date = str(today)

start_dt = datetime.strptime(start_date, '%Y-%m-%d')
end_dt = datetime.strptime(end_date, "%Y-%m-%d")

start = datetime.date(start_dt)
end = datetime.date(end_dt)

datesrange = pd.date_range(start=start, end=end)
print(datesrange)

# Add lims_df
df = get_lims_ephys()

df.columns = df.columns.astype("str")
df['recording_date'] = df['recording_date'].dt.strftime('%Y-%m-%d')


df['created_date'] = pd.to_datetime(df['created_date'])
df = df[df['created_date'] >= start_date]

store_dirs = list(df['storage_dir'])
storage_dirs  = ['/'+x for x in store_dirs]

#print('saving lims csv')
#df.to_csv(data_path + 'lims_data.csv')

for directory in storage_dirs:
    
    cell_row = df.loc[df['storage_dir'] == directory[1:]].index[0]
    cell_name = df.loc[cell_row, 'cell_name']
    roi_result = directory.split('/')[-2]
    roi_id = roi_result.split('_')[-1]

    for root, dirs, files in os.walk(directory):
        for fil in files:
            
            if fil.endswith('EPHYS_QC_V3_QUEUE_' + roi_id + '_output.json'):
                
                with open(directory + fil) as f:
                    ephysqc_data = json.load(f)
                
                qc_fail_tags = ephysqc_data["cell_state"]["fail_tags"]
                failed_qc= ephysqc_data["cell_state"]["failed_qc"]
                if failed_qc == True:
                    qc_status = "Failed"
                elif failed_qc == False:
                    qc_status = "Passed"
                else:
                    qc_status = None
                df.loc[cell_row, "QC status"] = qc_status

                # if qc_fail_tags:
                df.loc[cell_row, "ephys_qc_fail_tags"] = str(qc_fail_tags)

                if "electrode_0_pa missing value" in qc_fail_tags:
                    missing_e0 = True
                else:
                    missing_e0 = False
                df.loc[cell_row, "electrode_0_pa missing value"] = missing_e0

                if "Invalid seal (None)" in qc_fail_tags:
                    inv_seal = True
                else:
                    inv_seal = False
                df.loc[cell_row, "Invalid seal (None)"] = inv_seal

                if "Resistance ratio not available" in qc_fail_tags:
                    missing_rr = True
                else:
                    missing_rr = False
                df.loc[cell_row, "Resistance ratio not available"] = missing_rr

                if "Initial access resistance not available" in qc_fail_tags:
                    missing_ra = True
                else:
                    missing_ra = False
                df.loc[cell_row, "Initial access resistance not available"] = missing_ra

                if "No sweep states available" in qc_fail_tags:
                    no_sweep_states = True
                else:
                    no_sweep_states = False
                df.loc[cell_row, "No sweep states available"] = no_sweep_states

                if "No current clamps sweeps passed QC" in qc_fail_tags:
                    no_Iclamp_qc = True
                else:
                    no_Iclamp_qc = False
                df.loc[cell_row, "No current clamps sweeps passed QC"] = no_Iclamp_qc
                
            elif fil.endswith('EPHYS_NWB_STIMULUS_SUMMARY_V3_QUEUE_' + roi_id + '_output.json'):
                recording_stopped_sweeps = []
                NaN_stim_values = []
                missing_test_epoch = []
                missing_stim_epoch = []
                missing_exp_epoch = []

                with open(directory + fil) as f:
                    stimsum_data = json.load(f) 

                cell_tags = stimsum_data['cell_tags']
                sweep_features = stimsum_data['sweep_features']

                if cell_tags:
                    cell_tags = [x for x in cell_tags if x != "Blowout is not available"]
                    df.loc[cell_row, "stim_summary_cell_tags"] = str(cell_tags)
                else:
                    pass
                
                for sweep in sweep_features:
                    if 'tags' in sweep.keys():
                        tags = sweep['tags']
                        
                        if "Recording stopped before completing the experiment epoch" in tags:
                            recording_stopped_sweeps.append(sweep["sweep_number"])
                        if "Stimulus contains NaN values" in tags:
                            NaN_stim_values.append(sweep["sweep_number"])
                        if "test epoch is missing" in tags:
                            missing_test_epoch.append(sweep["sweep_number"])
                        if "stim epoch is missing" in tags:
                            missing_stim_epoch.append(sweep["sweep_number"])
                        if "experiment epoch is missing" in tags:
                            missing_exp_epoch.append(sweep["sweep_number"])
                        
                df.loc[cell_row, "Recording stopped before completing the experiment epoch"] = str(recording_stopped_sweeps)
                df.loc[cell_row, "Stimulus contains NaN values"] = str(NaN_stim_values)
                df.loc[cell_row, "test epoch is missing"] = str(missing_test_epoch)
                df.loc[cell_row, "stim epoch is missing"] = str(missing_stim_epoch)
                df.loc[cell_row, "experiment epoch is missing"] = str(missing_exp_epoch)
                
            elif fil.endswith('EPHYS_FEATURE_EXTRACTION_V3_QUEUE_' + roi_id + '_output.json'):
                
                try:
                    with open(directory + fil) as f:
                        features_data = json.load(f)
                except json.decoder.JSONDecodeError as e:
                    print(roi_id)
                
                if features_data:
                    cell_state = features_data['cell_state']
                    if cell_state['failed_fx'] == True:
                        cell_fx = "Failed"
                    elif cell_state['failed_fx'] == False:
                        cell_fx = "Passed"
                    df.loc[cell_row, 'failed_feature_ext'] = cell_fx   #str(cell_state['failed_fx'])
                    df.loc[cell_row, 'fail_fx_message'] = str(cell_state['fail_fx_message'])

                if 'feature_states' in features_data.keys():
                    feature_states = features_data['feature_states']

                    ls_state = feature_states['long_squares_state']
                    df.loc[cell_row, 'longsquares_failed_fx?'] = ls_state['failed_fx']
                    df.loc[cell_row, 'longsquares_fail_fx_message'] = ls_state['fail_fx_message']

                    ss_state = feature_states['short_squares_state']
                    df.loc[cell_row, 'shortsquares_failed_fx?'] = ss_state['failed_fx']
                    df.loc[cell_row, 'shortsquares_fail_fx_message'] = ss_state['fail_fx_message']                        

                    rmp_state = feature_states['ramps_state']
                    df.loc[cell_row, 'ramps_failed_fx?'] = rmp_state['failed_fx']
                    df.loc[cell_row, 'ramps_fail_fx_message'] = rmp_state['fail_fx_message']                         
                        
                    if 'cell_features' in features_data.keys():
                        cell_features = features_data['cell_features']
                        longsquares = cell_features['long_squares']
                        shortsquares = cell_features['short_squares']
                        ramps = cell_features['ramps']
                        
                        if longsquares is None:
                            ls_fx = "Failed"
                        else:
                            ls_fx = "Passed"
                        df.loc[cell_row, 'longsquares_fx'] = ls_fx
                    
                        if shortsquares is None:
                            ss_fx = "Failed"
                        else:
                            ss_fx = "Passed"
                        df.loc[cell_row, 'shortsquares_fx'] = ss_fx
                        
                        if ramps is None:
                            ramp_fx = "Failed"
                        else:
                            ramp_fx = "Passed"
                        df.loc[cell_row, 'ramps_fx'] = ramp_fx

col_order = ['recording_date','created_date', 'cell_name', 'cell_id', 'tube_id', 'rig_operator', 'roi_result_id', 'specimen_workflow_state', 'roiresult_workflow_state', 
             'project_code', 'storage_dir', 'electrode_0_pa', 'failed_electrode_0', 'seal_gohm', 'failed_no_seal', 
             'input_resistance_mohm', 'initial_access_resistance_mohm', 'input_access_resistance_ratio', 'failed_bad_rs',  
             'failed_other', 'stim_summary_cell_tags', 'Recording stopped before completing the experiment epoch', 
             'Stimulus contains NaN values', 'test epoch is missing', 'stim epoch is missing', 'experiment epoch is missing',
             'QC status', 'ephys_qc_fail_tags', 'electrode_0_pa missing value', 'Invalid seal (None)', 
             'Resistance ratio not available', 'Initial access resistance not available', 'No sweep states available', 
             'No current clamps sweeps passed QC', 'failed_feature_ext', 'fail_fx_message', 'longsquares_failed_fx?', 
             'longsquares_fail_fx_message', 'shortsquares_failed_fx?', 'shortsquares_fail_fx_message', 'ramps_failed_fx?', 
             'ramps_fail_fx_message', 'longsquares_fx', 'shortsquares_fx', 'ramps_fx', 'tau', 'upstroke_downstroke_ratio_short_square',
             'peak_v_short_square', 'upstroke_downstroke_ratio_ramp', 'threshold_v_ramp', 'sag', 'threshold_t_ramp', 'slow_trough_v_ramp', 'vrest',  
             'trough_t_ramp', 'trough_v_long_square', 'threshold_t_short_square', 'peak_t_ramp', 'fast_trough_v_ramp', 'trough_t_long_square', 
             'slow_trough_v_long_square', 'trough_t_short_square', 'slow_trough_t_long_square', 'threshold_v_long_square', 'fast_trough_t_long_square', 
             'ri', 'threshold_t_long_square', 'threshold_v_short_square', 'avg_isi', 'vm_for_sag', 'threshold_i_long_square', 'threshold_i_short_square', 
             'slow_trough_t_ramp', 'peak_v_ramp', 'fast_trough_v_short_square', 'fast_trough_t_short_square', 'fast_trough_t_ramp', 'threshold_i_ramp', 
             'slow_trough_v_short_square', 'peak_t_short_square', 'slow_trough_t_short_square', 'trough_v_short_square', 'f_i_curve_slope', 'peak_t_long_square',
             'latency', 'fast_trough_v_long_square', 'upstroke_downstroke_ratio_long_square', 'trough_v_ramp', 'peak_v_long_square', 'adaptation', 'has_delay', 
             'has_pause', 'has_burst']
df['created_date'] = df['created_date'].dt.strftime('%Y-%m-%d')
df = df[col_order]
df['storage_dir'] = df['storage_dir'].str.replace('/', "\\")
df['storage_dir'] = '\\' + df['storage_dir']

#print("saving lims and json data")
#df.to_csv(data_path + "lims_and_json_data.csv")

shiny_columns = ['cell_name', 'batch_vendor_name', 'patchseq_roi', 'Norm_Marker_Sum.0.4_label', 'rna_amplification_pass_fail']
shiny_mouse = pd.read_csv(shiny_visp_mouse_path + 'mapping.df.with.bp.40.lastmap.csv', usecols=shiny_columns)
shiny_human = pd.read_csv(shiny_mtg_human_path + 'mapping.df.lastmap.csv', usecols=shiny_columns)
shiny = pd.concat([shiny_mouse, shiny_human])

dash_data = pd.merge(left=df, right=shiny, how="left", left_on="cell_name", right_on="cell_name")

cell_list = list(dash_data["cell_name"])
core1_stims = ['X1PS_SubThresh', 'X3LP_Rheo', 'X4PS_SupraThresh', 'X6SP_Rheo', 'X7Ramp', 'X7RAMP']

sweep_qc_df = pd.DataFrame(columns=['cell_name'] + core1_stims)
sweep_qc_df['cell_name'] = cell_list

for cell_name in cell_list:
   # Add lims_df
    df = get_lims_sweep(cell_name)

    if len(df) > 0:

        for stim in core1_stims:

            stim_df = df[df['description'].str.startswith(stim)]
            stim_df.sort_values(by=['sweep_number'], inplace=True)

            if len(stim_df) > 0:
                max_idx = int(len(stim_df))
                wfs = stim_df.iloc[(max_idx-1), stim_df.columns.get_loc('workflow_state')]

                cell_loc = sweep_qc_df.loc[sweep_qc_df['cell_name'] == cell_name].index[0]
                sweep_qc_df.loc[cell_loc, stim] = wfs

full_dash_data = dash_data.merge(sweep_qc_df, left_on="cell_name", right_on="cell_name")
full_dash_data.to_csv(os.path.join(data_path, "electrophysiology_pipeline_metrics.csv"), index=False)

print("\nThe program was executed in", round(((time.time()-program_start_time)/60), 2), "minutes.")
