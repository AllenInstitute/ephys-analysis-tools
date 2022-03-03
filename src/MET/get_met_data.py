
import os
import numpy as np
import pandas as pd
from lims_funcs import limsquery
from met_funcs import assign_postpatch, assign_morpho_quality, assign_postpatch_v2, label_ei_mouse, label_ei_human


# report_dir = 'C:\\Users\\agatab\\Documents\\analysis-projects\\ps-reports'
report_dir = 'C:\\Users\\rustym\\Documents\\GitHub\\ephys_metadata_tools'
metadata_dir = '//allen/programs/celltypes/workgroups/279/Patch-Seq/compiled-jem-data/raw_data/'
# met_dir = 'C:\\Users\\agatab\\Documents\\analysis-projects\\met\\data'
met_dir = 'C:\\Users\\rustym\\Documents\\Agatas_MET_report\\MET\\data'


def exc_inh_divider(sample_row):
    label = sample_row["cluster_label"]
    
    terms_mouse_inh = ["Vip", "Sst", "Pvalb", "Lamp5", "Sncg", "Inhibitory"]
    terms_mouse_exc = ["L2/3", "L4", "L5", "L6 ", "L6a", "L6b", "IT", "PT", "CT","Car3", "Cdh13", "Excitatory"] # "VISp"]
    terms_mouse_neither = ["Meis2", "n3", "n4", "n5"]

    nodes_human_inh = ["n" + str(i) for i in range(3,47,1)]
    nodes_human_exc = ["n" + str(i) for i in range(47,63,1)]
    
    if isinstance(label, float):
        return None
    else:
        if sample_row["organism"]=="Homo Sapiens":
            return label_ei_human(label, nodes_human_inh, nodes_human_exc)
        elif sample_row["organism"]=="Mus musculus":                      
            return label_ei_mouse(label, terms_mouse_inh, terms_mouse_exc, terms_mouse_neither)
        else:
            return None

def get_cluster_family(cluster_label): 
    from itertools import compress
    family_labels = ['L2/3 IT', 'L4 IT', 'L5 IT', 'L5 PT', 'L6 CT', 'L6 IT', 'L6b', 'Lamp5', 'Lamp5 Lhx6', 'Meis2', 'Pvalb', 'Sncg', 'Sst', 'Sst Chodl', 'Vip']
    filt = [family_label in cluster_label for family_label in family_labels]
    families = list(compress(family_labels, filt))
    if len(families) > 0:
        return families[0]
    else:
        return None
    

# -*- coding: utf-8 -*-
"""
Created on Wed Jul 12 12:43:00 2017

@author: agatab
"""

import os
import numpy as np
import pandas as pd
from datetime import datetime
from dateutil import parser

from lims_funcs import limsquery
from file_funcs import validated_input, validated_date_input, get_jsons
from jem_data_set import JemDataSet

"""-------------------------FUNCTION DEFINITIONS--------------------------------"""

def get_lims_metadata(report_datestr):
    """Return LIMS specimens with patch cell container matching a specific report_date,
    or between two report dates.
    
    Parameters
    ----------
    report_datestr : list of length 1 or 2
        A list of dates in the format YYMMDD. (Either one date, or start and end date)
    
    Returns
    -------
    pandas dataframe
        A dataframe with LIMS specimen metadata.l
    """
    if len(report_datestr) == 1:
        datestr = "%%" + report_datestr[0] + "%%"
    else:
        # Build SQL regex for the entire date range
        start_day, end_day = [datetime.strptime(x, "%y%m%d").date() for x in report_datestr]
        report_date_range = pd.date_range(start=start_day, end=end_day)
        datestr = "|".join(["%%" + x.strftime("%y%m%d") +"%%" for x in report_date_range])

    lims_query_str="""SELECT DISTINCT cell.name, 
    cell.patched_cell_container AS lims_patch_container, 
    cell.cell_depth as cell_depth,
    d.external_donor_name AS labtracksID, 
    d.name AS donor_name, 
    d.full_genotype, 
    org.name AS organism_name, 
    err.*, 
    err.recording_date AS lims_date 
    FROM specimens cell 
    JOIN specimens slice ON cell.parent_id = slice.id 
    LEFT JOIN donors d ON d.id = cell.donor_id 
    JOIN organisms org ON d.organism_id = org.id 
    LEFT JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
    WHERE cell.patched_cell_container IS NOT NULL 
    AND cell.patched_cell_container SIMILAR TO '(%s)'""" %datestr
    
    return pd.DataFrame(limsquery(lims_query_str))


def get_specimen_id(row):
    """Return Specimen ID (LabTracksID or Human Case ID).
    
    Parameters
    ----------
    row : a pandas dataframe row containing a Mouse or Human specimen
    
    Returns
    -------
    string
        Specimen ID for the Mouse or Human specimen
    """
    
    organism = row["organism_name"]
    labtracks = row["labtracksid"]
    donorname = row["donor_name"]
    if organism == "Mus musculus":
        return labtracks
    else:
        return donorname
    
 
def extract_essential_limsdata(df, l_cols):
    """Extract essential metadata (specimen ID, species...)
    
    Parameters
    ----------
    df : pandas dataframe
        A dataframe of LIMS specimen metadata.
    l_cols : list
        A list of essential columns to return
    
    Returns
    -------
    df2 : pandas dataframe
        A minimal dataframe with essential LIMS metadata.
    """

    df2 = df.copy()
    df2.loc[:,"specimen_ID"] = df.apply(get_specimen_id, axis=1)
    df2["organism_name"].replace({"Homo Sapiens":"Human", "Mus musculus":"Mouse"}, inplace=True)
    df2.drop_duplicates(subset=["lims_patch_container"], inplace=True)
    return df2[l_cols]


def get_metadata(start_day=None):
    """---------------------------CONSTANTS-----------------------------------"""
    default_json_dir = "//allen/programs/celltypes/workgroups/279/Patch-Seq/all-metadata-files"
    constants_dir = os.path.join(report_dir, "constants")# "jem-constants")

    if start_day is None:
        start_day = datetime(2017, 10, 1).date()
    end_day = datetime.today().date()
    start_day_str, end_day_str = [x.strftime("%y%m%d") for x in [start_day, end_day]]

    """--------------Import PatchSeq user and roi info from csv files-----------------"""
    roi_df = pd.read_csv(os.path.join(constants_dir, "roi_info.csv"))
    roi_df.set_index(keys="name", drop=True, inplace=True)

    """Get JEM pathnames that have been created since the report start date (with a 3 day buffer)"""
    delta_mod_date = (datetime.today().date() - start_day).days + 3
    jem_paths = get_jsons(dirname=default_json_dir, expt="PS", delta_days=delta_mod_date)

    """Flatten data in recent JSON files and output successful experiments (with tube IDs) in a dataframe. """
    jem_df = flatten_jem_data(jem_paths, start_day, end_day)
    success_df = jem_df[jem_df["status"].str.contains("SUCCESS")]

    """Run a congruency check on JSON patch containers by querying LIMS."""
    merged_df = check_with_lims(start_day_str, end_day_str, jem_df=success_df)

    # Wrangle some unclean bits and pieces.
    final_df = clean_metadata_fields(merged_df)

    return final_df

def clean_metadata_fields(df, roi_df):
    """Cleans up some inconsistent fields in JEM data.
    
    Parameters
    ----------
    df : pandas dataframe
    roi_df : pandas dataframe

    Returns
    -------
    df : pandas dataframe
    """

    """Clean up ROIs"""
    df["roi"].replace({"VISp, layer 1":"VISp1","VISp, layer 2/3":"VISp2/3", 
                    "VISp, layer 4":"VISp4", "VISp, layer 5":"VISp5",
                    "VISp, layer 6a":"VISp6a", "VISp, layer 6b":"VISp6b",
                    "FCx, layer 1":"FCx1", "FCx, layer 2":"FCx2", "FCx, layer 3":"FCx3",
                    "FCx, layer 4":"FCx4", "FCx, layer 5":"FCx5"}, inplace=True)

    df.loc[:,"roi_major"] = df["roi"].apply(lambda x: roi_df.loc[x]["roi_major"] if x in roi_df.index else None)
    df.loc[:,"roi_minor"] = df["roi"].apply(lambda x: roi_df.loc[x]["roi_minor"] if x in roi_df.index else None)

    """Clean up post patch naming convention."""
    df["extraction.postPatch"].replace({
        "Nucleated":"nucleus_present", "Partial-Nucleus":"nucleus_present", 
        "No-Seal": "nucleus_absent", "Outside-Out":"nucleus_absent"}, inplace=True)

    """Parse JEM date."""
    df.loc[:,"jem_date_dt"] = df["date"].apply(lambda x: str(parser.parse(x).date()))
    df.loc[:,"jem_date_m"] = df["jem_date_dt"].apply(lambda x: "-".join([x.split("-")[0], x.split("-")[1]]))

    return df



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

    import pandas as pd
    from datetime import datetime
    from jem_data_set import JemDataSet
    
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


def check_with_lims(start_day_str, end_day_str, jem_df):
    """Returns metadata dataframe merged with basic LIMS data.
    
    Parameters
    ----------
    start_day_str : string in format 'YYMMDD'

    end_day_str : string in format 'YYMMDD'

    json_df : pandas dataframe
        Compiled JEM data

    Returns
    -------
    final_df : pandas dataframe
    """

    """Run a congruency check on JSON patch containers by querying LIMS."""
    l_cols = ["organism_name", "name", "lims_patch_container", "full_genotype", "specimen_ID", "cell_depth"]
    lims_df = get_lims_metadata(report_datestr=[start_day_str, end_day_str])
    if len(lims_df) > 0:
        final_lims_df = extract_essential_limsdata(lims_df, l_cols)
    else:
        print("No LIMS patch containers found.")
        final_lims_df = pd.DataFrame(columns=l_cols)

    """Merge JSON and LIMS metadata. 
    Make a patch container column that contains either lims or json patch container"""
    jem_df.rename(columns={"container":"jem_patch_container"}, inplace=True)
    merged_df = final_lims_df.merge(jem_df, how="outer", 
        left_on = "lims_patch_container", right_on="jem_patch_container", indicator=True)
    # merged_df = final_lims_df.merge(jem_df, how="outer", 
    # left_on = "name", right_on="pipetteSpecName", indicator=True)
    merged_df['_merge'] = merged_df['_merge'].map({
        'both':'lims_and_json', 'left_only':'lims_only', 'right_only':'json_only'})
    merged_df.rename(columns={'_merge':'lims_check'}, inplace=True)
    merged_df["container"] = merged_df["lims_patch_container"]
    merged_df["container"].fillna(merged_df["jem_patch_container"], inplace=True)
    merged_df.drop(columns=["lims_patch_container", "jem_patch_container"], inplace=True)

    comparison_column = np.where(merged_df["name"] == merged_df["pipetteSpecName"], True, False)
    merged_df["matching_LIMS_JEM_cells?"] = comparison_column
    # merged_df.loc[merged_df["matching_LIMS_JEM_cells?"] == False, 'lims_check'] = 'Mismatched_cells'
    # merged_df.drop(columns=["matching_LIMS_JEM_cells?"], inplace=True)
    merged_df['true_mismatch?'] = np.where(((merged_df['matching_LIMS_JEM_cells?'] == False) & (merged_df['lims_check'] == 'lims_and_json') & (~merged_df['pipetteSpecName'].isnull())), True, False)
    merged_df.loc[merged_df["true_mismatch?"] == True, 'lims_check'] = 'Mismatched_cells'
    merged_df.drop(columns=["matching_LIMS_JEM_cells?", "true_mismatch?"], inplace=True)

    return merged_df
  


def make_metadata_csv(default_json_dir=None, start_day_str="171001", fn="jem_metadata"):
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
    constants_dir = os.path.join(report_dir, "constants")
    if default_json_dir == None:
        # default_json_dir  = "//allen/programs/celltypes/workgroups/279/Patch-Seq/all-metadata-files"
        default_json_dir  = "//allen/programs/celltypes/workgroups/279/Patch-Seq/all-metadata-files"
        # output_dir =  os.path.join(met_dir, "core")
        output_dir = "//allen/programs/celltypes/workgroups/279/Patch-Seq/compiled-jem-data/raw_data"
    # else:
    #     output_dir = default_json_dir    


    start_day = datetime.strptime(start_day_str, "%y%m%d").date()
    end_day = datetime.today().date()
    end_day_str = end_day.strftime("%y%m%d")

    """--------------Import PatchSeq user and roi info from csv files-----------------"""
    roi_df = pd.read_csv(os.path.join(constants_dir, "roi_info.csv"))
    roi_df.set_index(keys="name", drop=True, inplace=True)

    """Get JEM pathnames that have been created since the report start date (with a 3 day buffer)"""
    delta_mod_date = (datetime.today().date() - start_day).days + 3
    jem_paths = get_jsons(dirname=default_json_dir, expt="PS", delta_days=delta_mod_date)

    """Flatten data in recent JSON files and output successful experiments (with tube IDs) in a dataframe. """
    jem_df = flatten_jem_data(jem_paths, start_day_str, end_day_str)
    print(jem_df['status'].unique())
    problem_row = jem_df[jem_df['status'].isnull()]
    print(problem_row['limsSpecName'])
    clean_jem_df = clean_metadata_fields(jem_df, roi_df)
    clean_jem_df.sort_values(by="date").to_csv(os.path.join(output_dir, "%s_wFAILURE.csv" %fn), encoding='utf-8-sig', index=False, date_format="%Y-%m-%d")
    success_df = jem_df[jem_df["status"].str.contains("SUCCESS")]

    # Wrangle some unclean bits and pieces.
    # clean_success_df = clean_metadata_fields(success_df, roi_df)

    """Separate tubes from NA specimens (no sample was sent for amplification)."""
    # clean_success_df["container"].fillna(value="NA", inplace=True)
    # tube_df = clean_success_df[~(clean_success_df["container"].isin(["NA", "na", "N/A", "n/a"]))]
    # na_df = clean_success_df[clean_success_df["container"].isin(["NA", "na", "N/A", "n/a"])]
    success_df["container"].fillna(value="NA", inplace=True)
    tube_df = success_df[~(success_df["container"].isin(["NA", "na", "N/A", "n/a"]))]
    na_df = success_df[success_df["container"].isin(["NA", "na", "N/A", "n/a"])]

    """Run a congruency check on JSON patch containers by querying LIMS."""
    final_tube_df = check_with_lims(start_day_str, end_day_str, jem_df=tube_df)

    for data, data_fn in zip([final_tube_df, na_df], [fn, "NA_jem_metadata"]):
        if len(data) > 0:
            try:
                data.sort_values(by="date").to_csv(os.path.join(output_dir, "%s.csv" %data_fn), encoding='utf-8-sig', index=False, date_format="%Y-%m-%d")
            except IOError:
                print("\nOh no! Unable to save spreadsheet :(\nMake sure you don't already have a file with the same name opened.")

    return final_tube_df, na_df


def get_shiny_data(shiny_path_list):
    """Grab human and mouse spreadsheets with Shiny data.
    Parameters
    ----------
    shiny_path_list : list of strings
        A list of two Shiny .csv files, one for mouse and one for human results.

    Returns
    -------
    pandas DataFrame with shiny data
    """
    shiny_df = pd.DataFrame()
    for s in shiny_path_list:
        temp = pd.read_csv(s)
        shiny_df = pd.concat([shiny_df, temp], sort=True)

    shiny_columns = [c for c in shiny_df.columns if "cluster" in c and "label" in c]
    shiny_columns += ["sample_id", "res_index", "marker_genes", "Genes.Detected.CPM", 
    "rna_amplification_pass_fail", "Norm_Marker_Sum.0.4_label", "marker_sum_norm_label"]
    
    return shiny_df[shiny_columns]



def collect_data(query, shiny_path_list, start_day_str="171001", pull_json=False):
    """Takes a LIMS query, location of Shiny files, and JSON metadata, and outputs a merged dataframe.
    
    Parameters
    ----------
    query : string
        SQL query to pass to LIMS.
        List of required fields:
            "cell_name"
            "container"
			"image_series_63x_id"
			"dendrite_type"
			"go"
			"image_series_63x_qc"
			"swc_filename"
			"amplified_quantity_ng"

        List of recommended fields:
			TO_CHAR(err.recording_date,'YYYY-MM-DD') AS date
			WHERE date > 2017-10-02

    shiny_path_list : list of strings
        A list of two Shiny .csv files, one for mouse and one for human results.

    start_day_str : string (default '171001', start of IVSCC-MET pipeline)

    pull_json : boolean
        Indicates whether to freshly compile JSON metadata (takes a couple of minutes), 
        or refer to previously saved .csv file. 

    Returns
    -------
    final_data : pandas dataframe
    na_df : pandas dataframe

    """
    # json_dir =  os.path.join(report_dir, "reports")
    # default_json = os.path.join(json_dir, "json_metadata.csv")
    default_json = os.path.join(metadata_dir, "json_metadata.csv")

    # Load in LIMS data
    #import pg8000 as pg 
    #try:
    #	lims_df = pd.DataFrame(limsquery(query))
    #except pg.ProgrammingError:
    #	print("Error in SQL query")
    print("Querying LIMS")
    lims_df = pd.DataFrame(limsquery(query))
    
    # Load in JSON data
    if pull_json:
        print("Compiling metadata from JSON files.")
        jem_df, na_df = make_metadata_csv(start_day_str=start_day_str)
    else:
        try:
            print("Grabbing pre-compiled JSON data.")
            json_df = pd.read_csv(default_json)
        except IOError:
            print("Compiling metadata from JSON files.")
            json_df = get_metadata()

    # Load in SHINY data
    print("Grabbing spreadsheets with Shiny data.")
    shiny_df = get_shiny_data(shiny_path_list)
    merged_data = lims_df.merge(jem_df, on="container", how="left")
    final_data = merged_data.merge(shiny_df, left_on="container", right_on="sample_id", how="left")

    return final_data, na_df



def process_data(data, cre_d, json_cols=None):

    """Take compiled MET data and calculate some secondary things."""
    data.loc[:,"container_date"] = data["container"].apply(lambda x: x[5:11])
    data.loc[:,"container_m"] = data["container_date"].apply(lambda x: "20" + x[0:2] + "." + x[2:4])

    """Assign morphology quality."""
    data.loc[:,"morpho_quality"] = data.apply(lambda x: assign_morpho_quality(x["image_series_63x_id"], 
                                         x["dendrite_type"],
                                         x["go"],
                                         x["image_series_63x_qc"],
                                         x["swc_filename"]), axis=1)
    """Remove 16 incorrect amplification values"""
    data = data[(data["amplified_quantity_ng"]<50)|(data["amplified_quantity_ng"].isnull())]

    """Assign triple modality states."""
    data.loc[:,"seal_gate"] = data["seal"].apply(lambda x: "Pass" if x >= 1.0 else ("Fail" if x < 1.0 else "NA"))
    data.loc[:,"seal_cell_gate"] = data["cell_qc"].apply(lambda x: "Pass" if x == "good" else ("Fail" if x == "bad" else "NA"))
    data.loc[:,"seal_amp_gate"] = data.apply(lambda x: "Pass" if x["amp_failed"] is False 
                                    else ("Fail" if x["amp_failed"] is True 
                                          else None), axis=1)
    data.loc[:,"seal_amp_ri_gate"] = data.apply(lambda x: "Pass" if x["res_index"] >= 0.75
                                    else ("Fail" if x["res_index"] < 0.75 
                                          else None), axis=1)
    data.loc[:,"seal_amp_ri_63x_gate"] = data.apply(lambda x: "Pass" if x["go"] == "63x go"
                                    else ("Fail" if x["go"] == "63x no go"
                                          else None), axis=1)

    """Divide into excitatory and inhibitory types"""
    data.loc[:,"type"] = data.apply(lambda x: exc_inh_divider(x), axis=1)

    """Add cre line and cluster families for each sample where relevant"""
    data.loc[:,"cre_family"] = data["drivers"].apply(lambda x: cre_d[x] if x is not None else None)
    data.loc[:,"cluster_family"] = data["cluster_label"].apply(lambda x: get_cluster_family(x) if isinstance(x,str) else x)

    """Populate drivers field for human samples."""
    data.loc[data.organism == "Homo Sapiens", "drivers"] = "Human"

    return data