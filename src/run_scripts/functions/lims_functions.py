"""
---------------------------------------------------------------------
File name: lims_functions.py
Maintainer: Ramkumar Rajanbabu
---------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 04/02/2022
Description: LIMS related functions
---------------------------------------------------------------------
"""


#-----Imports-----#
# General imports
import json
import pandas as pd
import pg8000
# File imports
from functions.file_functions import load_data_variables
from functions.io_functions import is_this_py3


#-----Variables-----#
# Load json file
data_variables = load_data_variables()


#-----Functions-----#
def _connect(user="limsreader", host="limsdb2", database="lims2", password="limsro", port=5432):
    conn = pg8000.connect(user=user, host=host, database=database, password=password, port=port)
    return conn, conn.cursor()


def _select(cursor, query):
    cursor.execute(query)
    columns = [ d[0] for d in cursor.description ]
    return [ dict(zip(columns, c)) for c in cursor.fetchall() ]


def limsquery(query, user="limsreader", host="limsdb2", database="lims2", password="limsro", port=5432):
    """Connects to the LIMS database, executes provided query and returns a dictionary with results.    
    Parameters
    ----------
    query : string containing SQL query
    user : string
    host : string
    database : string
    password : string
    port : int
    
    Returns
    -------
    results : dictionary
    """
    conn, cursor = _connect(user, host, database, password, port)
    try:
        results = _select(cursor, query)
    finally:
        cursor.close()
        conn.close()
    return results


def rename_byte_cols(df):
    """A conversion tool for pg8000 byte output (for Python 3 only).
    
    Parameters
    ----------
        df (Pandas dataframe): LIMS query output with byte column names
    
    Returns
    -------
        Pandas dataframe : output with string column names
    
    """

    rename_dict = {c:str(c, "utf-8") for c in df.columns if isinstance(c, (bytes, bytearray))}
    df_renamed = df.rename(columns=rename_dict)
    return df_renamed


def get_lims_ephys():

    project_codes = ("hIVSCC-MET", "hIVSCC-METx", "hIVSCC-METc", "mIVSCC-MET", "mIVSCC-METx", "qIVSCC-METa", "qIVSCC-METc", "MET-NM", "mMPATCHx", "H301", "H301x")
    user_codes = ("PC", "PX", "P1", "P2", "P4", "P6", "P8", "P9", "PA", "PB", "PE", "PF", "PI", "PJ", "PN", "PR", "PV")

    lims_query = """
    SELECT cell.name AS cell_name, 
    cell.id AS cell_id, 
    cell.patched_cell_container AS tube_id,
    substring(cell.patched_cell_container, 0, 3) as rig_operator,
    cell.workflow_state AS specimen_workflow_state,
    p.code AS project_code,
    err.id AS roi_result_id,
    err.storage_directory AS storage_dir,
    err.created_at AS created_date,
    err.recording_date AS recording_date,
    err.workflow_state AS roiresult_workflow_state,
    err.electrode_0_pa,
    err.failed_electrode_0,
    err.input_resistance_mohm,
    err.initial_access_resistance_mohm,
    err.input_access_resistance_ratio,
    err.seal_gohm,
    err.failed_no_seal,
    err.failed_bad_rs,
    err.failed_other,
    slice.name as slice_name,
    eff.tau,
    eff.upstroke_downstroke_ratio_short_square,
    eff.peak_v_short_square,
    eff.upstroke_downstroke_ratio_ramp,
    eff.threshold_v_ramp,
    eff.sag,
    eff.threshold_t_ramp,
    eff.slow_trough_v_ramp,
    eff.vrest, 
    eff.trough_t_ramp,
    eff.trough_v_long_square,
    eff.threshold_t_short_square,
    eff.peak_t_ramp,
    eff.fast_trough_v_ramp,
    eff.trough_t_long_square,
    eff.slow_trough_v_long_square,
    eff.trough_t_short_square,
    eff.slow_trough_t_long_square,
    eff.threshold_v_long_square,
    eff.fast_trough_t_long_square,
    eff.ri,
    eff.threshold_t_long_square,
    eff.threshold_v_short_square,
    eff.avg_isi,
    eff.vm_for_sag,
    eff.threshold_i_long_square,
    eff.threshold_i_short_square,
    eff.slow_trough_t_ramp,
    eff.peak_v_ramp,
    eff.fast_trough_v_short_square,
    eff.fast_trough_t_short_square,
    eff.fast_trough_t_ramp,
    eff.threshold_i_ramp,
    eff.slow_trough_v_short_square,
    eff.peak_t_short_square,
    eff.slow_trough_t_short_square,
    eff.trough_v_short_square,
    eff.f_i_curve_slope,
    eff.peak_t_long_square,
    eff.latency,
    eff.fast_trough_v_long_square,
    eff.upstroke_downstroke_ratio_long_square,
    eff.trough_v_ramp,
    eff.peak_v_long_square,
    eff.adaptation,
    eff.has_delay,
    eff.has_pause,
    eff.has_burst
    FROM specimens cell
    LEFT JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
    LEFT JOIN projects p ON cell.project_id = p.id
    LEFT JOIN specimens slice ON cell.parent_id = slice.id
    LEFT JOIN ephys_features eff ON cell.id = eff.specimen_id
    WHERE p.code IN {}
    AND substring(cell.patched_cell_container, 0, 3) IN {}
    AND cell.patched_cell_container IS NOT null
    ORDER BY err.created_at
    """ .format(project_codes, user_codes)
    # AND err.storage_directory IS NOT null

    df = pd.DataFrame(limsquery(lims_query))
    if is_this_py3:
        df = rename_byte_cols(df)
    return df


def get_lims_sweep(cell_name):
    sweep_qc_query = """
    SELECT sw.specimen_id, stim.description, sw.workflow_state, sw.sweep_number, stype.name, specimens.name AS cell_name

    FROM ephys_sweeps sw

    JOIN ephys_stimuli stim ON stim.id = sw.ephys_stimulus_id
    JOIN specimens ON specimens.id = sw.specimen_id
    JOIN ephys_stimulus_types stype ON stype.id = stim.ephys_stimulus_type_id

    WHERE specimens.name LIKE '{}'
    """.format(cell_name)
        
    df = pd.DataFrame(limsquery(sweep_qc_query))
    if is_this_py3:
        df = rename_byte_cols(df)
    return df


def get_lims():
    lims_query="""
    SELECT DISTINCT
    cell.name,
    cell.patched_cell_container,
    cell.cell_depth,
    slice.histology_well_name,
    d.external_donor_name AS id_cell_specimen_id,
    d.full_genotype AS id_slice_genotype,
    d.name AS donor_name, 
    org.name AS id_species,
    proj.code AS id_project_code,
    structures.acronym AS structure,
    cell_reporters.name AS cell_reporter
    FROM specimens cell
    INNER JOIN specimens slice ON cell.parent_id = slice.id 
    INNER JOIN donors d ON d.id = cell.donor_id
    LEFT JOIN organisms org ON d.organism_id = org.id
    LEFT JOIN projects proj ON cell.project_id = proj.id
    LEFT JOIN structures ON cell.structure_id = structures.id
    LEFT JOIN cell_reporters ON cell.cell_reporter_id = cell_reporters.id
    WHERE SUBSTRING(cell.patched_cell_container FROM 6 FOR 6) BETWEEN '171001' AND '301231'"""

    df = pd.DataFrame(limsquery(lims_query))
    if is_this_py3:
        df = rename_byte_cols(df)
    return df


def generate_lims_df(group, date):
    """
    Generates a lims dataframe based on the specified date.

    Parameters:
        group (string): "ivscc" to exclude the hct list or "hct" to only include the hct list.
        date (string): a date in the format of YYMMDD.

    Returns:
        lims_df (dataframe): a pandas dataframe.
    """

    # Lists
    hct_jt = [str(x) for x in range(101, 151, 1)]          # Jonathan(101-150)
    hct_cr = [str(x) for x in range(225, 251, 1)]          # Cristina(225-250)
    hct_bk_mk = [str(x) for x in range(301, 351, 1)]       # Brian K(301-350), Meanhwan(325-350)
    #hct_ln = [str(x) for x in range(351, 401, 1)]          # Lindsay(351-400)
    hct_ss = [str(x) for x in range(751, 801, 1)]          # Scott(751-800)
    hct_sv = [str(x) for x in range(801, 851, 1)]          # Sara(801-850)
    hct_user_tube_num_list = hct_jt + hct_cr + hct_bk_mk + hct_ss + hct_sv

    lims_df = get_lims()
    # Rename columns based on jem_dictionary
    lims_df.rename(columns=data_variables["lims_dictionary"], inplace=True)
    # Exclude Collaborator containers
    lims_df = lims_df[(~lims_df["lims-id_patched_cell_container"].str.startswith("PGS4")) & (~lims_df["lims-id_patched_cell_container"].str.startswith("PHS4"))]
    # Filters dataframe to user specified date
    lims_df = lims_df[lims_df["lims-id_patched_cell_container"].str.contains(date)]
    # Only run if patched cell containers were collected
    if len(lims_df) > 0:
        if group == "ivscc":
            # Exclude HCT containers (Ex. column output: 301)
            lims_df["lims-exclude_container"] = lims_df["lims-id_patched_cell_container"].str.slice(-7, -4)
            lims_df = lims_df[~lims_df["lims-exclude_container"].str.contains("|".join(hct_user_tube_num_list))]
        if group == "hct":
            # Include only HCT containers (Ex. column output: 301)
            lims_df["lims-include_container"] = lims_df["lims-id_patched_cell_container"].str.slice(-7, -4)
            lims_df = lims_df[lims_df["lims-include_container"].str.contains("|".join(hct_user_tube_num_list))]
        if len(lims_df) > 0:
            # Replace values
            lims_df["lims-id_species"].replace({"Homo Sapiens": "Human", "Mus musculus": "Mouse"}, inplace=True)
            lims_df["lims-id_slice_genotype"].replace({None: ""}, inplace=True)
            # Apply specimen id
            lims_df["lims-id_cell_specimen_id"] = lims_df.apply(get_specimen_id, axis=1)
            # Sort by patched_cell_container in ascending order
            lims_df.sort_values(by="lims-id_patched_cell_container", inplace=True)

    return lims_df


def generate_external_lims_df():
    """
    Generates a lims dataframe based on the specified date.

    Parameters:
        group (string): "ivscc" to exclude the hct list or "hct" to only include the hct list.
        date (string): a date in the format of YYMMDD.

    Returns:
        lims_df (dataframe): a pandas dataframe.
    """

    lims_df = get_lims_query_external()

    return lims_df


def get_specimen_id(row):
    species = row["lims-id_species"]
    specimen_id = row["lims-id_cell_specimen_id"]
    donor_name = row["lims-id_slice_specimen"]
    if species == "Mouse":
        return specimen_id
    else:
        return donor_name


#-----SQL Queries-----#
def create_ivscc_transcriptomics_query_to_df():
    """
    Uses same SQL query from ivscc_transcriptomics_query.sql file.
    """

    sql_query = """
    WITH Ivscc AS (
        SELECT
        C.name AS lims_cell_name,
        C.patched_cell_container AS lims_patch_tube,
        SUBSTRING(C.patched_cell_container, 1, 4) AS lims_patch_tube_id,
        CAST(SUBSTRING(C.patched_cell_container, 6, 6) AS DATE) AS lims_patch_tube_date,
        SUBSTRING(C.patched_cell_container, 13, 3) AS lims_patch_tube_number,
        Sp.histology_well_name AS lims_histology_well_name,
        P.code AS lims_project_code,
        O.name AS lims_species_type,
        S.acronym AS lims_structure,
        CR.name AS lims_cell_reporter,
        C.cell_depth AS lims_cell_depth,
        D.name AS lims_donor_name,
        D.external_donor_name AS lims_specimen_id,
        D.full_genotype AS lims_slice_genotype
        FROM specimens C
        INNER JOIN specimens Sp
            ON C.parent_id = Sp.id 
        INNER JOIN donors D 
            ON D.id = C.donor_id
        LEFT JOIN organisms O
            ON D.organism_id = O.id
        LEFT JOIN projects P
            ON C.project_id = P.id
        LEFT JOIN structures S
            ON C.structure_id = S.id
        LEFT JOIN cell_reporters CR 
            ON C.cell_reporter_id = CR.id
        WHERE SUBSTRING(C.patched_cell_container, 1, 4) 
            IN (
                'PCS4',
                'PXS4',
                'P1S4',
                'P2S4',
                'P4S4',
                'P5S4',
                'P6S4',
                'P8S4',
                'PAS4',
                'PBS4',
                'PFS4',
                'PNS4',
                'PVS4'
            )
        AND SUBSTRING(C.patched_cell_container FROM 6 FOR 6) >= '171001'
    )
    SELECT
        *
    FROM Ivscc
    WHERE SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '001' AND '050'
    OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '051' AND '100'
    OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '151' AND '200'
    OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '201' AND '250'
    OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '251' AND '300'
    OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '351' AND '400'
    OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '401' AND '450'
    OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '501' AND '550'
    OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '701' AND '750'
    OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '851' AND '900'
    ORDER BY lims_patch_tube_date DESC, lims_patch_tube_id ASC, lims_patch_tube_number ASC
    """

    df = pd.DataFrame(limsquery(sql_query))
    if is_this_py3:
        df = rename_byte_cols(df)

    return df


def create_hct_transcriptomics_query_to_df():
    """
    Uses same SQL query from hct_transcriptomics_query.sql file.
    """

    sql_query = """
    WITH Hct AS (
        SELECT
        C.name AS lims_cell_name,
        C.patched_cell_container AS lims_patch_tube,
        SUBSTRING(C.patched_cell_container, 1, 4) AS lims_patch_tube_id,
        CAST(SUBSTRING(C.patched_cell_container, 6, 6) AS DATE) AS lims_patch_tube_date,
        SUBSTRING(C.patched_cell_container, 13, 3) AS lims_patch_tube_number,
        Sp.histology_well_name AS lims_histology_well_name,
        P.code AS lims_project_code,
        O.name AS lims_species_type,
        S.acronym AS lims_structure,
        CR.name AS lims_cell_reporter,
        C.cell_depth AS lims_cell_depth,
        D.name AS lims_donor_name,
        D.external_donor_name AS lims_specimen_id,
        D.full_genotype AS lims_slice_genotype
        FROM specimens C
        INNER JOIN specimens Sp
            ON C.parent_id = Sp.id 
        INNER JOIN donors D 
            ON D.id = C.donor_id
        LEFT JOIN organisms O
            ON D.organism_id = O.id
        LEFT JOIN projects P
            ON C.project_id = P.id
        LEFT JOIN structures S
            ON C.structure_id = S.id
        LEFT JOIN cell_reporters CR 
            ON C.cell_reporter_id = CR.id
        WHERE SUBSTRING(C.patched_cell_container, 1, 4) 
            IN (
                'PCS4',
                'PXS4',
                'P3S4',
                'P7S4',
                'PJS4',
                'PKS4',
                'PLS4',
                'PSS4'
            )
        AND SUBSTRING(C.patched_cell_container FROM 6 FOR 6) >= '171001'
    )
    SELECT
        *
    FROM Hct
    WHERE SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '101' AND '150'
    OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '301' AND '350'
    OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '751' AND '800'
    ORDER BY lims_patch_tube_date DESC, lims_patch_tube_id ASC, lims_patch_tube_number ASC
    """

    df = pd.DataFrame(limsquery(sql_query))
    if is_this_py3:
        df = rename_byte_cols(df)

    return df


def create_collab_transcriptomics_query_to_df():
    """
    Uses same SQL query from collaborator_transcriptomics_query.sql file.
    """

    sql_query = """
    WITH Collaborators AS (
        SELECT DISTINCT
        C.name AS lims_cell_name,
        C.patched_cell_container AS lims_patch_tube,
        SUBSTRING(C.patched_cell_container, 1, 4) AS lims_patch_tube_id,
        CAST(SUBSTRING(C.patched_cell_container, 6, 6) AS DATE) AS lims_patch_tube_date,
        SUBSTRING(C.patched_cell_container, 13, 3) AS lims_patch_tube_number,
        C.cell_depth AS lims_cell_depth,
        D.external_donor_name AS lims_specimen_id,
        D.full_genotype AS lims_slice_genotype,
        D.name AS lims_donor_name, 
        O.name AS lims_species_type,
        P.code AS lims_project_code, 
        S.acronym AS lims_structure
        FROM specimens C
        INNER JOIN specimens Sp
            ON C.parent_id = Sp.id 
        INNER JOIN donors D 
            ON D.id = C.donor_id
        LEFT JOIN organisms O
            ON D.organism_id = O.id
        LEFT JOIN projects P
            ON C.project_id = P.id
        LEFT JOIN structures S
            ON C.structure_id = S.id
        WHERE SUBSTRING(C.patched_cell_container, 1, 4) 
            IN (
                'PDS4',
                'PGS4',
                'PHS4',
                'PRS4',
                'PWS4'
            )
        AND SUBSTRING(C.patched_cell_container FROM 6 FOR 6) >= '171001'
    )
    SELECT
        *
    FROM Collaborators
    WHERE SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '001' AND '051'
    OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '101' AND '150'
    OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '201' AND '250'
    OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '501' AND '550'
    OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '601' AND '650'
    OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '701' AND '750'
    OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '801' AND '850'
    OR SUBSTRING(lims_patch_tube, 13, 3) BETWEEN '901' AND '950'
    ORDER BY lims_patch_tube_date DESC, lims_patch_tube_id ASC, lims_patch_tube_number ASC
    """

    df = pd.DataFrame(limsquery(sql_query))
    if is_this_py3:
        df = rename_byte_cols(df)

    return df