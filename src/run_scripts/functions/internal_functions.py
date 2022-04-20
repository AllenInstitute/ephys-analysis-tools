# -*- coding: utf-8 -*-
"""
Created on Wed Sep 18 10:11:00 2019

@author: agatab
"""
import numpy as np
import pandas as pd
import os
from datetime import datetime
from functions.io_functions import is_this_py3

def _connect(user="limsreader", host="limsdb2", database="lims2", password="limsro", port=5432):
    import pg8000
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

def get_lims_metadata(report_date):
    """Return LIMS specimens with patch cell container matching a given date.
    
    Parameters
    ----------
    last_bday_str : string
        A date in the format YYMMDD.
    
    Returns
    -------
    pandas dataframe
        A dataframe with LIMS specimen metadata.l
    """
    
    report_date_query_str = "%%" + report_date + "%%"
    lims_query_str="""SELECT DISTINCT cell.name, 
    cell.patched_cell_container AS lims_patch_container, 
    d.external_donor_name AS labtracksID, 
    d.name AS donor_name, 
    d.full_genotype, 
    org.name AS organism_name, 
    err.recording_date AS lims_date 
    FROM specimens cell 
    JOIN specimens slice ON cell.parent_id = slice.id 
    LEFT JOIN donors d ON d.id = cell.donor_id 
    JOIN organisms org ON d.organism_id = org.id 
    LEFT JOIN ephys_roi_results err ON cell.ephys_roi_result_id = err.id 
    WHERE cell.patched_cell_container IS NOT NULL 
    AND cell.patched_cell_container LIKE '%s'""" %report_date_query_str
    
    df = pd.DataFrame(limsquery(lims_query_str))
    if is_this_py3:
        df = rename_byte_cols(df)
    return df

def get_prep_id(row):
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
    df2["specimen_ID"] = df.apply(get_prep_id, axis=1)
    df2["organism_name"].replace({"Homo Sapiens":"Human", "Mus musculus":"Mouse"}, inplace=True)
    df2.drop_duplicates(subset=["lims_patch_container"], inplace=True)
    return df2[l_cols]


#-----Ram's functions----#
def get_lims():
    lims_query="""
    SELECT DISTINCT cell.name, cell.patched_cell_container,
    d.external_donor_name AS id_cell_specimen_id, d.full_genotype AS id_slice_genotype, d.name AS donor_name, 
    org.name AS id_species
    FROM specimens cell 
    JOIN specimens slice ON cell.parent_id = slice.id 
    JOIN donors d ON d.id = cell.donor_id
    JOIN organisms org ON d.organism_id = org.id
    WHERE SUBSTRING(cell.patched_cell_container FROM 6 FOR 6) BETWEEN '170101' AND '301231'"""

    df = pd.DataFrame(limsquery(lims_query))
    if is_this_py3:
        df = rename_byte_cols(df)
    return df

def get_specimen_id(row):
    species = row["id_species"]
    specimen_id = row["id_cell_specimen_id"]
    donor_name = row["donor_name"]
    if species == "Mouse":
        return specimen_id
    else:
        return donor_name

def get_modification_date(filename):
    file_time_mod = os.path.getmtime(filename)
    file_date_mod = datetime.fromtimestamp(file_time_mod)
    file_date_mod_str = str(file_date_mod)[0:10]
    return file_date_mod_str
