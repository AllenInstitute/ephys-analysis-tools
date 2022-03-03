# -*- coding: utf-8 -*-
"""
Created on Wed Nov 22 11:19:34 2017

@author: agatab
"""

import re



"""-------------------------FUNCTION DEFINITIONS--------------------------------"""


def approximate_fix_delta(slice_time, wc_time, jem_time, organism):
    """Uses JEM creation timestamp to approximate duration of time between whole cell establishment
    and fixation for a given experiment.
    
    Parameters
    ----------
    slice_time : 
    wc_time : 
    jem_time :
    organism : string

    Returns
    -------
    fix_delta_m : float (minutes)

    """

    from dateutil import parser
    from datetime import timedelta, datetime
    
    slice_dt = parser.parse(slice_time[0:-5])
    wc_dt = parser.parse(wc_time)
    jem_dt = parser.parse(jem_time)
    
    min_rollover_h = -14 #Minumum slice time difference for rollover (in hours)
    
    if (wc_dt.time() < slice_dt.time()):
        rollover = (datetime.combine(slice_dt.date(), wc_dt.time()) - slice_dt).total_seconds()/3600.
        if (rollover < min_rollover_h) & (rollover > -24):
            slice_dt = slice_dt + timedelta(1)

    full_wc_dt = datetime.combine(slice_dt.date(), wc_dt.time())
    
    fix_delta = jem_dt - full_wc_dt
    fix_delta_m = fix_delta.total_seconds()/60.
    
    return fix_delta_m
                          


def identify_alexa_fill_visibility(observations):
    
    """Searches 'extraction.extractionNotes' for indication of Alexa fill visibility." 
     
    Parameters
    ----------
    observations : string
     
    Returns
    -------
    alexa : string
         "visible", "not_visible", "unknown"

    """
    
    if isinstance(observations, float):
        return "unknown"
    else:
        observations = observations.lower()
        if ("no alexa fill" in observations) or ("alexa fill not") in observations:
            alexa = "not_visible"
        elif ("alexa" in observations) or ("fill visible" in observations) or ("visible fill" in observations):
            alexa = "visible"
        else:
            alexa = "unknown"
        
        return alexa


def split_extraction_observations(observations):
    
    """Splits up concatenated extraction observations from JEM form into individual Boolean values 
     
    Parameters
    ----------
    observations : string
     
    Returns
    -------
    fluor : Boolean
         True if observations contain "Fluorescence in Pipette"
    dimmed : Boolean
         True if observations contain "Cell Dimmed"
        
    swelled : Boolean
         True if observations contain "Cell Swelled"
        
    shrunk : Boolean
         True if observations contain "Cell Shrunk"
        
    deep : Boolean
         True if observations contain "Too Deep"
        
    disappeared : Boolean
         True if observations contain "Cell Disappeared"
    """
    
    if isinstance(observations, float):
        return 0, 0, 0, 0, 0, 0
    else:
        if "Fluorescence in Pipette" in observations:
            fluor = 1
        else:
            fluor = 0
            
        if "Cell Dimmed" in observations:
            dimmed = 1
        else:
            dimmed = 0
            
        if "Cell Swelled" in observations:
            swelled = 1
        else:
            swelled = 0
        
        if "Cell Shrunk" in observations:
            shrunk = 1
        else:
            shrunk = 0
        
        if "Too Deep" in observations:
            deep = 1
        else:
            deep = 0
        
        if "Cell Disappeared" in observations:
            disappeared = 1
        else:
            disappeared = 0
        
        return fluor, dimmed, swelled, shrunk, deep, disappeared


def extract_prep_id(name):
    """Return prep id (Human Case ID) from specimen name.
       This is for cases where that information may not be found in LIMS 
       (Human BRAIN Collaboration)
    
    Parameters
    ----------
    name : string
        A LIMS-format specimen name or other valid specimen name.
    
    Returns
    -------
    prep : string
        Human Case ID if applicable.

    """
    lims_pattern = r'^H\d\d\.\d\d.\d\d\d'
    mlab_pattern =  r'^HM_Lab_Test_[a-zA-Z].\d\d'
    
    if name:
        lims_match = re.match(lims_pattern, name)
        mlab_match = re.match(mlab_pattern, name)
        if lims_match:
            prep = lims_match.group()
        elif mlab_match:
            prep = mlab_match.group()
        else:
            prep = None
    else:
        prep = None
    return prep

def extract_organism(name, default=None):
    """Return organism from specimen name.
       This is for cases where that information may not be found in LIMS 
       (Human BRAIN Collaboration)
    
    Parameters
    ----------
    name : string
        A LIMS-format specimen name
    default : string
        What to return if none of the patterns are matched.

    Returns
    -------    
    string
        ex. "Human"
    """
    import re

    human_pattern = r'^H\d\d\.'
    mac_pattern = r'^M\d\d\.'
    mlab_rat_pattern =  r'^HM_Lab_Test_r.\d\d'
    mlab_mouse_pattern =  r'^HM_Lab_Test_m.\d\d'

    if name:
        if re.match(human_pattern, name):
            return "Human"
        if re.match(mlab_rat_pattern, name):
            return "Rat"
        if re.match(mlab_mouse_pattern, name):
            return "Mouse"
        else:
            return default
    else:
        return None

def get_prep_from_specimen_name(name):
    """Return prep name (LabTracksID or Human Case ID).
    
    Parameters
    ----------
    name : string
        A Mouse or Human specimen name
    
    Returns
    -------
    string
        LabTracksID or Human Case ID
    """

    from single_cell_ephys.lims_funcs import limsquery
    
    lims_query_str = "SELECT DISTINCT d.external_donor_name AS labtracksID, \
    d.name AS donor_name, \
    org.name AS organism_name \
    FROM specimens cell \
    LEFT JOIN donors d ON d.id = cell.donor_id \
    JOIN organisms org ON d.organism_id = org.id \
    WHERE cell.name = '%s'" %name
    
    results = limsquery(lims_query_str)
    if len(results) > 0:
        organism = results[0]["organism_name"]
        labtracks = results[0]["labtracksid"]
        donorname = results[0]["donor_name"]  
        if organism == "Mus musculus":
            return labtracks
        else:
            return donorname
    else:
        try:
            return name.split(".")[0].split("-")[-1]
        except:
            return "Not found."
