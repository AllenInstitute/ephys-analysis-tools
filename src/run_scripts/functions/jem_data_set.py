# Allen Institute Software License - This software license is the 2-clause BSD
# license plus a third clause that prohibits redistribution for commercial
# purposes without further permission.
#
# Copyright 2015-2016. Allen Institute. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# 3. Redistributions for commercial purposes are not permitted without the
# Allen Institute's written permission.
# For purposes of this license, commercial purposes is the incorporation of the
# Allen Institute's software into anything for which you will charge fees or
# other compensation. Contact terms@alleninstitute.org for commercial licensing
# opportunities.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
import os
import sys
import json
import numpy as np
import pandas as pd
import re
from datetime import datetime
from dateutil import parser
import pytz

from cerberus import Validator, errors
import functions.schemas


import logging
logger = logging.getLogger('jem-validation')

class sliceValidator(Validator):
    def _validate_regex(self, regex_options, field, value):
        """ {'type': 'dict'} """
        pattern = regex_options['pattern']
        fail_message = regex_options['fail_message']
        re_obj = re.compile(pattern)
        if not re_obj.match(value):
            self._error(field, fail_message)
            

def normalize_dt_format(val, pnw, fmt):
    """Determine whether a value is a string in the correct UTC format, and if not, convert it.
    
    Parameters
    ----------
    val : string or np.nan
    pnw : boolean 
          indicates whether to use -07:00 UTC if UTC is missing
    fmt : string
          output format
          
    Returns
    -------
    val : string or np.nan
    
    """
    
    try:
        ts = parser.parse(val)            
        if ts.tzinfo is not None:
            ts = ts
        else:
            if not (ts.hour == 0 and ts.minute == 0 and ts.second == 0):
                if pnw is True:
                    val += " -07:00"
                    ts = parser.parse(val)
                else:
                    print("Missing UTC.")
                    ts = ts
        ts_formatted = ts.strftime(fmt)
        return ts_formatted
    
    except ValueError:
            #logger.info("Non-datetime entry in datetime field")
            return np.nan



def stitch_container(user, date, tube_str):
    """Stitch together the Patch-seq container from user date and tube ID.
    
    Parameters
    ----------
    user : string
            Patch-seq user code (ex. "P1")
    date : string
            Experiment date (ex. "180314")
    tube_id : string
            tube ID (ex. "1", "12")

    Returns
    -------
    container : string
            19 element long container (ex. "P1S4_180314_001_A01")"""
    
    #if type(tube_id) is str:           
    #    tube_id = unicode(tube_id, 'utf-8')
    #if tube_id.isnumeric():
    #    tube_str = str(int(tube_id))
    if len(tube_str) == 2:
        tube_str = '0' + tube_str
    elif len(tube_str) == 1:
        tube_str = '00' + tube_str
    else:
        tube_str = tube_str  
    container = '%sS4_%s_%s_A01' %(user, date, tube_str)
    #else:
    #    return np.nan
        
    return container


class JemDataSet(object):
    """ A very simple interface for extracting metadata from a JSON file
    created with JEM."""


    def __init__(self, file_path, project_key=None, lab_key=None):
        """ Initialize the NwbDataSet instance with a file name.

        Parameters
        ----------
        file_path: string
           JSON file path
        """
        default_project = "MET"
        default_lab = "AIBSPipeline"       
        
        self.file_path = file_path
        self.file_name = os.path.basename(file_path)

        if project_key is None:
            self.project = default_project
        else:
            self.project = project_key
            
        if lab_key is None:
            self.lab = default_lab
        else:
            self.lab = lab_key

        self._version = None
        self._date = None
        self.data = None
        self._slice_schema = None
        self._pipette_schema = None
        self._pipette_array_name = None
        self._valid_slice = None
        self._valid_attempts = None
        
	
    def get_jem_version(self):
        """ Returns the JEM version number, stored in the field 
        	'formVersion'. If 'formVersion' is missing, version 
        	1.0.0 is returned.

            Returns
            -------
            string: "x.x.x"
        """
        with open(self.file_path) as data_file:
            try:
                slice_info = json.load(data_file)
            except ValueError as e:
                logger.error("Unable to parse JSON data in %s.\n" %self.file_name)
                #logger.warning(e)
                #sys.exit("Unable to parse JSON data in %s." %self.file_name)
                return None
        try:
            self._version = slice_info["formVersion"]
        except KeyError:
            self._version = "1.0.0"
        return self._version
    
    
    def get_experiment_date(self):
        """ Returns experiment date in YYYY-MM-DD format.

            Returns
            -------
            string: "YYYY-MM-DD"
        """
        
        with open(self.file_path) as data_file:
            try:
                slice_info = json.load(data_file)
            except ValueError as e:
                logger.warning(e)
                sys.exit("Unable to parse JSON data in %s." %self.file_name)
        try:
            expt_date = parser.parse(slice_info["date"]).strftime("%Y-%m-%d")
            self._date = expt_date
        except KeyError:
            sys.exit("Slice date field missing in %s." %self.file_name)
        except ValueError:
            sys.exit("Cannot parse 'date' field in %s." %self.file_name)
        return self._date
    
    
    
    def _is_field(self, colname):
        """Determine whether a column name exists in the attempts dataframe.
    
        Parameters
        ----------
        colname : string
        
        Returns
        -------
        Boolean
            Boolean value indicating if the colname exists in the dataframe.
    """
    
        try:
            self.data[colname]
            return True
        except KeyError:
            return False

    
    def _normalize_rig_operator(self):
        """ Replaces full rig operator name (old JEM form style) with user login."""
        
        if self._version >= "2":
            return self.data
        else:
            path = "//allen/programs/celltypes/workgroups/279/Patch-Seq/all-metadata-files/ps_user_info.csv"
            try:
                users = pd.read_csv(path)
                name_to_login = users.set_index("name").to_dict()["login"]
                temp_df = self.data
                temp_df.replace({"rigOperator": name_to_login}, inplace=True)
                self.data = temp_df
                return self.data
            except IOError:
                print("'ps_user_info.csv' not found - rig operator name not normalized.")
                return self.data  
    
            
    
    def _normalize_dates_times(self):
        """ Normalizes date format in available date fields (from different JEM versions) as "YYYY-MM-DD"."""
    
        date_fields =["acsfProductionDate", "internalFillDate", "blankFillDate"]
        time_fields = ['extraction.timeExtractionEnd', 'extraction.timeExtractionStart', 'extraction.timeRetractionEnd',
                       'extraction.timeRetractionStart', 'recording.timeStart', 'recording.timeWholeCellStart']
        
        # If UTC is missing and data was collected at AIBS, use -07:00 UTC.
        if self.lab == "AIBSPipeline":
            pnw = True
        else:
            pnw = False
        
        temp_df = self.data
        
        # Recording Date and Time
        temp_df["date"] = temp_df["date"].apply(lambda x: normalize_dt_format(x, pnw=pnw, fmt="%Y-%m-%d %H:%M:%S %z"))
        # Batch Dates
        for d in date_fields:
            if self._is_field(d):
                temp_df[d] = temp_df[d].apply(lambda x: normalize_dt_format(x, pnw=pnw, fmt="%Y-%m-%d"))
            else:
                temp_df[d] = np.nan
        # Experiment Time Stamps
        for t in time_fields:
            if self._is_field(t):
                temp_df[t] = temp_df[t].apply(lambda x: normalize_dt_format(x, pnw=pnw, fmt="%H:%M:%S %z") if not pd.isnull(x) else x)           
            else:
                temp_df[t] = np.nan
        
        self.data = temp_df
        
        return self.data
    
    def _fillna_rois(self):
        """ Fills nans in all available roi fields (from different JEM versions) with "None, None"."""
    
        roi_fields =["autoRoi", "manualRoi", "approach.autoRoi", "approach.manualRoi", "approach.anatomicalLocation"]
        temp_df = self.data

        for roi in roi_fields:
            if self._is_field(roi):
                temp_df[roi].fillna("None, None", inplace=True)
                temp_df[roi].replace("None", "None, None", inplace=True)
        self.data = temp_df

        return self.data


    def _extract_roi(self):
        """Return ROI major, ROI minor and acronym from autoRoi or manualRoi or anatomicalLocation (old JEM) fields in      dataframe.
    
        Parameters
        ----------
        row : a pandas dataframe row 
        roi_df : a dataframe translating roi names to comma separated major and minor areas, and acronym
    
        Returns
        -------
        tuple
        Tuple with major recording region, minor recording region and acronym.
        """
        
        self.data = self._fillna_rois()
        temp_df = self.data
        
        if self._version >= "2.0.2":
            autoRoi = "autoRoi"
            manualRoi = "manualRoi"
            roi = temp_df.apply(lambda x: x[autoRoi] if x[autoRoi] != "None, None" else x[manualRoi], axis=1)
            temp_df.drop([autoRoi, manualRoi], axis=1, inplace=True)
        
        elif self._version >= "2":
            autoRoi = "approach.autoRoi"
            manualRoi = "approach.manualRoi"
            roi = temp_df.apply(lambda x: x[autoRoi] if x[autoRoi] != "None, None" else x[manualRoi], axis=1)
            temp_df.drop([autoRoi, manualRoi], axis=1, inplace=True)

        else:
            unstructuredRoi = "approach.anatomicalLocation"
            roi = temp_df[unstructuredRoi]
            temp_df.drop([unstructuredRoi], axis=1, inplace=True)

        temp_df["roi"] = roi
        self.data = temp_df
        return self.data

    
    def _generate_ps_container(self):
        """Generate LIMS tube ID (aka Patch Container) for a PatchSeq sample
        or Tissue Touch from user, date and tube ID in pre-2.0.2 JEM versions, 
        or take directly from extraction.tubeID in >= 2.0.2 JEM versions.
    

        Returns
        -------
        lims_tube_id : string
             LIMS tube ID (aka Patch Container).
        """

        temp_df = self.data
        date = parser.parse(self._date).strftime("%y%m%d")
        
        if self.project != "ME" and (sum(temp_df["status"].str.contains("SUCCESS"))>=1):
            if self._version >= "2.0.2":
                containers = temp_df["extraction.tubeID"]             
            else:
                if "approach.pilotName" in temp_df.columns:
                    tube_ids = temp_df.apply(lambda x: x["approach.pilotTest05"] if (x["approach.pilotName"] == "Tissue_Touch" and "approach.pilotTest05" in temp_df.columns) else x["extraction.tubeID"], axis=1)
                else:
                    tube_ids = temp_df["extraction.tubeID"]
                path = "//allen/programs/celltypes/workgroups/279/Patch-Seq/all-metadata-files/ps_user_info.csv"
                try:
                    users = pd.read_csv(path)
                    login_to_user = users.set_index("login").to_dict()["p_user"]
                    user_code = login_to_user[temp_df["rigOperator"].values[0]]
                    containers = tube_ids.apply(lambda x: stitch_container(user_code, date, x) if x is not np.nan else np.nan)
                except IOError:
                    containers = np.nan
                    print("'ps_user_info.csv' not found - Patch-seq container could not be generated.")
            temp_df.drop(["extraction.tubeID"], axis=1, inplace=True)
        else:
            containers = np.nan

        temp_df["container"] = containers
        self.data = temp_df
        return self.data
    
    
    def _add_empty_columns(self):
        """Ensure that all MET Production JEM form columns are present in 
        final data (add empty values if not)"""
        
        met_cols = ['acsfProductionDate','blankFillDate','date','flipped',
                    'formVersion','internalFillDate','limsSpecName',
                    'rigNumber','rigOperator','sliceQuality',
                    'approach.cellHealth','approach.creCell',
                    'approach.pilotName','approach.sliceHealth','depth',
                    'extraction.endPipetteR','extraction.extractionObservations',
                    'extraction.nucleus','extraction.postPatch',
                    'extraction.pressureApplied',
                    'extraction.retractionPressureApplied',
                    'extraction.sampleObservations','extraction.timeExtractionEnd',
                    'extraction.timeExtractionStart','extraction.timeRetractionEnd',
                    'extraction.fillquality','failureNotes','qcNotes','recording.pipetteR',
                    'recording.timeStart','recording.timeWholeCellStart','status',
                    'attempt','roi','container']
        
        temp_df = self.data
        for c in met_cols:
            if c not in temp_df.columns:
                if c == "status":
                    temp_df[c] = "NO ATTEMPTS"
                else:
                    temp_df[c] = np.nan
        
        self.data = temp_df
        
        return self.data
            
    def _define_schemas(self, version):
        """Return correct slice_schema, pipette_array_name and pipette_schema."""
        
        if self.project == "MET" and self.lab == "AIBSPipeline":
            if version >= "2":
                self._slice_schema = schemas.schemas["met_slice"]
                #self._pipette_schema = schemas.schemas["met_pipette"]
                self._pipette_array_name = "pipettes"

            else:
                self._slice_schema = schemas.schemas["met_slice_outdated"]
                #self._pipette_schema = schemas.schemas["met_pipette_outdated"]
                self._pipette_array_name = "pipettesPatchSeqPilot"
                logger.info(" %s:\n Old JEM version may have incomplete metadata. \n" %(self.file_name))
    
    def validate_slice(self):
        """Return validation of slice metadata.
    
        Returns
        -------
        error_log : 
        
        """
                
        version = self.get_jem_version()
        self._define_schemas(version)
        
        with open(self.file_path) as data_file:
            slice_info = json.load(data_file)
            #(A dictionary of slices with nested pipette attempts)
        slice_info["formVersion"] = version
                

        v = sliceValidator(self._slice_schema)
        v.allow_unknown = True
        v.validate(slice_info)
        error_dict = v.errors
        if any(error_dict):
            logger.warning(" Error(s) found in %s:\n %s\n" %(self.file_name, error_dict))
            self._valid_slice = False
        else:
            self._valid_slice = True
        return slice_info, error_dict

    def validate_attempts(self, attempts):
        """Return validation of nested attempts metadata.
    
        Returns
        -------
        error_log : 
        
        """
        data = attempts.to_dict('records')  
        attempts_errors = []
        for i, d in enumerate(data):
            pipette_v = sliceValidator(schemas.schemas["met_pipette"])
            pipette_v.allow_unknown = True
            pipette_v.validate(d)
            attempts_errors.append(pipette_v.errors)
            if any(pipette_v.errors):
                logger.warning(" Error(s) found in %s, attempt# %d:\n %s\n" %(self.file_name, i+1, pipette_v.errors))
                self._valid_attempts = False
            else:
                self._valid_attempts = True
                
        
            if self._valid_attempts:
                if d["status"] == "SUCCESS":
                    tube_v = sliceValidator(schemas.schemas["met_tube"])
                    tube_v.allow_unknown = True
                    tube_v.validate(d)
                    attempts_errors.append(tube_v.errors)
                    if any(tube_v.errors):
                        logger.warning(" Error(s) found in %s, attempt# %d:\n %s\n" %(self.file_name, i+1, tube_v.errors))
                        self._valid_attempts = False
        
        
        
    def get_data_dev(self):
        """Return flattened slice metadata dataframe.
    
        Returns
        -------
        attempts_slice_df : pandas dataframe
            A dataframe of all pipette attempts along with all slice metadata."""
        
        #version = self.get_jem_version()
        #self.get_experiment_date()
        #with open(self.file_path) as data_file:
        #    slice_info = json.load(data_file)
            #(A dictionary of slices with nested pipette attempts)
        # (Pre-version 2 contains IVSCC, PatchSeq and Electroporation arrays)
        #if version >= "2":
        #    array_name = "pipettes"
        #else:
        #    array_name = "pipettesPatchSeqPilot"       

        version = self.get_jem_version()
        if version == None:
            return None
        else:
            self.get_experiment_date()
            slice_info, slice_error_dict = self.validate_slice()
            slice_info["jem_created"] = datetime.fromtimestamp(os.path.getctime(self.file_path))
            df = pd.json_normalize(slice_info)
            try:
                attempts = pd.json_normalize(slice_info[self._pipette_array_name])
                self.validate_attempts(attempts)
                attempts["limsSpecName"] = df["limsSpecName"].values[0]
                attempts["attempt"] = [p + 1 for p in attempts.index.values]
                attempts_slice_df = pd.merge(df, attempts, how="outer", on="limsSpecName")
                attempts_slice_df.drop(self._pipette_array_name, axis=1, inplace=True)

                self.data = attempts_slice_df
                self.data = self._normalize_rig_operator()
                self.data = self._normalize_dates_times()

                if len(attempts) > 0:
                    self.data = self._extract_roi()
                    self.data = self._generate_ps_container()
                self.data = self._add_empty_columns()

                return self.data

            except KeyError:
                #logger.error("No valid pipette array in %s\n" %(self.file_name))
                return None
        
            #if not any(error_dict):
            #    attempts = pd.json_normalize(slice_info[self._pipette_array_name])
            #    return slice_info, attempts
            #else:
            #    return None, None

    def get_data(self):
        """Return flattened slice metadata dataframe.
    
        Returns
        -------
        attempts_slice_df : pandas dataframe
            A dataframe of all pipette attempts along with all slice metadata."""
        
    
        version = self.get_jem_version()
        if version == None:
            return None
        else:
            self.get_experiment_date()

            with open(self.file_path) as data_file:
                slice_info = json.load(data_file)
                #(A dictionary of slices with nested pipette attempts)
                slice_info["jem_created"] = datetime.fromtimestamp(os.path.getctime(self.file_path))

        
            df = pd.json_normalize(slice_info)
            df["formVersion"] = version
        
            # (Pre-version 2 contains IVSCC, PatchSeq and Electroporation arrays)
            if version >= "2":
                array_name = "pipettes"
            else:
                array_name = "pipettesPatchSeqPilot"
    
            attempts = pd.json_normalize(slice_info[array_name])
            try:
                attempts["limsSpecName"] = df["limsSpecName"].values[0]
            except KeyError:
                sys.exit("'limsSpecName' field missing in %s." %self.file_name)
            attempts["attempt"] = [p+1 for p in attempts.index.values]
            attempts_slice_df = pd.merge(df, attempts, how="outer", on="limsSpecName")
            attempts_slice_df.drop(array_name, axis=1, inplace=True)
        
        
            self.data = attempts_slice_df
            self.data = self._normalize_rig_operator()
            self.data = self._normalize_dates_times()

        
            if len(attempts) > 0:
                self.data = self._extract_roi()
                self.data = self._generate_ps_container()
            self.data = self._add_empty_columns()
        
            return self.data
    