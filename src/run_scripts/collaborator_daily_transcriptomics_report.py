"""
---------------------------------------------------------------------
File name: collaborator_daily_transcriptomics_report.py
Maintainer: Ramkumar Rajanbabu
---------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 05/19/2023
Description: Generate Collaborator daily transcriptomics report (excel document)
---------------------------------------------------------------------
"""


#-----Imports-----#
# General imports
import json
import numpy as np
import os
import pandas as pd
from datetime import datetime, date, timedelta
from pandas.tseries.offsets import CustomBusinessDay
from pandas.tseries.holiday import USFederalHolidayCalendar
from pathlib import Path, PureWindowsPath
from tkinter.filedialog import askdirectory
# File imports
from functions.file_functions import get_jsons, load_data_variables
from functions.jem_data_set import JemDataSet
from functions.io_functions import validated_input, validated_date_input,save_xlsx, get_jsons_walk


#-----General Information-----#
"""
project_dictionary details: New project codes (2021-present)
- 102-01-045-10: CTY IVSCC (Mouse/NHP) # IVSCC
- 102-01-061-20.3 : CTY BICAN Human and NHP Atlas # IVSCC
- 122-01-002-20.2.1 : AIND Thalamus U19 # IVSCC
- 102-04-006-20 : MSP Measuring Consciousness Ph2 (TBD) # HCT
- 102-01-051-20: CTY Neuromodulation of NHP Cell Types # HCT

project_dictionary details: Old project codes
- 102-01-020-20: CTY BRAIN Human Cell Types (Human Acute/Culture, U01 shipping pilot) # IVSCC (10/01/2017 - 6/03/2022) 
- 102-04-009-10: CTY SR: Targeted CNS Gene Therapy (Dravet pilot) # IVSCC (Dates?)
"""


# Ask user for input on file directory location
JSON_DIR = askdirectory()

json_paths = get_jsons_walk(dirname=JSON_DIR, expt="PS")
json_df = pd.DataFrame()
for json_path in json_paths:
    json = JemDataSet(json_path)
    slice_data = json.get_data()
    if slice_data is None:
        continue
    success_slice_data = slice_data[slice_data["status"].str.contains("SUCCESS")]
    json_df = pd.concat([json_df, slice_data], axis=0, sort=True)
json_df.reset_index(drop=True, inplace=True)


json_df.to_csv(os.path.join("C:/Users/ramr/Documents/Github/ai_repos/ephys-analysis-tools/collab_shipment.csv"), index=False)

