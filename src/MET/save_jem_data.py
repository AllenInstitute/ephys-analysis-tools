
import os
import numpy as np
import pandas as pd
from lims_funcs import limsquery
from datetime import datetime
from get_met_data import make_metadata_csv, flatten_jem_data, clean_metadata_fields, check_with_lims


# report_dir = 'C:\\Users\\agatab\\Documents\\analysis-projects\\ps-reports'
output_dir = '//allen/programs/celltypes/workgroups/279/Patch-Seq/compiled-jem-data/raw_data/'

datestr = datetime.today().strftime('%y%m%d')
#fn = '%s_jem' %datestr
fn = "jem_metadata"

if __name__ == '__main__':
    _, _ = make_metadata_csv(default_json_dir=None, start_day_str="171001", fn=fn) #, output_dir=output_dir)
