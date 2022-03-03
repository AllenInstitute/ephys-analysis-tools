import numpy as np
import pandas as pd


def print_grouped_counts(data, grouping_fields, count_field, dropna=False, normalize=False):
    """Prints value counts for a dataframe field after grouping by another field
    
    Parameters
    ----------
    data : pandas dataframe

    grouping_fields : string or list of string(s)
        Name of column(s) in the dataframe for grouping.

    count_field : string
        Name of a column in the dataframe for which to show value counts.
        
    dropna : boolean
        Indicates whether to drop null values in value counts. Default is True.
        
    normalize : boolean
        Indicates whether to print normalized value counts. Default is False.
    """
    
    if not isinstance(grouping_fields, list):
        grouping_fields = [grouping_fields]
    for f in grouping_fields+[count_field]:
        if f not in data.columns:
            raise Exception("%s is not a valid fieldname." %f)
    
    grouped = data.groupby(by=grouping_fields)
    for name, group in grouped:
        name = ' & '.join(name) if isinstance(name, tuple) else name 
        print("\n%s:" %name)
        print(group[count_field].value_counts(dropna=dropna, normalize=normalize).to_string(header=None, index=True))