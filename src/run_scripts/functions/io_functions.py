"""
---------------------------------------------------------------------
File name: io_functions.py
Maintainer: Ramkumar Rajanbabu
---------------------------------------------------------------------
Author: Ramkumar Rajanbabu
Date/time created: 04/02/2022
Description: IO related functions
---------------------------------------------------------------------
"""


#-----Imports-----#
# General imports
import fnmatch
import os
import pandas as pd
from datetime import datetime


#-----Functions-----#
def get_jsons_walk(dirname, expt, delta_days=None):
    """Return filepaths of metadata files that were created within
    delta_days of today. Searches within subdirectories as well.
    
    Parameters
    ----------
    dirname : string
        Path to metadata file directory.
    expt : string
        Experiment type for filename match ("PS" or "IVSCC" or "").
    delta_days : int
        A number of days in the past. If no number of days is provided, 
        make it approximately the number of days since jsons have been collected.

    Returns
    -------
    json_paths : list
        A list of filepaths that are a expt string match and were
        created within delta_days of today.
    """
    
    json_paths = []
    comparison_date = datetime.today()
    if delta_days is None:
        oldest_date = datetime(2016,1,1)
        delta_days = (comparison_date - oldest_date).days 
    
    for root, directories, jfiles in os.walk(dirname):
        for jfile in jfiles: 
            if fnmatch.fnmatch(jfile,'*%s.json' %expt):
                jpath = os.path.join(root, jfile)
                created_date = datetime.fromtimestamp(os.path.getctime(jpath))
                if abs((comparison_date - created_date ).days) < delta_days:
                    json_paths.append(jpath)
    return json_paths


def save_xlsx(df, dirname, spreadname, norm_d, head_d):
    """Save an excel spreadsheet from dataframe
    
    Parameters
    ----------
    df : pandas dataframe
    dirname : string
    spreadname : string
    norm_d, head_d: dictionaries
    
    Returns
    -------
    Saved .xlsx file with name spreadname in directory dirname.
    """
    
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(os.path.join(dirname, spreadname), engine="xlsxwriter", date_format="mm/dd/yy")
    
    # Convert the dataframe to an XlsxWriter Excel object.
    df.to_excel(writer, sheet_name='Sheet1', index=False)    
    
    # Get the xlsxwriter workbook and worksheet objects.
    workbook  = writer.book
    worksheet = writer.sheets['Sheet1']
    norm_fmt = workbook.add_format(norm_d)
    head_fmt = workbook.add_format(head_d)
    worksheet.set_column('A:L', 26, norm_fmt)

    # Write the column headers with the defined format.
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num, value, head_fmt)
    try:
        writer.save()
    except IOError:
        print("\nOh no! Unable to save spreadsheet :(\nMake sure you don't already have a file with the same name opened.")


def is_this_py3():
    """'Checks whether interpreter is Python 3.
    
    
    Returns
    -------
        Boolean: True if interpreter is Python 3 else False
    
    """

    import sys
    if (sys.version_info > (3,0)):
        return True
    else:
        return False


def validated_input(prompt_text, invalid_response, valid_options=None):
    """Keep asking user for input until a valid input has been entered
    
    Parameters
    ----------
        prompt_text (string): [description]
        invalid_response (string): A message to return to user if entry was invalid.
        valid_options (None or list, optional): Optional argument with valid options. Defaults to None.
    
    Returns
    -------
        result (string): User's validated response to prompt text.
    
    """

    while True:
        if is_this_py3():
            result = input(prompt_text)
        else:
            result = raw_input(prompt_text)
        result = result.lower()
        if (valid_options) and (result not in valid_options):
            response = get_response_to_invalid(result, invalid_response, valid_options)
            print(response)
            continue
        else:
            break
    return result


def validated_date_input(prompt_text, invalid_response, valid_options=None):
    """Prompt user to enter date, and check whether date is valid input.
    Keep prompting until a valid input has been entered.
    
    Parameters
    ----------
        prompt_text (string): [description]
        invalid_response (string): A message to return to user if entry was invalid.
        valid_options (None or list, optional): Optional argument with valid options. Defaults to None.
    
    Returns
    -------
        result (string): User's validated response to prompt text.
    
    """

    while True:
        if is_this_py3():
            result = input(prompt_text)
        else:
            result = raw_input(prompt_text)
        result = result.lower()
        try:
            datetime.strptime(result, "%y%m%d")
        except:
            response = get_response_to_invalid(result, invalid_response, valid_options)
            print(response)
            continue
        else:
            break
    return result


def get_similar_values(invalid_val, valid_vals):
    """Look for valid values that start with the first letter of user entry.
    
    Parameters
    ----------
        invalid_val ([type]): [description]
        valid_vals ([type]): [description]
    
    Returns
    -------
        [type]: [description]
    
    """

    if len(invalid_val) > 0:
        first = invalid_val[0]
        result = [v for v in valid_vals if v.startswith(first)]
    else:
        result = []
    return result


def get_response_to_invalid(invalid_val, invalid_response, valid_vals=None):
    """Return a non-specific error message, or similar valid values if they exist
    
    Parameters
    ----------
        invalid_val ([type]): [description]
        invalid_response ([type]): [description]
        valid_vals ([type], optional): [description]. Defaults to None.
    
    Returns
    -------
        [type]: [description]
    
    """

    if valid_vals:
        similar = get_similar_values(invalid_val, valid_vals)
        if len(similar) > 0:
            return "\nPerhaps you meant one of the following? \n%s " %similar
        else:
            return invalid_response
    else:
        return invalid_response


def get_jsons(dirname, expt, delta_days=None):
    """Return filepaths of metadata files that were created within
    delta_days of today.
    
    Parameters
    ----------
    dirname : string
        Path to metadata file directory.
    expt : string
        Experiment type for filename match ("PS" or "IVSCC" or "").
    delta_days : int
        A number of days in the past. If no number of days is provided, 
        make it approximately the number of days since jsons have been collected.

    Returns
    -------
    json_paths : list
        A list of filepaths that are a expt string match and were
        created within delta_days of today.
    """

    json_paths = []
    comparison_date = datetime.today()
    if delta_days is None:
        oldest_date = datetime(2016,1,1)
        delta_days = (comparison_date - oldest_date).days 
    
    for jfile in os.listdir(dirname):
        if fnmatch.fnmatch(jfile,'*%s.json' %expt):
            jpath = os.path.join(dirname, jfile)
            created_date = datetime.fromtimestamp(os.path.getctime(jpath))
            if abs((comparison_date - created_date ).days) < delta_days:
                json_paths.append(jpath)
    return json_paths
