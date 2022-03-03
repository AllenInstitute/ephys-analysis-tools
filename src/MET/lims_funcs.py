# -*- coding: utf-8 -*-
"""
Created on Wed May 9 10:11:00 2018

@author: agatab
"""


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

def linux_to_windows(file_name):

    import re, os
    
    p = re.compile('/(.*?)/(.*?)/vol1/(.*)')
    p2 = re.compile('/allen/(.*)')

    m = p.match(file_name)
    m2 = p2.match(file_name)

    if m:
        prefix = ""
        if m.group(1) == "data":
            prefix = "\\\\aibsdata"
        elif m.group(1) == "projects":
            #prefix = "\\\\titan\\cns"
            prefix = "\\\\allen\\programs\\celltypes\\production"
        return os.path.normpath(os.path.join(prefix, m.group(2), m.group(3)))
    elif m2:
        return os.path.normpath('/' + file_name)
    else:
        return os.path.normpath(file_name)

def linux_to_mac(file_name):
    # For Mac OS, where //allen/programs/celltypes/ is mounted as /volumes/celltypes/

    import re, os
    
    p = re.compile('/(.*?)/(.*?)/vol1/(.*)')
    p2 = re.compile('/allen/(.*)')

    m = p.match(file_name)
    m2 = p2.match(file_name)

    if m:
        prefix = ""
        if m.group(1) == "data":
            prefix = "\\\\aibsdata"
        elif m.group(1) == "projects":
            prefix = "/volumes/celltypes/production"
        return os.path.normpath(os.path.join(prefix, m.group(2), m.group(3)))
    #elif m2:
    #    file_name = file_name.replace("/allen/programs/celltypes/", "/volumes/celltypes/")
    #    return os.path.normpath('/' + file_name)
    else:
        file_name = file_name.replace("/allen/programs/celltypes/", "/volumes/celltypes/")
        return os.path.normpath(file_name)
 
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


def is_this_py3():
    """Checks whether interpreter is Python 3.
    
    
    Returns
    -------
        Boolean : True if interpreter is Python 3 else False
    
    """

    import sys
    if (sys.version_info > (3,0)):
        return True
    else:
        return False