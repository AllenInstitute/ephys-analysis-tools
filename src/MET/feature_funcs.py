"""Functions to get data out of feature files.
Agata Budzillo."""

def get_fx_file_data(fx_path):
    """Returns feature file data (if it exists), for a filename.
    Parameters
    ----------
    fx_path: str
    Windows file name
    
    Returns
    -------
    dict with feature file data
    """
    import json
    try:
        with open(fx_path) as fx_file:
            data = json.load(fx_file)
    except:
        data = None
    return data
        

def get_features_by_stim_type(fx_path, stim_type):
    """Returns stimulus-specific data (if it exists), for a filename.
    Parameters
    ----------
    fx_path: str
    Windows file name
    
    stim_type: str
    "long_squares", "short_squares", "ramp"
    
    Returns
    -------
    dict with feature file data
    """

    data = get_fx_file_data(fx_path)
    if data:
    	try:
    		return data["cell_features"][stim_type]
    	except:
    		return None
    else:
    	return None

def get_cell_record(fx_path):
    """Returns cell record summary from feature file.
    Parameters
    ----------
    fx_path: str
    Windows file name
    
    Returns
    -------
    dict with feature file cell summary
    """
    data = get_fx_file_data(fx_path)
    try:
        return data["cell_record"]
    except:
        return None

def get_width_ss_spike_1(fx_path):
    """Returns mean spike width of first spike in common amp short square sweeps (if this exists).
    Parameters
    ----------
    fx_path: str
    Windows file name
    
    Returns
    -------
    float
    """
    import numpy as np
    ss = get_features_by_stim_type(fx_path, "short_squares")
    if ss:
        try:
            spiking_sweeps = ss["common_amp_sweeps"]
            mean_ap_width = np.mean([s["spikes"][0]["width"] for s in spiking_sweeps])
            return mean_ap_width
        except:
            return None
    else:
        return None

    
def get_mean_width_ls_spike_1(fx_path):
    """Returns mean spike width of first spike from all spiking long square sweeps (if this exists).
    Parameters
    ----------
    fx_path: str
    Windows file name
    
    Returns
    -------
    float
    """
    import numpy as np
    ls = get_features_by_stim_type(fx_path, "long_squares")
    if ls:
        try:
            spiking_sweeps = ls["spiking_sweeps"]
            mean_ap_width = np.mean([s["spikes"][0]["width"] for s in spiking_sweeps])
            return mean_ap_width
        except:
            return None
    else:
        return None

def get_isi_cvs(fx_path, stims=[20,40]):
    """Returns ISI CVs calculated from spiking long square sweeps (if this exists).
    Parameters
    ----------
    fx_path: str
    Windows file name
    
    Returns
    -------
    float
    """
    import numpy as np

    ls = get_features_by_stim_type(fx_path, "long_squares")
    if ls:
        try:
        	rheo = ls["rheobase_i"]
        	spiking_sweeps = ls["spiking_sweeps"]
        	isi_cv_list = [(s["stim_amp"]-rheo,s["isi_cv"]) for s in spiking_sweeps]
        	med_isi_cvs = []
        	for s in stims:
        		isi_cvs = [j for i,j in isi_cv_list if i==s]
        		med_isi_cvs.append(np.median(isi_cvs) if len(isi_cvs) >0 else np.nan)
        	return med_isi_cvs
        except:
        	return None
    else:
    	return None