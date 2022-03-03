"""Various functions to analyze MET data.
Agata Budzillo."""

import numpy as np
from lims_funcs import limsquery, linux_to_windows
from allensdk.core.nwb_data_set import NwbDataSet

def label_ei_human(cluster, nodes_inh, nodes_exc):
    """Takes a human transcriptomic cluster label and categorizes it 
    as excitatory, inhibitory or neither.
    
    Parameters
    ----------
    cluster : string
        Transcriptomic cluster label (human only).

    nodes_inh : list of strings
        A list of internal nodes in the inhibitory part of the human dendrogram. 
        for example - ["n" + str(i) for i in range(3,47,1)]

    nodes_exc : list of strings
        A list of internal nodes in the excitatory part of the human dendrogram. 
        for example - ["n" + str(i) for i in range(47,63,1)]

    Returns
    -------
    label : string
        (inhibitory, excitatory, neither)
    """

    pattern_inh = r'^cl[0-9]{1,}_i[0-9]{1,}_'
    nodes_inh = ["n" + str(i) for i in range(3,47,1)]
    pattern_exc = r'^cl[0-9]{1,}_e[0-9]{1,}_'
    nodes_exc = ["n" + str(i) for i in range(47,63,1)]
        
    import re
    if re.match(pattern_inh, cluster) or cluster in nodes_inh:
        label = "inhibitory"
    elif re.match(pattern_exc, cluster) or cluster in nodes_exc:
        label = "excitatory"
    else:
        label = "neither"

    return label


def label_ei_mouse(cluster, terms_inh, terms_exc, terms_neither):
    """Takes a mouse transcriptomic cluster label and categorizes it 
    as excitatory, inhibitory or neither.
    
    Parameters
    ----------
    cluster : string
        Transcriptomic cluster label (mouse only).

    terms_inh : list of strings
        A list of cluster detail labels for internal and leaf nodes in the inhibitory 
        part of the mouse dendrogram. 
        for example - ["Vip", "Sst", "Pvalb", "Lamp5", "Sncg", "Inhibitory"]

    terms_exc : list of strings
        A list of cluster detail labels for internal and leaf nodes in the excitatory 
        part of the mouse dendrogram. 
        for example - ["L2/3", "L4", "L5", "L6 ", "L6a", "L6b", "IT", "PT", "CT","Car3", "Cdh13", "Excitatory"]
        
    terms_neither : list of strings
        A list of cluster detail labels for internal and leaf nodes that are neither in the excitatory 
        nor inhibitory part of the mouse dendrogram. 
        for example - ["Meis2", "n3", "n4", "n5"]

    Returns
    -------
    label : string
        (inhibitory, excitatory, neither)
    """

    import re

    if any(term in cluster for term in terms_inh):
        return "inhibitory"
    elif any(term in cluster for term in terms_exc):
        return "excitatory"
    elif any(term in cluster for term in terms_neither):
        return "neither"
    
def assign_morpho_quality(img, dend, go, qc, swc):
    """Categorize the quality of a morphology sample based on various
    LIMS fields in the morphology pipeline.
    
    Parameters
    ----------
    img : float
        63x image id # OR nan if not available.
    dend : string/unicode
        dendrite type OR nan if not available.
        ('dendrite type - spiny', 'dendrite type - NA', 
        'dendrite type - sparsely spiny', 'dendrite type - aspiny') 
    go:  string/unicode
        63x image go/no go decision OR nan if not available.
        ('63x no go', '63x go')
    qc : string/unicode
        63x image qc tag OR nan if not available.
        ('qc', 'image_qc_passed', 'deferred', 'ready_for_dendrite_trace',
         'rescanning', 'autotracing', 'failed','ready_for_neuron_trace', 
         'processing', 'passed')
    swc : string/unicode
        path to reconstruction file OR nan if not available. 
    
    Returns
    -------
    quality : string
        A quality category for the morphology specimen -
        (best, great, not great, worst, not looked at, drop cell)
    """

    import numpy as np

    # Is there a 63x image ID OR has there been a 63x go decision?
    if not np.isnan(img) or go == "63x go":
        # Was the cell deferred?
        if qc == "deferred":
            quality = "not great"
        else:
            # Has the image failed qc?
            if qc == "failed":
                quality = "drop cell"
            else:
                # Is the dendrite type == NA?
                if dend == "dendrite type - NA":
                    quality = "worst"
                else:
                    # Is there a reconstruction?
                    if isinstance(swc, str) or isinstance(swc, unicode):
                        quality = "best"
                    else:
                        quality = "great"
    else:
        # Is there a dendrite type assignment?
        if isinstance(dend, str) or isinstance(dend, unicode):
            # Is the dendrite type == NA?
            if dend == "dendrite type - NA":
                quality = "worst"
            else:
                quality = "not great"
        else:
            quality = "not looked at"      
                
    return quality


def assign_postpatch(nucleus, resistance, nuc_thresh=500, nonuc_thresh=500):
    """
    
    Parameters
    ----------
    nucleus : string
            May be in old ('No-Seal', 'Nucleated', 'Partial-Nucleus', 'Outside-Out') or
            new ('nucleus_absent', 'nucleus_present') format.
    resistance : float
    nuc_thresh : int
            Threshold for nucleus / partial nucleus (default is 500 MOhm)
    nonuc_thresh : int
            Threshold for outside out / no seal (default is 500 MOhm)
    
    Returns
    -------
    """

    d = {"No-Seal":"no seal", 
         "Nucleated":"nucleated", 
         "Partial-Nucleus":"partial nucleated", 
         "Outside-Out":"outside out"}
    
    if nucleus == "nucleus_present":
        if resistance >= nuc_thresh:
            return "nucleated"
        else:
            return "partial nucleated"
    elif nucleus == "nucleus_absent":
        if resistance >= nonuc_thresh:
            return "outside out"
        else:
            return "no seal"
    else:
        return d[nucleus]
    
def assign_postpatch_v2(nucleus, resistance, nuc_thresh=500, nonuc_thresh=500):
    """
    
    Parameters
    ----------
    nucleus : string
            May be in old ('No-Seal', 'Nucleated', 'Partial-Nucleus', 'Outside-Out') or
            new ('nucleus_absent', 'nucleus_present') format.
    resistance : float
    nuc_thresh : int
            Threshold for nucleus / partial nucleus (default is 500 MOhm)
    nonuc_thresh : int
            Threshold for outside out / no seal (default is 500 MOhm)
    
    Returns
    -------
    """

    d = {"No-Seal":"no seal", 
         "Nucleated":"nucleated", 
         "Partial-Nucleus":"nucleated", 
         "Outside-Out":"outside out"}
    
    if nucleus == "nucleus_present":
        return "nucleated"
    elif nucleus == "nucleus_absent":
        if resistance >= nonuc_thresh:
            return "outside out"
        else:
            return "no seal"
    else:
        return d[nucleus]
    

def assign_postpatch_simple(nucleus):
    """
    
    Parameters
    ----------
    nucleus : string
            May be in old ('No-Seal', 'Nucleated', 'Partial-Nucleus', 'Outside-Out') or
            new ('nucleus_absent', 'nucleus_present') format.
    
    Returns
    -------
    string
    """
    d = {"No-Seal":"nucleus-", 
         "Nucleated":"nucleus+", 
         "Partial-Nucleus":"nucleus+", 
         "Outside-Out":"nucleus-"}

    if nucleus in d.keys():
        return d[nucleus]
    elif nucleus == "nucleus_present":
        return "nucleus+"
    else:
        return "nucleus-"
    

def get_data_sweep(nwb_path, sweep_number):
    """Returns stimulus, response and time series as well as 
    sampling rate given an NWB path from LIMS and sweep number of a 
    voltage clamp trace in that dataset.
    
    Parameters
    ----------
    nwb_path : string
        NWB path from LIMS
    sweep_number: integer
    
    Returns
    -------
    v : array (in Volts)
    i : array (in Amps)
    t : array (in seconds)
    sampling_rate : integer
    """

    p = linux_to_windows(nwb_path)
    data_set = NwbDataSet(p)
    i, v, t, sampling_rate = get_cc_sweep_v_i_t_from_set(data_set, sweep_number)
    i = i*1E12 # pA
    v = v*1E3 # mV
    t = np.arange(0, len(v)) * (1.0 / sampling_rate) # s

    return i, v, t



def get_vc_sweep_v_i_t_from_set(data_set, sweep_number):
    
    """Returns stimulus, response and time series as well as 
    sampling rate given an NWB dataset and sweep number of a 
    voltage clamp trace in that dataset.
    
    Parameters
    ----------
    dataset : 
        nwb dataset
    sweep_number: integer
    
    Returns
    -------
    v : array (in Volts)
     
    i : array (in Amps)
    t : array (in seconds)
    sampling_rate : integer
    """


    sweep_data = data_set.get_sweep(sweep_number)
    v = sweep_data["stimulus"] # in V
    i = sweep_data["response"] # in A

    sampling_rate = sweep_data["sampling_rate"] # in Hz
    t = np.arange(0, len(v)) * (1.0 / sampling_rate)
    return v, i, t, sampling_rate


def get_cc_sweep_v_i_t_from_set(data_set, sweep_number):
    
    """Returns stimulus, response and time series as well as 
    sampling rate given an NWB dataset and sweep number of a 
    current clamp trace in that dataset.
    
    Parameters
    ----------
    dataset : 
        nwb dataset

    sweep_number: integer
    
    Returns
    -------
    v : array (in Volts)
     
    i : array (in Amps)

    t : array (in seconds)

    sampling_rate : integer

    """


    sweep_data = data_set.get_sweep(sweep_number)
    i = sweep_data["stimulus"] # in A
    v = sweep_data["response"] # in V

    sampling_rate = sweep_data["sampling_rate"] # in Hz
    t = np.arange(0, len(i)) * (1.0 / sampling_rate)
    return i, v, t, sampling_rate



def get_avg_deflection(stim_ranges, buff, sampling_rate, trace):
    """Get deflection in last 5 ms of each pulse. Average across all pulses
    
    Parameters
    ----------
    stim_ranges : a list of tuples 
            stimulus onset and offset times (in seconds)
    buff: float
            a buffer to add to stimulus onset and subtract from stimulus offset (in seconds)
    sampling_rate : integer
    trace : array
            stimulus or response trace to analyze
    
    Returns
    -------
    avg_deflection : float
    
    """
    inds=[(int((k-buff)*sampling_rate), int(k*sampling_rate)) for j,k in stim_ranges]
    avg_deflection = np.mean([np.mean(trace[x[0]:x[1]]) for x in inds])
    
    return avg_deflection



def get_pipetteR(path, sweep_number, buff=0.001):
    """
    
    Parameters
    ----------
    path : string
            path to nwb file
    sweep_number: integer
            sweep number of resistance sweep to analyze (ex number of INBATH sweep)
    buff : float
            buffer from stimulus start and end time (in seconds)
    Returns
    -------
    tip_r : float
            pipette resistance in MOhm
    
    """    
    from allensdk.core.nwb_data_set import NwbDataSet
    from single_cell_ephys.lims_funcs import linux_to_windows

    stim_ranges = [(0.005, 0.015), (0.07, 0.08), (0.13, 0.14), (0.19, 0.2)] 

    p=linux_to_windows(path)
    
    try:
        data_set = NwbDataSet(p)
        v, i, t, sampling_rate = get_vc_sweep_v_i_t_from_set(data_set, sweep_number)

        current_baseline = get_avg_deflection([(0, 0.005)], buff, sampling_rate, i)
        current_response = get_avg_deflection(stim_ranges, buff, sampling_rate, i) - current_baseline
        voltage_step = get_avg_deflection(stim_ranges, buff, sampling_rate, v)
        tip_r = (voltage_step/current_response)*1E-6
        return tip_r
    except:
        return None
    
def get_pipetteR_with_stim_ranges(path, sweep_number, stim_ranges, buff=0.001):
    """
    
    Parameters
    ----------
    path : string
            path to nwb file
    sweep_number: integer
            sweep number of resistance sweep to analyze (ex number of INBATH sweep)
    buff : float
            buffer from stimulus start and end time (in seconds)
    Returns
    -------
    tip_r : float
            pipette resistance in MOhm
    
    """    
    from allensdk.core.nwb_data_set import NwbDataSet
    from single_cell_ephys.lims_funcs import linux_to_windows

    p=linux_to_windows(path)
    
    try:
        data_set = NwbDataSet(p)
        v, i, t, sampling_rate = get_vc_sweep_v_i_t_from_set(data_set, sweep_number)

        current_baseline = get_avg_deflection([(0, 0.005)], buff, sampling_rate, i)
        current_response = get_avg_deflection(stim_ranges, buff, sampling_rate, i) - current_baseline
        voltage_step = get_avg_deflection(stim_ranges, buff, sampling_rate, v)
        tip_r = (voltage_step/current_response)*1E-6
        return tip_r
    except:
        return None
    

def plot_cc_sweep(data_set, sweep_number, start_ind=0, end_ind=-1):
    """
    
    Parameters
    ----------
    data_set : NwbDataSet
            
    sweep_number : int

    start_ind : int
        index at which to start plotting the trace
    end_ind : int
        index at which to stop plotting the trace
   
    Returns
    -------

    fig : matplotlib figure

    i : array (in Amps)
     
    v : array (in Volts)

    t : array (in seconds)

    sampling_rate : integer
    """
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(2, 1, sharex=True, figsize=(10,5))
    v, i, t, sampling_rate = get_cc_sweep_v_i_t_from_set(data_set, sweep_number)
    i = i*1E12 # pA
    v = v*1E3 # mV
    axes[0].plot(t[start_ind:end_ind], i[start_ind:end_ind], color='k')
    axes[1].plot(t[start_ind:end_ind], v[start_ind:end_ind], color='k')
    axes[0].set(ylabel="Current Injection (pA)")
    axes[1].set(ylabel="Membrane Potential (mV)", xlabel="seconds")
    
    return fig, i, v, t, sampling_rate

def plot_vc_sweep(data_set, sweep_number, start_ind=0, end_ind=-1):
    """
    
    Parameters
    ----------
    data_set : NwbDataSet
            
    sweep_number : int

    start_ind : int
        index at which to start plotting the trace
    end_ind : int
        index at which to stop plotting the trace
   
    Returns
    -------

    fig : matplotlib figure

    i : array (in Amps)
     
    v : array (in Volts)

    t : array (in seconds)

    sampling_rate : integer
    """
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(2, 1, sharex=True, figsize=(10,5))
    v, i, t, sampling_rate = get_vc_sweep_v_i_t_from_set(data_set, sweep_number)
    i = i*1E12 # pA
    v = v*1E3 # mV
    axes[0].plot(t[start_ind:end_ind], v[start_ind:end_ind], color='k')
    axes[1].plot(t[start_ind:end_ind], i[start_ind:end_ind], color='k')
    axes[0].set(ylabel="Voltage (mV)")
    axes[1].set(ylabel="Current Response (pA)", xlabel="seconds")
    
    return fig, i, v, t, sampling_rate


def get_nwb_data(path):
    """
    
    Parameters
    ----------
    path : linux friendly path to NWB file
            
    Returns
    -------
    acq : HDF5 object with acquisition data

    """
    import h5py
    with h5py.File(path) as f:
        f = h5py.File(path)
        acq = f['acquisition']
    return acq

def calculate_wc_duration(acq):
    """
    
    Parameters
    ----------
    acq : HDF5 object with acquisition data 

    Returns
    -------
    wc_duration : time (in seconds) between BREAKN (or CELLATT) sweep and EXPTGGAEND 
                    (or whatever is the last sweep in the experiment)

    """
    from natsort import natsorted
    sweep_names = natsorted(acq['timeseries'].keys(), reverse=True)
    end_times = []; start_times = []
    for sw in sweep_names:
        if acq['timeseries'][sw]['aibs_stimulus_description'][0].startswith("EXTPGGAEND"):
            end_times.append(acq['timeseries'][sw]['starting_time'][0])
        if acq['timeseries'][sw]['aibs_stimulus_description'][0].startswith(("EXTPCllATT", "EXTPBREAKN")):
            start_times.append(acq['timeseries'][sw]['starting_time'][0])

    if len(end_times) == 0:
        end_times.append(acq['timeseries'][sweep_names[0]]['starting_time'][0])

    wc_duration = max(end_times) - max(start_times)
    
    return wc_duration


def get_wc_duration(filepath):
    """
    
    Parameters
    ----------
    filepath : path to NWB file with Ephys experiment


    Returns
    -------            
    wc_duration : time (in seconds) between BREAKN (or CELLATT) sweep and EXPTGGAEND 
                    (or whatever is the last sweep in the experiment)

    """
    from single_cell_ephys.lims_funcs import linux_to_windows
    cleanpath = linux_to_windows(filepath)
    try:
        acq = get_nwb_data(cleanpath)
        wc_duration = calculate_wc_duration(acq)
    except:
        return None
    return wc_duration




