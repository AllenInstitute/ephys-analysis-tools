import matplotlib.pyplot as plt
from met_funcs import get_cc_sweep_v_i_t_from_set


def pretty_tick_labels(ax, fs=16, xrot=0, yrot=0):
    """
    Edits fontsize and rotation of x and y tick labels.
    
    Parameters
    ----------
    ax : matplotlib figure axes handle

    fs: int
        fontsize (default is 16)
    xrot : int
    	angle of text rotation for x-axis (default is 0)
	yrot : int 
		angle of text rotation for y-axis (default is 0)
   
    Returns
    -------

    ax: matplotlib figure axes handle

    """
    for tick in ax.xaxis.get_major_ticks():
        tick.label.set_fontsize(fs)
        tick.label.set_rotation(xrot)
    for tick in ax.yaxis.get_major_ticks():
        tick.label.set_fontsize(fs)
        tick.label.set_rotation(yrot)
    return ax


def get_half_height(thresh, peak):
    return (peak - thresh) / 2. + thresh

def plot_spike_0(cell, data_set, sweep_number, spike_info=None, start_ind=0, end_ind=-1):
    """
    Plots 1st spike of a sweep from NWB file.
    
    Parameters
    ----------
    cell : 

    data_set : NwbDataSet
            
    sweep_number : int

    spike_info :

        (default is None)
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
    import matplotlib.ticker as ticker
    
    fig, axes = plt.subplots(1, 1, sharex=True, figsize=(15,10))
    i, v, t, sampling_rate = get_cc_sweep_v_i_t_from_set(data_set, sweep_number)
    i = i*1E12 # pA
    v = v*1E3 # mV
    t = t*1E3 # ms
    
    if spike_info != None:
        ms=10; fs=12
        start_ind = spike_info["threshold_index"] - 5
        end_ind = spike_info["trough_index"] + 5   
        axes.plot(t[start_ind:end_ind], v[start_ind:end_ind], color='k')
        for idx in ["threshold_index", "peak_index", "trough_index"]:
            axes.plot(t[spike_info[idx]], v[spike_info[idx]], marker='o', markersize=ms, color='b')
            axes.annotate('%0.2f' %v[spike_info[idx]], xy=(t[spike_info[idx]-3], v[spike_info[idx]]+2), fontsize=fs)
        axes.axhline(y=get_half_height(spike_info["threshold_v"], spike_info["peak_v"]), ls="--", color="g")
        axes.axhline(y=get_half_height(spike_info["peak_v"], spike_info["trough_v"]), ls="--", color="orange")

        plt.title("%s   Sweep %d\n Spike Width: %0.3f ms" %(cell, sweep_number, spike_info["width"]*1E3)) # ms 
        

    else:
        axes.plot(t[start_ind:end_ind], v[start_ind:end_ind], color='k')
    
    if axes.axis()[1] - axes.axis()[0] < 2:
        xloc = ticker.MultipleLocator(base=0.05)
        yloc = ticker.MultipleLocator(base=5)
        axes.xaxis.set_major_locator(xloc)
        axes.yaxis.set_major_locator(yloc)
    
    axes.set_xlabel("ms", fontsize=fs)
    axes.set_ylabel("Membrane Potential (mV)", fontsize=fs)
    axes.grid(color='lightgray', linestyle='-', linewidth=1)
    axes = pretty_tick_labels(ax=axes, fs=fs, xrot=45, yrot=0)
    axes.set_xlim([t[start_ind], t[int(min(end_ind, start_ind + 2*sampling_rate/1000.))]])
    
    return fig


def plot_pretty_hist(data, x, hue, bins, color_d, cdf=True, fs=14, title='Cumulative step histogram', ax=None):
    """
    Plots a cumulative (or regular) step histogram.
    
    Parameters
    ----------
    data : pandas DataFrame

    x : string
        (a numerical column in data)
            
    hue : string or None
        (a categorical column in data, used for grouping)

    bins : array
        (bins for column x)

    color_d : dict
        dictionary mapping hues to colors

    cdf : boolean (default True)
        indicates whether to plot cdf (or pdf)

    fs : int (default 14)
        fontsize

    title : string (default 'Cumulative step histogram')

    ax : matplotlib figure axes

    Returns
    -------

    ax : matplotlib figure axes
    
    """
    if ax==None:
        fig, ax = plt.subplots(figsize=(6, 4))
    

    for q in color_d.keys():
        n, bins, patches = ax.hist(data[data[hue]==q][x].dropna(), 
                               bins, normed=1, histtype='step',
                               cumulative=cdf, label=q, color=color_d[q], linewidth=2)
    # tidy up the figure
    ax.grid(True)
    ax.legend(loc='right', fontsize=fs)
    ax.set_title(title, fontsize=fs)
    ax.set_xlabel(x, fontsize=fs)
    ax.set_ylabel('Likelihood of occurrence', fontsize=fs)

    plt.tight_layout()
    return ax