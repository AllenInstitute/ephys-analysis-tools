import os
import time
import pprint
from datetime import datetime
import numpy as np
import pandas as pd
import collections

import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import matplotlib.ticker as ticker
from matplotlib.dates import MonthLocator, DayLocator, HourLocator, DateFormatter, drange,  WeekdayLocator, MO, TU, WE, TH, FR, SA, SU


from bokeh.plotting import figure#, show, output_file
from bokeh.models import HoverTool, Legend
from bokeh.io import export_png


def make_unstacked_day_frequency_columns(df, myvar, date):
    day_summary = pd.DataFrame({"count": df.groupby([date, myvar]).size()}).reset_index()
    day_summary["frequency"] = day_summary.groupby(by=date)["count"].transform(lambda x: 100*(x/x.sum()))
    unstacked_day = pd.DataFrame(columns={date})
    day_grouped = day_summary.groupby(by=myvar)
    for call, day in day_grouped:
        unstacked_day = unstacked_day.merge(day[[date, "count", "frequency"]], how="outer", on=date)
        unstacked_day.rename(columns={"count": "Count %s" %call, "frequency": "Frequency %s" %call}, inplace=True)
    unstacked_day.replace(np.nan, 0, inplace=True)
    day_combined_df = df.merge(unstacked_day, how="left", on=date)
    return day_combined_df

def make_custom_legend(colors, labels):
    handles = [];
    for i, c in enumerate(colors):
        handles.append(mlines.Line2D([], [], color=c, marker="s", markersize=20,markeredgecolor="black", markeredgewidth=1, linewidth=0, label=labels[i]))
    labels = [h.get_label() for h in handles] 
    
    return handles, labels

def plot_stack_ax(df, date, color_dict, ax, fs=18, title=None):   
    df.sort_values(by=date, axis=0, ascending=True, inplace=True)

    # Stacked Plots with QC calls PER DAY
    frequency_cols = df.columns[df.columns.str.contains("Frequency")]
    labels = [x[10:] for x in frequency_cols]
    colors = [color_dict[x] for x in labels]

    frequency_data = [df[x] for x in frequency_cols]
    y = np.row_stack(frequency_data)
    x = [d.to_pydatetime() for d in df[date]]
        
    ax.set_ylabel("daily frequency", fontdict={"size":fs})
    ax.tick_params(axis='both', labelsize=fs)
    ax.xaxis.set_major_locator(WeekdayLocator(byweekday=MO, interval=2))
    #ax.xaxis.set_major_locator(DayLocator(bymonthday=1, interval=1))

    ax.xaxis.set_major_formatter(DateFormatter('%m-%d-%y'))

    ax.set_ylim([0, 100])
    ax.set_xlim([min(x), max(x)])
    ax.tick_params(length=10, width=2, which="major", direction="out", 
                                 bottom=True, top=False, left=True, right=False)
    ax.stackplot(x, y, baseline="zero", labels=labels,
                                colors=colors)
    #ax.axvline(pd.to_pydatetime("02-27-17"), color="#ce1256", linestyle='--', 
    #                          lw=4, alpha=1.)  # Date of Tolias lab visit
        
    for tick in ax.xaxis.get_major_ticks():
        
        tick.label.set_fontsize(fs)
        tick.label.set_rotation(45)
    if title:
        ax.set_title(title, fontdict={"fontsize":fs})
    plt.axis('tight')
    ax.legend(loc="upper right", bbox_to_anchor=(1.3, 1.1), fontsize=fs)

def plot_pct_nucleated(df, date, ax, fs=18, title=None):   
    df.sort_values(by=date, axis=0, ascending=True, inplace=True)

    # Stacked Plots with QC calls PER DAY
    frequency_col = "Frequency Nucleated"
    frequency_data = df[frequency_col]
    y = frequency_data
    x = [d.to_pydatetime() for d in df[date]]
    
    ax.set_ylabel("% nucleated", fontdict={"size":fs})
    ax.tick_params(axis='both', labelsize=fs)
    ax.xaxis.set_major_locator(WeekdayLocator(byweekday=MO, interval=2))
    #ax.xaxis.set_major_locator(DayLocator(bymonthday=1, interval=1))

    ax.xaxis.set_major_formatter(DateFormatter('%m-%d-%y'))

    ax.set_ylim([0, 100])
    ax.set_xlim([min(x), max(x)])
    ax.tick_params(length=10, width=2, which="major", direction="out", 
                                 bottom=True, top=False, left=True, right=False)
    ax.bar(x, y, color="#d01c8b")

    for tick in ax.xaxis.get_major_ticks():
        tick.label.set_fontsize(fs)
        tick.label.set_rotation(45)
    if title:
        ax.set_title(title, fontdict={"fontsize":fs})
    plt.axis('tight')

def fill_in_dates(data_df, date, data_dates, start_date):
    all_dates = set(pd.date_range(start_date, datetime.today()).tolist())
    missing_dates = list(all_dates - data_dates)
    missing_date_df = pd.DataFrame({date:missing_dates})
    filled_df = pd.concat([data_df, missing_date_df])
    return filled_df

def annotate_mod_date(ax, mod_date):
    ax.text(0, 1.2, "Analyzed on: %s" %mod_date,
        verticalalignment='top', horizontalalignment='left',
        transform=ax.transAxes, color='black', fontsize=16)

def plot_mpl_post_patch_summary(df, date_col, mod_date, colors, fs, dirs, fig_name):
    fig, ax = plt.subplots(2,1, sharex=True, sharey=True, figsize=(24,7))
    frequency_cols = df.columns[df.columns.str.contains("Frequency")]
    
    plot_stack_ax(df, date_col, color_dict=colors, fs=fs, ax=ax[0], title="")
    plot_pct_nucleated(df, date_col, ax[1], fs=fs, title=None)
    annotate_mod_date(ax[0], mod_date)
    ax[0].legend(loc="upper right", bbox_to_anchor=(0.8, 1.4), fontsize=fs, ncol=len(colors)) 
    ax[1].grid(True, which='major', axis="y")

    plt.tight_layout()
    for plot_dir in dirs:
        plt.savefig(os.path.join(plot_dir, fig_name), dpi=300, bbox_inches="tight", transparent=True)