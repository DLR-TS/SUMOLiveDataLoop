#!/usr/bin/env python
# SUMO Live Data Loop
# Copyright (C) 2007-2024 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# SPDX-License-Identifier: EPL-2.0

# @file    resultEvaluation.py
# @author  Yun-Pang Floetteroed
# @date    2025-11-17

"""
Evaluate the online flow calibration with GEH
 - calculate GEH and percentage with GEH less than or equal to 5
 - plot a daily GEH-heatmap (x: time interval, y: detected road segment)
 - all output files are under plots/YYYYMMDD/
Items to define/adjust:
    - data and output directories
    - time interval and units (currently 5 min; flow: veh/h; speed: km/h)
    - related prefix and suffix for output filenames
"""

import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
import numpy as np
from matplotlib.colors import LinearSegmentedColormap, Normalize  #BoundaryNorm # TwoSlopeNorm
import matplotlib.patches as Patch

def geh(sim, detected):
    """ 
        GEH values: np.nan for undefined, 0 if both flows are 0
    """
    sim = np.asarray(sim, dtype=float)
    detected = np.asarray(detected, dtype=float)
    
    total = sim + detected
    
    # Initialize output with NaN
    geh_value = np.full_like(total, np.nan, dtype=float)
    
    # Valid mask: denominator > 0
    valid_mask = total > 0
    geh_value[valid_mask] = np.sqrt(2 * (sim[valid_mask] - detected[valid_mask])**2 / total[valid_mask])
    
    # set GEH = 0 if both flows are 0
    zero_mask = (sim == 0) & (detected == 0)
    geh_value[zero_mask] = 0
    
    return geh_value

# Defube the data directories
DATA_FOLDER = "leipzig/simdata_2"      # Folder containing compare*.txt files
PLOT_ROOT = "data_comparison/plots_test"     # Root folder for all generated plots
os.makedirs(PLOT_ROOT, exist_ok=True)

# Choose the attributes for real flow and speed
real_flow = "loop-flow"
real_speed = "loop-speed"
interval_minutes = 5
suffix = "_5min"
frequency = "5min"

# Read all input files
files = glob.glob(os.path.join(DATA_FOLDER, "*.txt"))
daily_data = {}
if len(files) == 0:
    print('**** No input files are found. **** ')

# Create a GEH colormap (e.g., smooth gradient from green to orange to red)
colors = [
    (0.00, 0.55, 0.00),  # dark green
    (0.13, 0.55, 0.13),  # forest green
    (1.00, 0.80, 0.00),  # gold yellow
    (1.00, 0.75, 0.20),  # warm orange
    (1.00, 0.45, 0.00),  # dark orange
    (1.00, 0.00, 0.00)   # red
]

positions = [0/10, 4.5/10, 5.3/10, 6.8/10, 8/10, 10/10]

for f in files:
    if "compare" not in f:
        print('the input file is not compare*.txt but ' %f)

    with open(f, "r") as fh:
        timestamp_line = fh.readline().strip()  # YYYYMMDDHHMMSS
        date_str = timestamp_line[:8]
        time_str = timestamp_line[8:]

    # Read the rest of the file as tab-separated
    df = pd.read_csv(f, sep=r"\s+", engine="python",skiprows=1,na_values=["None", "NULL"])
    
    # Convert numeric columns safely
    numeric_cols = [
        "loop-flow", "loop-speed",
        "fusion-flow", "fusion-speed",
        "simulation-flow", "simulation-speed",
        "prediction-flow", "prediction-speed"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    # Add datetime column
    df["datetime"] = pd.to_datetime(date_str + time_str, format="%Y%m%d%H%M%S")
    
    #print(df.head(5))

    # Collect data per day
    daily_data.setdefault(date_str, []).append(df)

# PLOTTING per day and then per section
for date_str, dfs in daily_data.items():
    # Create daily plot directory
    day_plot_dir = os.path.join(PLOT_ROOT, date_str)
    os.makedirs(day_plot_dir, exist_ok=True)
    
    # Create daily section directory
    section_plot_dir = os.path.join(day_plot_dir, "sections")
    os.makedirs(section_plot_dir, exist_ok=True)
    
    # Open a log file to save extreme data
    log_file = os.path.join(day_plot_dir,"0_log_5min.txt")
    loghour_file = os.path.join(day_plot_dir,"0_log_hour.txt")

    # Read data with pandas
    day_df = pd.concat(dfs).sort_values("datetime")

    # Keep only rows where loop-flow and loop-speed are valid
    day_df = day_df.dropna(subset=["loop-flow", "loop-speed"])
    
    # Convert datetime to hour for x-axis; only hour
    day_df['hour'] = day_df['datetime'].dt.hour # + day_df['datetime'].dt.minute / 60
    
    # Write out data when real flow > 2500
    high_flow_df = day_df[day_df[real_flow] > 2500]
    
    # Write out data when real flow >3500 and sim flow = 0
    check_flow_df = day_df[(day_df[real_flow] > 3500) & (day_df["simulation-flow"] == 0)]

    # sort and write out the selected data
    high_flow_df = high_flow_df.sort_values(by=["section-id", "datetime"])
    check_flow_df = check_flow_df.sort_values(by=["section-id", "datetime"])
    high_flow_df.to_csv(os.path.join(day_plot_dir,"0_high_flows_over_2500.csv"), index=False)
    check_flow_df.to_csv(os.path.join(day_plot_dir,"0_real_flows_over_3500_and_sim_flows_0.csv"), index=False)
    
    # Aggregate flows/speeds to 1-hour sums per section for GEH and plotting ---
    day_df = day_df.set_index("datetime")

    # Calculate relative errors
    day_df = day_df.reset_index()
    # handle zero and very small values for real_flow
    day_df = day_df.copy()
    small_threshold = 1
    day_df.loc[day_df[real_flow] < small_threshold, real_flow] = np.nan
    
    # 5-min
    day_df["flow_error"] = (day_df["simulation-flow"] - day_df[real_flow])/day_df[real_flow]
    day_df["speed_error"] = (day_df["simulation-speed"] - day_df[real_speed])/day_df[real_speed]

    # Calculate GEH for flow (5-min scaled) 
    day_df["GEH"] = geh(day_df["simulation-flow"], day_df[real_flow])

    # Save all calcuated 5-min based GEH into a csv file 
    for i, outf_df in enumerate([day_df]):#, hourly
        temp_df = outf_df[["section-id", "datetime", "GEH", "simulation-flow", real_flow]]

        # sortieren nach sec_id und datetime
        temp_df = temp_df.sort_values(by=["section-id", "datetime"])

        # als CSV speichern
        temp_df.to_csv(os.path.join(day_plot_dir, "0_GEH_all_5min.csv"), float_format='%.2f', index=False)
    
    # compute per-day GEH percentages
    # Drop NA values before counting; considering 5-min data in unit (veh/h)
    valid_geh_flow  = day_df["GEH"].dropna()

    if len(valid_geh_flow) > 0:
        pct_day_geh5_flow = (valid_geh_flow <= 5).mean() * 100   # get percentage
    else:
        pct_day_geh5_flow = np.nan

    # Prepare daily summary DataFrame
    day_summary_df = pd.DataFrame({
        "date": [date_str],
        "pct_GEH_less_than_5_flow": [pct_day_geh5_flow],
        "num_records_flow": [len(valid_geh_flow)]
    })

    # Save to CSV (one file per day)
    day_summary_path = os.path.join(day_plot_dir, "0_GEH_summary_day.csv")
    day_summary_df.to_csv(day_summary_path, float_format='%.2f', index=False)

    # Flow comparison + deviation (sum over all edges)
    # Sum flows over all section-ids per timestamp for the whole day (using flows (veh/h) according to 5-min data)
    df_sum = day_df.groupby("datetime").agg({
        real_flow: "sum",
        "simulation-flow": "sum",
        real_speed: "mean",
        "simulation-speed": "mean"
    }).reset_index()
    
    df_sum["flow_error"] = (df_sum["simulation-flow"] - df_sum[real_flow])/df_sum[real_flow]  # the original unit is veh/h
    df_sum["speed_error"] = (df_sum["simulation-speed"] - df_sum[real_speed])/df_sum[real_speed]

    fig, ax = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    ax[0].plot(df_sum["datetime"], df_sum[real_flow], label="Detected Flow", linewidth=2, marker='o', markersize=6)
    ax[0].plot(df_sum["datetime"], df_sum["simulation-flow"], label="Simulated Flow", linestyle="--", marker='x', markersize=6)
    ax[0].set_title(f"Ttoal Flow Comparison (summed over all sections with active detectors)– {date_str}")
    ax[0].set_ylabel("Flow (veh/h)")
    ax[0].grid(True)
    ax[0].legend()

    ax[1].plot(df_sum["datetime"], df_sum["flow_error"], color="red")
    ax[1].axhline(0, color="black", linewidth=1)
    ax[1].set(ylim=(-0.5,0.5))
    ax[1].set_title("Relative Flow Deviation")
    ax[1].set_xlabel("Time (Month-Day-Hour)")
    ax[1].set_ylabel("Relative Flow Deviation")
    ax[1].grid(True)

    plt.tight_layout()
    plt.savefig(os.path.join(day_plot_dir, "flow_comparison _and_deviation%s.png" %suffix))
    plt.close()
    
    # Speed comparison + deviation (mean over all edges)
    fig, ax = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    ax[0].plot(df_sum["datetime"], df_sum[real_speed], label="Detected Speed", linewidth=2, marker='o', markersize=6)
    ax[0].plot(df_sum["datetime"], df_sum["simulation-speed"], label="Simulated Speed", linestyle="--", marker='x', markersize=6)
    ax[0].set_title(f"Average Speed Comparison (averaged over all sections with active detectors) – {date_str}")
    ax[0].set_ylabel("Speed (km/h)")
    ax[0].grid(True)
    ax[0].legend()

    ax[1].plot(df_sum["datetime"], df_sum["speed_error"], color="green")
    ax[1].axhline(0, color="black", linewidth=1)
    ax[1].set(ylim=(-0.5,0.5))
    ax[1].set_title("Relative Speed Deviation")
    ax[1].set_xlabel("Time (Month-Day-Hour)")
    ax[1].set_ylabel("Relative Speed Deviation")
    ax[1].grid(True)

    plt.tight_layout()
    plt.savefig(os.path.join(day_plot_dir, "speed_comparison _and_deviation%s.png" %suffix))
    plt.close()

    # Flow GEH heatmap (section-id vs time)
    # Full time range for the day (5-minute interval)
    full_times = pd.date_range(start=day_df["datetime"].min().normalize(), end=day_df["datetime"].min().normalize() + pd.Timedelta(days=1), freq = frequency)
    
    pivot_flow = day_df.pivot_table(index="section-id", columns="datetime", values="GEH")
    
    # reindex columns to full time range so missing times appear as NaN
    pivot_flow = pivot_flow.reindex(columns=full_times)

    fig, ax = plt.subplots(figsize=(14,8))
    # heatmap
    cmap = LinearSegmentedColormap.from_list("geh_orange_red", list(zip(positions, colors))).copy()
    norm = Normalize(vmin=0, vmax=10)
    cmap.set_bad(color='lightgrey')  # color for NaN values
           
    im = ax.imshow(pivot_flow.values, aspect="auto", cmap=cmap, norm=norm)

    # Create the colorbar
    cb = fig.colorbar(im, ax=ax)
    # Add tick at 5, 0 and 10
    cb.set_ticks(np.arange(0, 11, 1))  # ticks from 0 to 10, step 1
    cb.set_label("GEH")
    
    # fix y-axis (section_ids)
    ax.set_yticks(np.arange(len(pivot_flow.index)))
    ax.set_yticklabels(pivot_flow.index)
    ax.invert_yaxis()
    # fix x-axis (datetimes)
    # Create tick times at 2-hour intervals
    xtick_times = pd.date_range(start=full_times.min(), end=full_times.max(), freq="2h")

    # Convert times to positions (indices)
    xtick_pos = [full_times.get_loc(t) for t in xtick_times]

    ax.set_xticks(xtick_pos)
    ax.set_xticklabels([t.strftime("%H:%M") for t in xtick_times], ha='right') #, rotation=45
    ax.set_title(f"GEH Heatmap – {date_str}")
    ax.set_xlabel("Time")
    ax.set_ylabel("section-id")
    plt.savefig(os.path.join(day_plot_dir, "GEH_heatmap_%s.png" %suffix))
    plt.close()

    print(f"The daily plots and GEH-outputs saved to: {day_plot_dir}")

    # Loop over each section_id and calcuate GEH and save the result
    section_records = []
    for sec_id, sec_df in day_df.groupby("section-id"):
        valid_geh_flow  = sec_df["GEH"].dropna()
        
        pct_sec_geh5_flow  = (valid_geh_flow <= 5).mean() * 100 if len(valid_geh_flow) > 0 else np.nan

        section_records.append({
            "date": date_str,
            "section_id": sec_id,
            "pct_GEH_less_than_5_flow": f"{pct_sec_geh5_flow:.2f}",
            "num_records_flow": len(valid_geh_flow)
        })
    
        fig, axes = plt.subplots(4, 1, figsize=(12, 12), sharex=True)

        # Upper 1: Flow comparison
        axes[0].plot(sec_df["datetime"], sec_df['loop-flow'], label="Detected Flow", linewidth=2, marker='o', markersize=4)
        axes[0].plot(sec_df["datetime"], sec_df['simulation-flow'], linestyle="--", label="Simulated Flow", linewidth=2, marker='x', markersize=4)
        axes[0].set_ylabel("Flow (veh/h)")
        axes[0].set_title(f"Section {sec_id} – Flow & Speed Comparison – {date_str}")
        axes[0].legend()
        axes[0].grid(True)

        # Upper 2: GEH # Flow deviation
        axes[1].plot(sec_df["datetime"], sec_df['GEH'], color="red", label="GEH", linewidth=2)
        axes[1].set_ylabel("GEH")            
        #axes[1].set(ylim=(0,10))
        axes[1].legend()
        axes[1].grid(True)

        # Lower 1: Speed comparison
        axes[2].plot(sec_df["datetime"], sec_df['loop-speed'], label="Detected Speed", linewidth=2,  marker='o', markersize=4)
        axes[2].plot(sec_df["datetime"], sec_df['simulation-speed'], linestyle="--", label="Simulated Speed", linewidth=2, marker='x', markersize=4)
        axes[2].set_ylabel("Speed (km/h)")
        axes[2].legend()
        axes[2].grid(True)

        # Lower 2: Speed deviation
        axes[3].plot(sec_df["datetime"], sec_df['speed_error'], color="green", label="Relative Speed Deviation", linewidth=2)
        axes[3].set_xlabel("Datetime (Month-Day-Hour)")
        axes[3].set_ylabel("Relative Speed Deviation")
        axes[3].set(ylim=(-0.6,0.6))
        axes[3].legend()
        axes[3].grid(True)

        plt.tight_layout()
        plt.savefig(os.path.join(section_plot_dir, f"{date_str}_section_{sec_id}_4plots%s.png" %suffix))
        plt.close()

    section_summary_df = pd.DataFrame(section_records)
    section_summary_path = os.path.join(day_plot_dir, "0_GEH_summary_sections_%s.csv" %suffix)
    section_summary_df.to_csv(section_summary_path, float_format='%.2f', index=False)

    print(f"All section-plots saved to: {section_plot_dir}")
