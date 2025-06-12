import pandas as pd
from datetime import datetime

SESSION_CSV = "C:/Users/niek2/Documents/Afstudeerstage/Output van RXLDBC/stats_session.csv"

session_df = pd.read_csv(SESSION_CSV, delimiter=',', header=0)

Relax_start = session_df["relax_session_start"]
Relax_end = session_df["relax_session_end"]

EDA_Before = session_df["EDA_SCR_peaks_before"]
EDA_During = session_df["EDA_SCR_peaks_during"]
EDA_After = session_df["EDA_SCR_peaks_after"]

dataframes = []

# Calculate SCR per minute for each session

for start, end, before, during, after in zip(Relax_start, Relax_end, EDA_Before, EDA_During, EDA_After):
    duration = (datetime.strptime(end, "%Y-%m-%d %H:%M:%S") - datetime.strptime(start, "%Y-%m-%d %H:%M:%S")).total_seconds() / 60  # Convert to minutes
    scr_per_minute_before = before / 5
    scr_per_minute_during = during / duration if duration > 0 else 0
    scr_per_minute_after = after / 5

    new_scr = {
        "SCR_per_minute_before": scr_per_minute_before,
        "SCR_per_minute_during": scr_per_minute_during,
        "SCR_per_minute_after": scr_per_minute_after
    }

    df = pd.DataFrame([new_scr])
    dataframes.append(df)

# Concatenate all dataframes into one
session_scr_df = pd.concat(dataframes, ignore_index=True)
# Add the new SCR per minute columns to the original session_df

session_scr_df.to_csv("new_scr.csv", header=True)
