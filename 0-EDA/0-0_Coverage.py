import os
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import matplotlib.dates as mdates
from matplotlib import ticker

data_dir = "C:/Users/niek2/AppData/Roaming/JetBrains/DataSpell2024.3/projects/RelaxXL/coverage_test/data"

# List of your HR CSV files


# Create a dictionary to group segments by calendar date.
# Each key is a date (as a datetime.date object) and its value is a list of segments.
# A segment is a tuple: (start_seconds, duration_in_seconds)
segments_by_date = {}

# Process each file
for file in os.listdir(data_dir):
    filepath = f"{data_dir}/{file}"
    # Extract the Unix timestamp from the filename (without extension)
    start_ts = int(os.path.splitext(file)[0])
    # Read the CSV (assuming a single column with no header)
    df = pd.read_csv(filepath, header=None)
    n = len(df) - 2  # number of seconds/data points

    # Convert the timestamp to a datetime to get the start-of-day and time-of-day
    start_dt = datetime.datetime.fromtimestamp(start_ts)
    start_seconds = start_dt.hour * 3600 + start_dt.minute * 60 + start_dt.second
    # Calculate the end time in seconds from midnight
    end_seconds = start_seconds + n - 1

    # The first segment belongs to the start date.
    date1 = start_dt.date()
    if end_seconds < 86400:
        # All data fits within the same day.
        segment = (start_seconds, n)
        segments_by_date.setdefault(date1, []).append(segment)
    else:
        # Data wraps past midnight.
        # First segment: from start_seconds to midnight
        first_duration = 86400 - start_seconds
        segment1 = (start_seconds, first_duration)
        segments_by_date.setdefault(date1, []).append(segment1)

        # Second segment: from midnight onward belongs to the next day.
        second_duration = end_seconds - 86400 + 1
        date2 = date1 + datetime.timedelta(days=1)
        segment2 = (0, second_duration)
        segments_by_date.setdefault(date2, []).append(segment2)

# Sort the dates to plot in chronological order
sorted_dates = sorted(segments_by_date.keys())

# Create the plot: one row per calendar day
fig, ax = plt.subplots(figsize=(10, len(sorted_dates)*0.6 + 2))
y_positions = range(len(sorted_dates))
y_labels = [date.strftime("%Y-%m-%d") for date in sorted_dates]

for i, d in enumerate(sorted_dates):
    segments = segments_by_date[d]
    # Plot all segments for the day using broken_barh.
    ax.broken_barh(segments, (i - 0.3, 0.6), facecolors='skyblue')
    # Optionally, mark start and end boundaries with red dots.
    for seg in segments:
        start, width = seg
        end = start + width - 1
        ax.plot([start, end], [i, i], "o", color="red")

ax.set_yticks(y_positions)
ax.set_yticklabels(y_labels)
ax.set_xlim(0, 86400)
# Formatter to display the x-axis in HH:MM format
ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f"{int(x//3600):02d}:{int((x % 3600)//60):02d}"))
plt.xlabel("Time of Day")
plt.title("Daily Data Coverage")
plt.tight_layout()
plt.show()