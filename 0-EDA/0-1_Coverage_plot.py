import os
import zipfile
import pandas as pd
import matplotlib.pyplot as plt
import datetime
from matplotlib import ticker
from multiprocessing import Pool

TOTAL_SECONDS_WEEK = 604800  # Total seconds in one week

# Polling rates (in Hz) for each measurement.
POLLING_RATES = {
    "ACC.csv": 32,
    "EDA.csv": 4,
    "BVP.csv": 64,
    "TEMP.csv": 4,
    "IBI.csv": 0.33,  # Ensure filename matches exactly (case sensitive)
    "HR.csv": 1,
}

class HRDataLoader:
    def __init__(self, data_dir):
        self.data_dir = data_dir

    def load_week_data(self, week_folder, patient_id):
        """
        Loads data from zipped CSV files located in:
            <data_dir>/<patient_id>/<week_folder>
        Returns a dictionary mapping a timestamp (datetime) to a dictionary of CSV DataFrames.
        """
        week_data = {}
        week_path = os.path.join(self.data_dir, patient_id, week_folder)
        for file in os.listdir(week_path):
            file_path = os.path.join(week_path, file)
            with zipfile.ZipFile(file_path, 'r') as archive:
                # Assumes filename format: "<timestamp>_....zip"
                timestamp = datetime.datetime.fromtimestamp(int(file.split("_")[0]))
                week_data[timestamp] = {}
                for name in archive.namelist():
                    if name.endswith('.csv'):
                        with archive.open(name) as f:
                            if f.read(1):  # Check non-empty
                                f.seek(0)
                                df = pd.read_csv(f)
                                week_data[timestamp][name] = df
        return week_data

def compute_daily_segments(week_data):
    """
    Computes daily segments (start time and duration in seconds) from HR.csv data.
    If the data spans past midnight, it splits the segment so that the part after midnight
    is associated with the next day.

    Returns:
        segments_by_date: dict mapping each calendar date (datetime.date) to a list of segments.
                          Each segment is a tuple (start_seconds, duration).
    """
    segments_by_date = {}
    for timestamp, files in week_data.items():
        if "HR.csv" not in files:
            continue
        df = files["HR.csv"]
        # Adjust number of data points if needed (here -2 as in your original code)
        n = len(df) - 2
        start_dt = timestamp
        start_seconds = start_dt.hour * 3600 + start_dt.minute * 60 + start_dt.second
        end_seconds = start_seconds + n - 1
        date1 = start_dt.date()
        if end_seconds < 86400:
            segment = (start_seconds, n)
            segments_by_date.setdefault(date1, []).append(segment)
        else:
            first_duration = 86400 - start_seconds
            segment1 = (start_seconds, first_duration)
            segments_by_date.setdefault(date1, []).append(segment1)
            second_duration = end_seconds - 86400 + 1
            date2 = date1 + datetime.timedelta(days=1)
            segment2 = (0, second_duration)
            segments_by_date.setdefault(date2, []).append(segment2)
    return segments_by_date

def generate_and_save_coverage_plot(args):
    """
    Loads week data for one patient and one week folder, computes daily coverage segments,
    creates a coverage plot, and saves it as a PNG file.

    args: tuple (data_dir, week_folder, patient, output_folder)
    """
    data_dir, week_folder, patient, output_folder = args
    loader = HRDataLoader(data_dir)
    try:
        week_data = loader.load_week_data(week_folder, patient)
    except Exception as e:
        return f"Patient {patient} week {week_folder}: error loading data: {e}"

    segments = compute_daily_segments(week_data)

    # Create the plot.
    if not segments:
        fig, ax = plt.subplots(figsize=(10, 2))
        ax.text(0.5, 0.5, "No Data", ha='center', va='center', fontsize=20)
        ax.set_axis_off()
    else:
        sorted_dates = sorted(segments.keys())
        fig, ax = plt.subplots(figsize=(10, len(sorted_dates)*0.6 + 2))
        y_positions = range(len(sorted_dates))
        y_labels = [date.strftime("%Y-%m-%d") for date in sorted_dates]

        for i, date in enumerate(sorted_dates):
            segs = segments[date]
            ax.broken_barh(segs, (i - 0.3, 0.6), facecolors='skyblue')
            for seg in segs:
                start, width = seg
                end = start + width - 1
                ax.plot([start, end], [i, i], "o", color="red")

        ax.set_yticks(y_positions)
        ax.set_yticklabels(y_labels)
        ax.set_xlim(0, 86400)
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f"{int(x//3600):02d}:{int((x % 3600)//60):02d}"))
        ax.set_xlabel("Time of Day")
        ax.set_title(f"Daily Data Coverage - {patient} {week_folder}")
        plt.tight_layout()

    # Ensure output folder exists.
    os.makedirs(output_folder, exist_ok=True)
    output_file = os.path.join(output_folder, f"{patient}_{week_folder}_coverage.png")
    fig.savefig(output_file)
    plt.close(fig)
    return f"Saved coverage plot for {patient} {week_folder} to {output_file}"

def compute_coverage_plots_all_patients_mp(data_dir, week_folders, output_folder):
    """
    Loops over all patient directories in data_dir and for each week folder provided,
    generates and saves the coverage plots using multiprocessing.

    Returns:
        List of result messages.
    """
    patient_dirs = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]
    args_list = []
    for patient in patient_dirs:
        for week_folder in week_folders:
            args_list.append((data_dir, week_folder, patient, output_folder))
    with Pool() as pool:
        results = pool.map(generate_and_save_coverage_plot, args_list)
    return results

def main():
    data_dir = "C:/Users/niek2/OneDrive - UMCG/Data/Onderzoeksdata E4"
    output_folder = "plots"
    week_folders = ["Week1", "Week2"]

    results = compute_coverage_plots_all_patients_mp(data_dir, week_folders, output_folder)
    for res in results:
        print(res)

if __name__ == "__main__":
    main()
