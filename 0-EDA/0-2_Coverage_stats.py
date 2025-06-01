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
    "IBI.csv": 0.33,
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
                            if f.read(1):  # Non-empty check
                                f.seek(0)
                                df = pd.read_csv(f)
                                week_data[timestamp][name] = df
        return week_data

def plot_hr_data(week_data, week_label):
    """
    Plots HR data (line plot) for each timestamp in the provided week data.
    """
    for timestamp, files in week_data.items():
        if "HR.csv" in files:
            df = files["HR.csv"]
            plt.figure()
            plt.plot(df)
            plt.title(f"Heart Rate Data for {week_label}\n{timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
            plt.xlabel("Time (s)")
            plt.ylabel("Heart Rate")
            plt.legend(["Heart Rate"])
            plt.ylim(0, 180)
            plt.show()

def compute_daily_segments(week_data):
    """
    Computes daily segments (start time and duration in seconds) from HR.csv data.
    If the data spans past midnight, the segment is split so that the part after midnight
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
        # Adjust the count if needed (here using -2 as in the original code)
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

def plot_daily_coverage(segments_by_date, title):
    """
    Plots a 24-hour coverage chart using a horizontal bar chart (broken_barh).
    Each calendar day is plotted on its own row.
    """
    sorted_dates = sorted(segments_by_date.keys())
    fig, ax = plt.subplots(figsize=(10, len(sorted_dates)*0.6 + 2))
    y_positions = range(len(sorted_dates))
    y_labels = [date.strftime("%Y-%m-%d") for date in sorted_dates]

    for i, date in enumerate(sorted_dates):
        segments = segments_by_date[date]
        ax.broken_barh(segments, (i - 0.3, 0.6), facecolors='skyblue')
        for seg in segments:
            start, width = seg
            end = start + width - 1
            ax.plot([start, end], [i, i], "o", color="red")

    ax.set_yticks(y_positions)
    ax.set_yticklabels(y_labels)
    ax.set_xlim(0, 86400)
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f"{int(x//3600):02d}:{int((x % 3600)//60):02d}"))
    plt.xlabel("Time of Day")
    plt.title(title)
    plt.tight_layout()
    plt.show()

def calculate_week_coverage(week_data, polling_rates):
    """
    Calculates the coverage percentage for each measurement in a given week.
    For each measurement, coverage is defined as:

        (Total data points recorded / (polling_rate * TOTAL_SECONDS_WEEK)) * 100

    If a measurement is absent for a timestamp, it contributes 0 data points.

    Returns:
        coverage: dict mapping measurement name to its coverage percentage.
    """
    expected_counts = {m: polling_rates[m] * TOTAL_SECONDS_WEEK for m in polling_rates}
    actual_counts = {m: 0 for m in polling_rates}

    for timestamp, measurements in week_data.items():
        for m in polling_rates:
            if m in measurements:
                df = measurements[m]
                actual_counts[m] += len(df)
    coverage = {}
    for m in polling_rates:
        coverage[m] = (actual_counts[m] / expected_counts[m]) * 100
    return coverage

def compute_coverage_for_patient(args):
    """
    Helper function for multiprocessing.
    Loads week data for one patient and computes coverage percentages.
    """
    data_dir, week_folder, patient = args
    loader = HRDataLoader(data_dir)
    print(f"Processing {patient}...")
    try:
        week_data = loader.load_week_data(week_folder, patient)
    except Exception as e:
        print(f"Error loading data for {patient}: {e}")
        week_data = {}
    coverage = calculate_week_coverage(week_data, POLLING_RATES)
    coverage["patient"] = patient
    return coverage

def compute_coverage_for_all_patients_mp(data_dir, week_folder):
    """
    Loops over all patient directories in data_dir, loads the specified week data,
    and calculates coverage percentages for all measurements using multiprocessing.
    Returns a pandas DataFrame with one row per patient.
    """
    patient_dirs = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]
    args_list = [(data_dir, week_folder, patient) for patient in patient_dirs]
    with Pool(processes=20) as pool:
        results = pool.map(compute_coverage_for_patient, args_list)
    df = pd.DataFrame(results)
    # Order columns: patient first, then sorted measurement names.
    cols = ["patient"] + sorted(list(POLLING_RATES.keys()))
    df = df[cols]
    return df

def main():
    data_dir = "C:/Users/niek2/OneDrive - UMCG/Data Empatica E4"

    # (Optional) Plot raw HR data and daily coverage for one patient, e.g., "F001"
    loader = HRDataLoader(data_dir)
    week1_F001 = loader.load_week_data("Week1", "F001")
    week2_F001 = loader.load_week_data("Week2", "F001")
    plot_hr_data(week1_F001, "Week 1 (Patient F001)")
    plot_daily_coverage(compute_daily_segments(week1_F001), "Daily Data Coverage - Week 1 (Patient F001)")
    plot_hr_data(week2_F001, "Week 2 (Patient F001)")
    plot_daily_coverage(compute_daily_segments(week2_F001), "Daily Data Coverage - Week 2 (Patient F001)")

    # Compute coverage percentages for all patients using multiprocessing.
    df_week1 = compute_coverage_for_all_patients_mp(data_dir, "Week1")
    df_week2 = compute_coverage_for_all_patients_mp(data_dir, "Week2")

    print("Coverage Percentages for Week 1:")
    print(df_week1)
    print("\nCoverage Percentages for Week 2:")
    print(df_week2)

    # Optionally, save the tables to CSV files.
    df_week1.to_csv("coverage_week1.csv", index=False)
    df_week2.to_csv("coverage_week2.csv", index=False)

if __name__ == "__main__":
    main()
