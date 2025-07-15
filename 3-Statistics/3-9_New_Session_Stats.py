from datetime import timedelta, datetime

from RXLDBC import connect
from multiprocessing import Process, Queue

import neurokit2 as nk
import numpy as np
import pandas as pd

def main():
    conn = connect.Connection()
    cursor = conn.conn.cursor()
    # Set work_mem to 1GB
    cursor.execute("SET work_mem = '1GB'")

    cursor.execute("SHOW work_mem")
    print(cursor.fetchone())
    cursor.execute("SELECT id FROM patient ORDER BY id")
    patient_ids = cursor.fetchall()

    # processes = []
    # queue = Queue()
    dataframes = []

    # patient_ids = [ ('L007',)]

    for patient_id in patient_ids:
        patient_id = patient_id[0]
        e4_timestamps = conn.get_all_timestamps_from_patient_id(patient_id)
        relax_timestamps = conn.get_all_relax_sessions_from_patient_id(patient_id)

        filtered_relax_sessions = filter_5min_of_e4_before_and_after_relax_sessions(e4_timestamps, relax_timestamps)

        for index, session in enumerate(filtered_relax_sessions.items()):
            print(f"Starting process {index} of {len(filtered_relax_sessions)} for patient {patient_id}...")
            dataframes.append(calculate_stats_for_relax_session(session))

    # for process in processes:
    #     process.join(timeout=10)
    #     if process.is_alive():
    #         print(f"Process {process.pid} is still alive after 120 seconds. Terminating...")
    #         process.terminate()
    #         process.join()

    # Get all dataframes from the queue and combine them into one

    # while not queue.empty():
    #     dataframes.append(queue.get())
    # remove empty dataframes
    dataframes = [df for df in dataframes if df is not None and not df.empty]

    combined_df = pd.concat(dataframes, ignore_index=True)
    # Save the combined dataframe to a CSV file
    combined_df.to_csv("stats_session.csv", header=True)


def filter_5min_of_e4_before_and_after_relax_sessions(e4_timestamps, relax_timestamps):
    # Filter E4 sessions that are 5 minutes before and after relaxation sessions
    filtered_sessions = {}
    for session_id, (start, end) in e4_timestamps.items():
        for relax_id, (relax_start, relax_end) in relax_timestamps.items():
            if start <= relax_start + timedelta(minutes=5) and end >= relax_end + timedelta(minutes=5):
                if relax_id not in filtered_sessions:
                    filtered_sessions[relax_id] = [{session_id: (start, end)}]
                else:
                    filtered_sessions[relax_id].append({session_id: (start, end)})
    return filtered_sessions

def calculate_stats_for_relax_session(relax_session):
    # test = ('F001_Exercise_25029', [{'F001_Week_1_ACC_X_1559': (datetime.datetime(2022, 7, 10, 10, 51, 47), datetime.datetime(2022, 7, 11, 0, 25, 22))}, {'F001_Week_1_ACC_Y_1562': (datetime.datetime(2022, 7, 10, 10, 51, 47), datetime.datetime(2022, 7, 11, 0, 25, 22))}, {'F001_Week_1_ACC_Z_1565': (datetime.datetime(2022, 7, 10, 10, 51, 47), datetime.datetime(2022, 7, 11, 0, 25, 22))}, {'F001_Week_1_EDA_1575': (datetime.datetime(2022, 7, 10, 10, 51, 47), datetime.datetime(2022, 7, 11, 0, 25, 26))}, {'F001_Week_1_BVP_1667': (datetime.datetime(2022, 7, 10, 10, 51, 47), datetime.datetime(2022, 7, 11, 0, 25, 19))}, {'F001_Week_1_TEMP_1684': (datetime.datetime(2022, 7, 10, 10, 51, 47), datetime.datetime(2022, 7, 11, 0, 24, 31))}, {'F001_Week_1_HR_1695': (datetime.datetime(2022, 7, 10, 10, 51, 57), datetime.datetime(2022, 7, 11, 0, 25, 20))}])
    print(f"Calculating stats for {relax_session[0]}...")
    # Get the start and end timestamps of the relaxation session
    conn = connect.Connection()
    cursor = conn.conn.cursor()
    relax_id = relax_session[0].split("_")[2]
    cursor.execute("SELECT patient_id, start_timestamp, end_timestamp, ontspanning_start, ontspanning_eind, kalm_start, kalm_eind FROM relax_session WHERE id = %s", (relax_id,))
    patient_id, start_timestamp, end_timestamp, ontspanning_start, ontspanning_eind, kalm_start, kalm_eind = cursor.fetchone()

    # Categorize the relaxation session start timestamp into morning, afternoon, evening, or night
    if 6 < start_timestamp.hour < 12:
        relaxation_time = "Morning"
    elif 12 <= start_timestamp.hour < 18:
        relaxation_time = "Afternoon"
    elif 18 <= start_timestamp.hour < 24:
        relaxation_time = "Evening"
    else:
        relaxation_time = "Night"

    # Get the patient info from the database
    cursor.execute("SELECT origin, patient_group, age, sex FROM patient WHERE id = %s", (patient_id,))
    origin, patient_group, age, sex = cursor.fetchone()

    # Get the amount of relaxation sessions for this patient
    cursor.execute("SELECT COUNT(*) FROM relax_session WHERE patient_id = %s", (patient_id,))
    relax_count = cursor.fetchone()[0]

    # Get the average duration of relaxation sessions for this patient in seconds
    cursor.execute("SELECT start_timestamp, end_timestamp FROM relax_session WHERE patient_id = %s", (patient_id,))
    relax_sessions = cursor.fetchall()
    relax_durations = [(end - start).total_seconds() for start, end in relax_sessions]
    avg_relax_duration = sum(relax_durations) / len(relax_durations) if relax_durations else 0

    print(relax_session)

    acc_x = ""
    acc_y = ""
    acc_z = ""
    bvp = ""
    eda = ""
    hr = ""
    temp = ""
    ibi = ""

    for measurement_session in relax_session[1]:
        for measure_id, (start, end) in measurement_session.items():
            # Get the measurement type from the session ID
            measurement_type = measure_id.split("_")[-2]

            invalid_indices = conn.get_invalid_data_indices_from_measure_session(measure_id.split("_")[-1])

            if measurement_type == "HR":
                hr = measure_id.split("_")[-1]
                # Get the amount of seconds difference between the start of the measurement session and the start of the relaxation session
                start_of_relax = int((start_timestamp - start).total_seconds())
                # Subtract 5 minutes from the seconds difference
                minus_5_mins = start_of_relax - 300
                # Get the data points from the database with the slice of the seconds difference

                cursor.execute("SELECT data[%s:%s] FROM measure_session WHERE id = %s", (minus_5_mins, start_of_relax, measure_id.split("_")[-1]))
                hr_data_before = cursor.fetchone()[0]

                # Calculate stats
                hr_sd_before = np.std(hr_data_before)
                hr_mean_before = np.mean(hr_data_before)
                hr_median_before = np.median(hr_data_before)
                hr_min_before = np.min(hr_data_before)
                hr_max_before = np.max(hr_data_before)
                hr_range_before = hr_max_before - hr_min_before
                hr_1q_before = np.percentile(hr_data_before, 25)
                hr_3q_before = np.percentile(hr_data_before, 75)
                hr_iqr_before = hr_3q_before - hr_1q_before

                # Get the difference between the end of the relaxation session and the start of the measurement session
                end_of_relax = int((end_timestamp - start).total_seconds())

                # Get the data points from the database with the slice of the beginning and end of the relaxation session
                cursor.execute("SELECT data[%s:%s] FROM measure_session WHERE id = %s", (start_of_relax, end_of_relax, measure_id.split("_")[-1]))
                hr_data_during = cursor.fetchone()[0]

                # Calculate stats
                hr_sd_during = np.std(hr_data_during)
                hr_mean_during = np.mean(hr_data_during)
                hr_median_during = np.median(hr_data_during)
                hr_min_during = np.min(hr_data_during)
                hr_max_during = np.max(hr_data_during)
                hr_range_during = hr_max_during - hr_min_during
                hr_1q_during = np.percentile(hr_data_during, 25)
                hr_3q_during = np.percentile(hr_data_during, 75)
                hr_iqr_during = hr_3q_during - hr_1q_during

                # Add 5 minutes to the end of the relaxation session
                plus_5_mins = end_of_relax + 300
                # Get the data points from the database with the slice of the end of the relaxation session
                cursor.execute("SELECT data[%s:%s] FROM measure_session WHERE id = %s", (end_of_relax, plus_5_mins, measure_id.split("_")[-1]))
                hr_data_after = cursor.fetchone()[0]

                # Calculate stats
                hr_sd_after = np.std(hr_data_after)
                hr_mean_after = np.mean(hr_data_after)
                hr_median_after = np.median(hr_data_after)
                hr_min_after = np.min(hr_data_after)
                hr_max_after = np.max(hr_data_after)
                hr_range_after = hr_max_after - hr_min_after
                hr_1q_after = np.percentile(hr_data_after, 25)
                hr_3q_after = np.percentile(hr_data_after, 75)
                hr_iqr_after = hr_3q_after - hr_1q_after

                # If more than 20% of the data within 5 min before and after the relaxation session is within the invalid indices, return nothing
                if invalid_indices:
                    total_data_points = plus_5_mins - minus_5_mins
                    invalid_data_points = 0
                    # Print all invalid indices
                    print(f"Invalid indices for {measure_id}: {invalid_indices}")
                    print(f"minutes before and after the relaxation session: {minus_5_mins} to {plus_5_mins}")
                    for invalid_index in invalid_indices:
                        if invalid_index[0] <= minus_5_mins and invalid_index[1] <= minus_5_mins:
                            continue
                        elif invalid_index[0] <= minus_5_mins and invalid_index[1] >= plus_5_mins:
                            invalid_data_points += plus_5_mins - minus_5_mins
                        elif invalid_index[0] >= plus_5_mins and invalid_index[1] >= plus_5_mins:
                            continue
                        elif invalid_index[0] >= minus_5_mins and invalid_index[1] >= minus_5_mins:
                            invalid_data_points += plus_5_mins - invalid_index[0]
                        elif invalid_index[0] <= minus_5_mins and invalid_index[1] <= plus_5_mins:
                            invalid_data_points += invalid_index[1] - minus_5_mins
                        elif invalid_index[0] >= minus_5_mins and invalid_index[1] <= plus_5_mins:
                            invalid_data_points += invalid_index[1] - invalid_index[0]
                    print(f"Invalid data points for {measure_id}: {invalid_data_points} out of {total_data_points} total data points.")
                    print(f"Invalid data points percentage for {measure_id}: {invalid_data_points / total_data_points * 100:.2f}%")
                    if invalid_data_points / total_data_points > 0.2:
                        # Print in blue that more than 20% of the data within 5 min before and after the relaxation session is invalid
                        print(f"\033[94mMore than 20% of the data within 5 min before and after the relaxation session is invalid for {measure_id}. Skipping...\033[0m")
                        return pd.DataFrame()




            elif measurement_type == "EDA":
                eda = measure_id.split("_")[-1]
                # Get the amount of seconds difference between the start of the measurement session and the start of the relaxation session
                start_of_relax = int((start_timestamp - start).total_seconds()) * 4
                # Subtract 5 minutes from the seconds difference
                minus_5_mins = start_of_relax - (300 * 4)
                # Get the data points from the database with the slice of the seconds difference
                cursor.execute("SELECT data[%s:%s] FROM measure_session WHERE id = %s", (minus_5_mins, start_of_relax, measure_id.split("_")[-1]))
                eda_data_before = cursor.fetchone()[0]

                # Calculate stats
                eda_sd_before = np.std(eda_data_before)
                eda_mean_before = np.mean(eda_data_before)
                eda_median_before = np.median(eda_data_before)
                eda_min_before = np.min(eda_data_before)
                eda_max_before = np.max(eda_data_before)
                eda_range_before = eda_max_before - eda_min_before
                eda_1q_before = np.percentile(eda_data_before, 25)
                eda_3q_before = np.percentile(eda_data_before, 75)
                eda_iqr_before = eda_3q_before - eda_1q_before

                nk_data_before = np.array(eda_data_before).ravel()

                signals, info = nk.eda_process(nk_data_before, sampling_rate=4)

                scl = signals["EDA_Tonic"]
                eda_scl_sd_before = np.std(scl)
                eda_scl_mean_before = np.mean(scl)
                eda_scl_median_before = np.median(scl)
                eda_scl_min_before = np.min(scl)
                eda_scl_max_before = np.max(scl)
                eda_scl_range_before = eda_scl_max_before - eda_scl_min_before
                eda_scl_1q_before = np.percentile(scl, 25)
                eda_scl_3q_before = np.percentile(scl, 75)
                eda_scl_iqr_before = eda_scl_3q_before - eda_scl_1q_before

                eda_scr_peaks_before = len(np.where(signals["SCR_Peaks"])[0])

                amplitudes = info["SCR_Amplitude"]
                eda_scr_amplitude_sd_before = np.std(amplitudes)
                eda_scr_amplitude_mean_before = np.mean(amplitudes)
                eda_scr_amplitude_median_before = np.median(amplitudes)
                eda_scr_amplitude_min_before = np.min(amplitudes)
                eda_scr_amplitude_max_before = np.max(amplitudes)
                eda_scr_amplitude_range_before = eda_scr_amplitude_max_before - eda_scr_amplitude_min_before
                eda_scr_amplitude_1q_before = np.percentile(amplitudes, 25)
                eda_scr_amplitude_3q_before = np.percentile(amplitudes, 75)
                eda_scr_amplitude_iqr_before = eda_scr_amplitude_3q_before - eda_scr_amplitude_1q_before


                # Get the difference between the end of the relaxation session and the start of the measurement session
                end_of_relax = int((end_timestamp - start).total_seconds()) * 4
                # Get the data points from the database with the slice of the beginning and end of the relaxation session
                cursor.execute("SELECT data[%s:%s] FROM measure_session WHERE id = %s", (start_of_relax, end_of_relax, measure_id.split("_")[-1]))
                eda_data_during = cursor.fetchone()[0]

                # Calculate stats
                eda_sd_during = np.std(eda_data_during)
                eda_mean_during = np.mean(eda_data_during)
                eda_median_during = np.median(eda_data_during)
                eda_min_during = np.min(eda_data_during)
                eda_max_during = np.max(eda_data_during)
                eda_range_during = eda_max_during - eda_min_during
                eda_1q_during = np.percentile(eda_data_during, 25)
                eda_3q_during = np.percentile(eda_data_during, 75)
                eda_iqr_during = eda_3q_during - eda_1q_during

                nk_data_during = np.array(eda_data_during).ravel()

                signals, info = nk.eda_process(nk_data_during, sampling_rate=4)

                scl = signals["EDA_Tonic"]
                eda_scl_sd_during = np.std(scl)
                eda_scl_mean_during = np.mean(scl)
                eda_scl_median_during = np.median(scl)
                eda_scl_min_during = np.min(scl)
                eda_scl_max_during = np.max(scl)
                eda_scl_range_during = eda_scl_max_during - eda_scl_min_during
                eda_scl_1q_during = np.percentile(scl, 25)
                eda_scl_3q_during = np.percentile(scl, 75)
                eda_scl_iqr_during = eda_scl_3q_during - eda_scl_1q_during

                eda_scr_peaks_during = len(np.where(signals["SCR_Peaks"])[0])

                amplitudes = info["SCR_Amplitude"]
                eda_scr_amplitude_sd_during = np.std(amplitudes)
                eda_scr_amplitude_mean_during = np.mean(amplitudes)
                eda_scr_amplitude_median_during = np.median(amplitudes)
                eda_scr_amplitude_min_during = np.min(amplitudes)
                eda_scr_amplitude_max_during = np.max(amplitudes)
                eda_scr_amplitude_range_during = eda_scr_amplitude_max_during - eda_scr_amplitude_min_during
                eda_scr_amplitude_1q_during = np.percentile(amplitudes, 25)
                eda_scr_amplitude_3q_during = np.percentile(amplitudes, 75)
                eda_scr_amplitude_iqr_during = eda_scr_amplitude_3q_during - eda_scr_amplitude_1q_during


                # Add 5 minutes to the end of the relaxation session
                plus_5_mins = end_of_relax + (300 * 4)

                # Get the data points from the database with the slice of the end of the relaxation session
                cursor.execute("SELECT data[%s:%s] FROM measure_session WHERE id = %s", (end_of_relax, plus_5_mins, measure_id.split("_")[-1]))
                eda_data_after = cursor.fetchone()[0]

                # Calculate stats
                eda_sd_after = np.std(eda_data_after)
                eda_mean_after = np.mean(eda_data_after)
                eda_median_after = np.median(eda_data_after)
                eda_min_after = np.min(eda_data_after)
                eda_max_after = np.max(eda_data_after)
                eda_range_after = eda_max_after - eda_min_after
                eda_1q_after = np.percentile(eda_data_after, 25)
                eda_3q_after = np.percentile(eda_data_after, 75)
                eda_iqr_after = eda_3q_after - eda_1q_after

                nk_data_after = np.array(eda_data_after).ravel()

                signals, info = nk.eda_process(nk_data_after, sampling_rate=4)

                scl = signals["EDA_Tonic"]
                eda_scl_sd_after = np.std(scl)
                eda_scl_mean_after = np.mean(scl)
                eda_scl_median_after = np.median(scl)
                eda_scl_min_after = np.min(scl)
                eda_scl_max_after = np.max(scl)
                eda_scl_range_after = eda_scl_max_after - eda_scl_min_after
                eda_scl_1q_after = np.percentile(scl, 25)
                eda_scl_3q_after = np.percentile(scl, 75)
                eda_scl_iqr_after = eda_scl_3q_after - eda_scl_1q_after

                eda_scr_peaks_after = len(np.where(signals["SCR_Peaks"])[0])

                amplitudes = info["SCR_Amplitude"]
                eda_scr_amplitude_sd_after = np.std(amplitudes)
                eda_scr_amplitude_mean_after = np.mean(amplitudes)
                eda_scr_amplitude_median_after = np.median(amplitudes)
                eda_scr_amplitude_min_after = np.min(amplitudes)
                eda_scr_amplitude_max_after = np.max(amplitudes)
                eda_scr_amplitude_range_after = eda_scr_amplitude_max_after - eda_scr_amplitude_min_after
                eda_scr_amplitude_1q_after = np.percentile(amplitudes, 25)
                eda_scr_amplitude_3q_after = np.percentile(amplitudes, 75)
                eda_scr_amplitude_iqr_after = eda_scr_amplitude_3q_after - eda_scr_amplitude_1q_after

            elif measurement_type == "BVP":
                bvp = measure_id.split("_")[-1]
                # Get the amount of seconds difference between the start of the measurement session and the start of the relaxation session
                start_of_relax = int((start_timestamp - start).total_seconds()) * 64
                # Subtract 5 minutes from the seconds difference
                minus_5_mins = start_of_relax - (300 * 64)
                # Get the data points from the database with the slice of the seconds difference
                cursor.execute("SELECT data[%s:%s] FROM measure_session WHERE id = %s", (minus_5_mins, start_of_relax, measure_id.split("_")[-1]))
                bvp_data_before = cursor.fetchone()[0]

                # Calculate stats
                bvp_sd_before = np.std(bvp_data_before)
                bvp_mean_before = np.mean(bvp_data_before)
                bvp_median_before = np.median(bvp_data_before)
                bvp_min_before = np.min(bvp_data_before)
                bvp_max_before = np.max(bvp_data_before)
                bvp_range_before = bvp_max_before - bvp_min_before
                bvp_1q_before = np.percentile(bvp_data_before, 25)
                bvp_3q_before = np.percentile(bvp_data_before, 75)
                bvp_iqr_before = bvp_3q_before - bvp_1q_before

                # Get the difference between the end of the relaxation session and the start of the measurement session
                end_of_relax = int((end_timestamp - start).total_seconds()) * 64
                # Get the data points from the database with the slice of the beginning and end of the relaxation session
                cursor.execute("SELECT data[%s:%s] FROM measure_session WHERE id = %s", (start_of_relax, end_of_relax, measure_id.split("_")[-1]))
                bvp_data_during = cursor.fetchone()[0]

                # Calculate stats
                bvp_sd_during = np.std(bvp_data_during)
                bvp_mean_during = np.mean(bvp_data_during)
                bvp_median_during = np.median(bvp_data_during)
                bvp_min_during = np.min(bvp_data_during)
                bvp_max_during = np.max(bvp_data_during)
                bvp_range_during = bvp_max_during - bvp_min_during
                bvp_1q_during = np.percentile(bvp_data_during, 25)
                bvp_3q_during = np.percentile(bvp_data_during, 75)
                bvp_iqr_during = bvp_3q_during - bvp_1q_during

                # Add 5 minutes to the end of the relaxation session
                plus_5_mins = end_of_relax + (300 * 64)
                # Get the data points from the database with the slice of the end of the relaxation session
                cursor.execute("SELECT data[%s:%s] FROM measure_session WHERE id = %s", (end_of_relax, plus_5_mins, measure_id.split("_")[-1]))
                bvp_data_after = cursor.fetchone()[0]

                # Calculate stats
                bvp_sd_after = np.std(bvp_data_after)
                bvp_mean_after = np.mean(bvp_data_after)
                bvp_median_after = np.median(bvp_data_after)
                bvp_min_after = np.min(bvp_data_after)
                bvp_max_after = np.max(bvp_data_after)
                bvp_range_after = bvp_max_after - bvp_min_after
                bvp_1q_after = np.percentile(bvp_data_after, 25)
                bvp_3q_after = np.percentile(bvp_data_after, 75)
                bvp_iqr_after = bvp_3q_after - bvp_1q_after

            elif measurement_type == "TEMP":
                temp = measure_id.split("_")[-1]
                # Get the amount of seconds difference between the start of the measurement session and the start of the relaxation session
                start_of_relax = int((start_timestamp - start).total_seconds()) * 4
                # Subtract 5 minutes from the seconds difference
                minus_5_mins = start_of_relax - (300 * 4)
                # Get the data points from the database with the slice of the seconds difference
                cursor.execute("SELECT data[%s:%s] FROM measure_session WHERE id = %s", (minus_5_mins, start_of_relax, measure_id.split("_")[-1]))
                temp_data_before = cursor.fetchone()[0]

                # Calculate stats
                temp_sd_before = np.std(temp_data_before)
                temp_mean_before = np.mean(temp_data_before)
                temp_median_before = np.median(temp_data_before)
                temp_min_before = np.min(temp_data_before)
                temp_max_before = np.max(temp_data_before)
                temp_range_before = temp_max_before - temp_min_before
                temp_1q_before = np.percentile(temp_data_before, 25)
                temp_3q_before = np.percentile(temp_data_before, 75)
                temp_iqr_before = temp_3q_before - temp_1q_before

                # Get the difference between the end of the relaxation session and the start of the measurement session
                end_of_relax = int((end_timestamp - start).total_seconds()) * 4
                # Get the data points from the database with the slice of the beginning and end of the relaxation session
                cursor.execute("SELECT data[%s:%s] FROM measure_session WHERE id = %s", (start_of_relax, end_of_relax, measure_id.split("_")[-1]))
                temp_data_during = cursor.fetchone()[0]

                # Calculate stats
                temp_sd_during = np.std(temp_data_during)
                temp_mean_during = np.mean(temp_data_during)
                temp_median_during = np.median(temp_data_during)
                temp_min_during = np.min(temp_data_during)
                temp_max_during = np.max(temp_data_during)
                temp_range_during = temp_max_during - temp_min_during
                temp_1q_during = np.percentile(temp_data_during, 25)
                temp_3q_during = np.percentile(temp_data_during, 75)
                temp_iqr_during = temp_3q_during - temp_1q_during

                # Add 5 minutes to the end of the relaxation session
                plus_5_mins = end_of_relax + (300 * 4)

                # Get the data points from the database with the slice of the end of the relaxation session
                cursor.execute("SELECT data[%s:%s] FROM measure_session WHERE id = %s", (end_of_relax, plus_5_mins, measure_id.split("_")[-1]))
                temp_data_after = cursor.fetchone()[0]

                # Calculate stats
                temp_sd_after = np.std(temp_data_after)
                temp_mean_after = np.mean(temp_data_after)
                temp_median_after = np.median(temp_data_after)
                temp_min_after = np.min(temp_data_after)
                temp_max_after = np.max(temp_data_after)
                temp_range_after = temp_max_after - temp_min_after
                temp_1q_after = np.percentile(temp_data_after, 25)
                temp_3q_after = np.percentile(temp_data_after, 75)
                temp_iqr_after = temp_3q_after - temp_1q_after

            elif measurement_type == "X":
                acc_x = measure_id.split("_")[-1]
            elif measurement_type == "Y":
                acc_y = measure_id.split("_")[-1]   
            elif measurement_type == "Z":
                acc_z = measure_id.split("_")[-1]
                
                # Get the amount of seconds difference between the start of the measurement session and the start of the relaxation session
                start_of_relax = int((start_timestamp - start).total_seconds()) * 32
                # Subtract 5 minutes from the seconds difference
                minus_5_mins = start_of_relax - (300 * 32)
                # Get the data points from the database with the slice of the seconds difference
                cursor.execute("SELECT data[%s:%s] FROM measure_session WHERE id = %s", (minus_5_mins, start_of_relax, acc_x))
                acc_x_data_before = cursor.fetchone()[0]
                cursor.execute("SELECT data[%s:%s] FROM measure_session WHERE id = %s", (minus_5_mins, start_of_relax, acc_y))
                acc_y_data_before = cursor.fetchone()[0]
                cursor.execute("SELECT data[%s:%s] FROM measure_session WHERE id = %s", (minus_5_mins, start_of_relax, acc_z))
                acc_z_data_before = cursor.fetchone()[0]
                
                # Calculate vectors of magnitude
                acc_x_data_before = np.array(acc_x_data_before)
                acc_y_data_before = np.array(acc_y_data_before)
                acc_z_data_before = np.array(acc_z_data_before)
                acc_magnitude_before = np.sqrt(acc_x_data_before**2 + acc_y_data_before**2 + acc_z_data_before**2)
                
                # Calculate stats
                acc_magnitude_sd_before = np.std(acc_magnitude_before)
                acc_magnitude_mean_before = np.mean(acc_magnitude_before)   
                acc_magnitude_median_before = np.median(acc_magnitude_before)
                acc_magnitude_min_before = np.min(acc_magnitude_before)
                acc_magnitude_max_before = np.max(acc_magnitude_before)
                acc_magnitude_range_before = acc_magnitude_max_before - acc_magnitude_min_before
                acc_magnitude_1q_before = np.percentile(acc_magnitude_before, 25)
                acc_magnitude_3q_before = np.percentile(acc_magnitude_before, 75)
                acc_magnitude_iqr_before = acc_magnitude_3q_before - acc_magnitude_1q_before
                
                # Get the difference between the end of the relaxation session and the start of the measurement session
                end_of_relax = int((end_timestamp - start).total_seconds()) * 32
                # Get the data points from the database with the slice of the beginning and end of the relaxation session
                cursor.execute("SELECT data[%s:%s] FROM measure_session WHERE id = %s", (start_of_relax, end_of_relax, acc_x))
                acc_x_data_during = cursor.fetchone()[0]
                cursor.execute("SELECT data[%s:%s] FROM measure_session WHERE id = %s", (start_of_relax, end_of_relax, acc_y))
                acc_y_data_during = cursor.fetchone()[0]
                cursor.execute("SELECT data[%s:%s] FROM measure_session WHERE id = %s", (start_of_relax, end_of_relax, acc_z))
                acc_z_data_during = cursor.fetchone()[0]
                
                # Calculate vectors of magnitude
                acc_x_data_during = np.array(acc_x_data_during)
                acc_y_data_during = np.array(acc_y_data_during)
                acc_z_data_during = np.array(acc_z_data_during)
                acc_magnitude_during = np.sqrt(acc_x_data_during**2 + acc_y_data_during**2 + acc_z_data_during**2)
                
                # Calculate stats
                acc_magnitude_sd_during = np.std(acc_magnitude_during)
                acc_magnitude_mean_during = np.mean(acc_magnitude_during)
                acc_magnitude_median_during = np.median(acc_magnitude_during)
                acc_magnitude_min_during = np.min(acc_magnitude_during)
                acc_magnitude_max_during = np.max(acc_magnitude_during)
                acc_magnitude_range_during = acc_magnitude_max_during - acc_magnitude_min_during
                acc_magnitude_1q_during = np.percentile(acc_magnitude_during, 25)
                acc_magnitude_3q_during = np.percentile(acc_magnitude_during, 75)
                acc_magnitude_iqr_during = acc_magnitude_3q_during - acc_magnitude_1q_during
                
                # Add 5 minutes to the end of the relaxation session
                plus_5_mins = end_of_relax + (300 * 32)
                # Get the data points from the database with the slice of the end of the relaxation session
                cursor.execute("SELECT data[%s:%s] FROM measure_session WHERE id = %s", (end_of_relax, plus_5_mins, acc_x))
                acc_x_data_after = cursor.fetchone()[0]
                cursor.execute("SELECT data[%s:%s] FROM measure_session WHERE id = %s", (end_of_relax, plus_5_mins, acc_y))
                acc_y_data_after = cursor.fetchone()[0]
                cursor.execute("SELECT data[%s:%s] FROM measure_session WHERE id = %s", (end_of_relax, plus_5_mins, acc_z))
                acc_z_data_after = cursor.fetchone()[0]
                
                # Calculate vectors of magnitude
                acc_x_data_after = np.array(acc_x_data_after)
                acc_y_data_after = np.array(acc_y_data_after)
                acc_z_data_after = np.array(acc_z_data_after)
                acc_magnitude_after = np.sqrt(acc_x_data_after**2 + acc_y_data_after**2 + acc_z_data_after**2)
                
                # Calculate stats
                acc_magnitude_sd_after = np.std(acc_magnitude_after)
                acc_magnitude_mean_after = np.mean(acc_magnitude_after)
                acc_magnitude_median_after = np.median(acc_magnitude_after)
                acc_magnitude_min_after = np.min(acc_magnitude_after)
                acc_magnitude_max_after = np.max(acc_magnitude_after)
                acc_magnitude_range_after = acc_magnitude_max_after - acc_magnitude_min_after
                acc_magnitude_1q_after = np.percentile(acc_magnitude_after, 25)
                acc_magnitude_3q_after = np.percentile(acc_magnitude_after, 75)
                acc_magnitude_iqr_after = acc_magnitude_3q_after - acc_magnitude_1q_after

            elif measurement_type == "IBI":
                ibi = measure_id.split("_")[-1]
                cursor.execute("SELECT data FROM measure_session WHERE id = %s", (ibi,))
                ibi_data = cursor.fetchone()[0]
                time_sequence = [entry[0] for entry in ibi_data]
                ibi_sequence = [round(entry[1] * 1000) for entry in ibi_data]

                # Find the index of the value in time_sequence that is closest to the minus_5_mins
                closest_index_before = min(range(len(time_sequence)), key=lambda i: abs(time_sequence[i] - minus_5_mins))
                # Find the index of the value in time_sequence that is closest to the start_of_relax
                closest_index_start = min(range(len(time_sequence)), key=lambda i: abs(time_sequence[i] - start_of_relax))
                # Find the index of the value in time_sequence that is closest to the end_of_relax
                closest_index_end = min(range(len(time_sequence)), key=lambda i: abs(time_sequence[i] - end_of_relax))
                # Find the index of the value in time_sequence that is closest to the plus_5_mins
                closest_index_after = min(range(len(time_sequence)), key=lambda i: abs(time_sequence[i] - plus_5_mins))

                # Check if the values of the closest indices are within 5 seconds of the before, start, end and after relaxation session
                if abs(time_sequence[closest_index_before] - minus_5_mins) > 75 or \
                     abs(time_sequence[closest_index_start] - start_of_relax) > 75 or \
                        abs(time_sequence[closest_index_end] - end_of_relax) > 75 or \
                            abs(time_sequence[closest_index_after] - plus_5_mins) > 75:
                    print(f"IBI data for {measure_id} is not within 5 seconds of the before, start, end and after relaxation session. Skipping...")
                    # Print all values in orange and their difference to the before, start, end and after relaxation session
                    print(f"\033[93mIBI data for {measure_id} is not within 5 seconds of the before, start, end and after relaxation session.\033[0m")
                    print(f"Closest before: {time_sequence[closest_index_before]} (diff: {abs(time_sequence[closest_index_before] - minus_5_mins)})")
                    print(f"Closest start: {time_sequence[closest_index_start]} (diff: {abs(time_sequence[closest_index_start] - start_of_relax)})")
                    print(f"Closest end: {time_sequence[closest_index_end]} (diff: {abs(time_sequence[closest_index_end] - end_of_relax)})")
                    print(f"Closest after: {time_sequence[closest_index_after]} (diff: {abs(time_sequence[closest_index_after] - plus_5_mins)})")
                    ibi_data_before = pd.DataFrame()
                    ibi_data_during = pd.DataFrame()
                    ibi_data_after = pd.DataFrame()

                else:
                    # Print in bright green that the IBI data is within 5 seconds of the before, start, end and after relaxation session
                    print(f"\033[92mIBI data for {measure_id} is within 5 seconds of the before, start, end and after relaxation session.\033[0m")
                    # Get the IBI data for the before, during and after relaxation session
                    ibi_data_before = nk.hrv(
                        nk.intervals_to_peaks(ibi_sequence[closest_index_before:closest_index_start],
                                              time_sequence[closest_index_before:closest_index_start]))
                    ibi_data_during = nk.hrv(nk.intervals_to_peaks(ibi_sequence[closest_index_start:closest_index_end],
                                                                   time_sequence[
                                                                   closest_index_start:closest_index_end]))
                    ibi_data_after = nk.hrv(nk.intervals_to_peaks(ibi_sequence[closest_index_end:closest_index_after],
                                                                  time_sequence[closest_index_end:closest_index_after]))

                    # Change the names in the dataframe to add before, during and after
                    ibi_data_before = ibi_data_before.rename(columns=lambda x: f"{x}_before")
                    ibi_data_during = ibi_data_during.rename(columns=lambda x: f"{x}_during")
                    ibi_data_after = ibi_data_after.rename(columns=lambda x: f"{x}_after")










    print(f"Stats for {relax_session[0]} calculated.")
    if acc_x and acc_y and acc_z and bvp and eda and hr and temp:
        # Create a DataFrame with the stats
        stats = {
            "patient_id": patient_id,
            "patient_group": patient_group,
            "patient_origin": origin,
            "patient_age": age,
            "patient_sex": sex,
            "patient_mean_relax_duration": avg_relax_duration,
            "patient_relax_count": relax_count,
            
            "relax_id": relax_id,
            "relax_session_start": start_timestamp,
            "relax_session_end": end_timestamp,
            "relax_session_duration": (end_timestamp - start_timestamp).total_seconds(),
            "relaxation_time": relaxation_time,
            "Q_ontspanning_start": ontspanning_start,
            "Q_ontspanning_eind": ontspanning_eind,
            "Q_kalm_start": kalm_start,
            "Q_kalm_eind": kalm_eind,
            
            "HR_id": hr,
            "HR_sd_before": hr_sd_before,
            "HR_sd_during": hr_sd_during,
            "HR_sd_after": hr_sd_after,
            "HR_mean_before": hr_mean_before,
            "HR_mean_during": hr_mean_during,
            "HR_mean_after": hr_mean_after,
            "HR_median_before": hr_median_before,
            "HR_median_during": hr_median_during,
            "HR_median_after": hr_median_after,
            "HR_min_before": hr_min_before,
            "HR_min_during": hr_min_during,
            "HR_min_after": hr_min_after,
            "HR_max_before": hr_max_before,
            "HR_max_during": hr_max_during,
            "HR_max_after": hr_max_after,
            "HR_range_before": hr_range_before,
            "HR_range_during": hr_range_during,
            "HR_range_after": hr_range_after,
            "HR_1q_before": hr_1q_before,
            "HR_1q_during": hr_1q_during,
            "HR_1q_after": hr_1q_after,
            "HR_3q_before": hr_3q_before,
            "HR_3q_during": hr_3q_during,
            "HR_3q_after": hr_3q_after,
            "HR_iqr_before": hr_iqr_before,
            "HR_iqr_during": hr_iqr_during,
            "HR_iqr_after": hr_iqr_after,
            
            "BVP_id": bvp,
            "BVP_sd_before": bvp_sd_before,
            "BVP_sd_during": bvp_sd_during,
            "BVP_sd_after": bvp_sd_after,
            "BVP_mean_before": bvp_mean_before,
            "BVP_mean_during": bvp_mean_during,
            "BVP_mean_after": bvp_mean_after,
            "BVP_median_before": bvp_median_before,
            "BVP_median_during": bvp_median_during,
            "BVP_median_after": bvp_median_after,
            "BVP_min_before": bvp_min_before,
            "BVP_min_during": bvp_min_during,
            "BVP_min_after": bvp_min_after,
            "BVP_max_before": bvp_max_before,
            "BVP_max_during": bvp_max_during,
            "BVP_max_after": bvp_max_after,
            "BVP_range_before": bvp_range_before,
            "BVP_range_during": bvp_range_during,
            "BVP_range_after": bvp_range_after,
            "BVP_1q_before": bvp_1q_before,
            "BVP_1q_during": bvp_1q_during,
            "BVP_1q_after": bvp_1q_after,
            "BVP_3q_before": bvp_3q_before,
            "BVP_3q_during": bvp_3q_during,
            "BVP_3q_after": bvp_3q_after,
            "BVP_iqr_before": bvp_iqr_before,
            "BVP_iqr_during": bvp_iqr_during,
            "BVP_iqr_after": bvp_iqr_after,

            "TEMP_id": temp,
            "TEMP_sd_before": temp_sd_before,
            "TEMP_sd_during": temp_sd_during,
            "TEMP_sd_after": temp_sd_after,
            "TEMP_mean_before": temp_mean_before,
            "TEMP_mean_during": temp_mean_during,
            "TEMP_mean_after": temp_mean_after,
            "TEMP_median_before": temp_median_before,
            "TEMP_median_during": temp_median_during,
            "TEMP_median_after": temp_median_after,
            "TEMP_min_before": temp_min_before,
            "TEMP_min_during": temp_min_during,
            "TEMP_min_after": temp_min_after,
            "TEMP_max_before": temp_max_before,
            "TEMP_max_during": temp_max_during,
            "TEMP_max_after": temp_max_after,
            "TEMP_range_before": temp_range_before,
            "TEMP_range_during": temp_range_during,
            "TEMP_range_after": temp_range_after,
            "TEMP_1q_before": temp_1q_before,
            "TEMP_1q_during": temp_1q_during,
            "TEMP_1q_after": temp_1q_after,
            "TEMP_3q_before": temp_3q_before,
            "TEMP_3q_during": temp_3q_during,
            "TEMP_3q_after": temp_3q_after,
            "TEMP_iqr_before": temp_iqr_before,
            "TEMP_iqr_during": temp_iqr_during,
            "TEMP_iqr_after": temp_iqr_after,

            "VM_id": f"{acc_x}_{acc_y}_{acc_z}",
            "VM_sd_before": acc_magnitude_sd_before,
            "VM_sd_during": acc_magnitude_sd_during,
            "VM_sd_after": acc_magnitude_sd_after,
            "VM_mean_before": acc_magnitude_mean_before,
            "VM_mean_during": acc_magnitude_mean_during,
            "VM_mean_after": acc_magnitude_mean_after,
            "VM_median_before": acc_magnitude_median_before,
            "VM_median_during": acc_magnitude_median_during,
            "VM_median_after": acc_magnitude_median_after,
            "VM_min_before": acc_magnitude_min_before,
            "VM_min_during": acc_magnitude_min_during,
            "VM_min_after": acc_magnitude_min_after,
            "VM_max_before": acc_magnitude_max_before,
            "VM_max_during": acc_magnitude_max_during,
            "VM_max_after": acc_magnitude_max_after,
            "VM_range_before": acc_magnitude_range_before,
            "VM_range_during": acc_magnitude_range_during,
            "VM_range_after": acc_magnitude_range_after,
            "VM_1q_before": acc_magnitude_1q_before,
            "VM_1q_during": acc_magnitude_1q_during,
            "VM_1q_after": acc_magnitude_1q_after,
            "VM_3q_before": acc_magnitude_3q_before,
            "VM_3q_during": acc_magnitude_3q_during,
            "VM_3q_after": acc_magnitude_3q_after,
            "VM_iqr_before": acc_magnitude_iqr_before,
            "VM_iqr_during": acc_magnitude_iqr_during,
            "VM_iqr_after": acc_magnitude_iqr_after,
            
            "EDA_id": eda,
            "EDA_sd_before": eda_sd_before,
            "EDA_sd_during": eda_sd_during,
            "EDA_sd_after": eda_sd_after,
            "EDA_mean_before": eda_mean_before,
            "EDA_mean_during": eda_mean_during,
            "EDA_mean_after": eda_mean_after,
            "EDA_median_before": eda_median_before,
            "EDA_median_during": eda_median_during,
            "EDA_median_after": eda_median_after,
            "EDA_min_before": eda_min_before,
            "EDA_min_during": eda_min_during,
            "EDA_min_after": eda_min_after,
            "EDA_max_before": eda_max_before,
            "EDA_max_during": eda_max_during,
            "EDA_max_after": eda_max_after,
            "EDA_range_before": eda_range_before,
            "EDA_range_during": eda_range_during,
            "EDA_range_after": eda_range_after,
            "EDA_1q_before": eda_1q_before,
            "EDA_1q_during": eda_1q_during,
            "EDA_1q_after": eda_1q_after,
            "EDA_3q_before": eda_3q_before,
            "EDA_3q_during": eda_3q_during,
            "EDA_3q_after": eda_3q_after,
            "EDA_iqr_before": eda_iqr_before,
            "EDA_iqr_during": eda_iqr_during,
            "EDA_iqr_after": eda_iqr_after,
            "EDA_SCL_sd_before": eda_scl_sd_before,
            "EDA_SCL_sd_during": eda_scl_sd_during,
            "EDA_SCL_sd_after": eda_scl_sd_after,
            "EDA_SCL_mean_before": eda_scl_mean_before,
            "EDA_SCL_mean_during": eda_scl_mean_during,
            "EDA_SCL_mean_after": eda_scl_mean_after,
            "EDA_SCL_median_before": eda_scl_median_before,
            "EDA_SCL_median_during": eda_scl_median_during,
            "EDA_SCL_median_after": eda_scl_median_after,
            "EDA_SCL_min_before": eda_scl_min_before,
            "EDA_SCL_min_during": eda_scl_min_during,
            "EDA_SCL_min_after": eda_scl_min_after,
            "EDA_SCL_max_before": eda_scl_max_before,
            "EDA_SCL_max_during": eda_scl_max_during,
            "EDA_SCL_max_after": eda_scl_max_after,
            "EDA_SCL_range_before": eda_scl_range_before,
            "EDA_SCL_range_during": eda_scl_range_during,
            "EDA_SCL_range_after": eda_scl_range_after,
            "EDA_SCL_1q_before": eda_scl_1q_before,
            "EDA_SCL_1q_during": eda_scl_1q_during,
            "EDA_SCL_1q_after": eda_scl_1q_after,
            "EDA_SCL_3q_before": eda_scl_3q_before,
            "EDA_SCL_3q_during": eda_scl_3q_during,
            "EDA_SCL_3q_after": eda_scl_3q_after,
            "EDA_SCL_iqr_before": eda_scl_iqr_before,
            "EDA_SCL_iqr_during": eda_scl_iqr_during,
            "EDA_SCL_iqr_after": eda_scl_iqr_after,
            "EDA_SCR_peaks_before": eda_scr_peaks_before,
            "EDA_SCR_peaks_during": eda_scr_peaks_during,
            "EDA_SCR_peaks_after": eda_scr_peaks_after,
            "EDA_SCR_amplitude_sd_before": eda_scr_amplitude_sd_before,
            "EDA_SCR_amplitude_sd_during": eda_scr_amplitude_sd_during,
            "EDA_SCR_amplitude_sd_after": eda_scr_amplitude_sd_after,
            "EDA_SCR_amplitude_mean_before": eda_scr_amplitude_mean_before,
            "EDA_SCR_amplitude_mean_during": eda_scr_amplitude_mean_during,
            "EDA_SCR_amplitude_mean_after": eda_scr_amplitude_mean_after,
            "EDA_SCR_amplitude_median_before": eda_scr_amplitude_median_before,
            "EDA_SCR_amplitude_median_during": eda_scr_amplitude_median_during,
            "EDA_SCR_amplitude_median_after": eda_scr_amplitude_median_after,
            "EDA_SCR_amplitude_min_before": eda_scr_amplitude_min_before,
            "EDA_SCR_amplitude_min_during": eda_scr_amplitude_min_during,
            "EDA_SCR_amplitude_min_after": eda_scr_amplitude_min_after,
            "EDA_SCR_amplitude_max_before": eda_scr_amplitude_max_before,
            "EDA_SCR_amplitude_max_during": eda_scr_amplitude_max_during,
            "EDA_SCR_amplitude_max_after": eda_scr_amplitude_max_after,
            "EDA_SCR_amplitude_range_before": eda_scr_amplitude_range_before,
            "EDA_SCR_amplitude_range_during": eda_scr_amplitude_range_during,
            "EDA_SCR_amplitude_range_after": eda_scr_amplitude_range_after,
            "EDA_SCR_amplitude_1q_before": eda_scr_amplitude_1q_before,
            "EDA_SCR_amplitude_1q_during": eda_scr_amplitude_1q_during,
            "EDA_SCR_amplitude_1q_after": eda_scr_amplitude_1q_after,
            "EDA_SCR_amplitude_3q_before": eda_scr_amplitude_3q_before,
            "EDA_SCR_amplitude_3q_during": eda_scr_amplitude_3q_during,
            "EDA_SCR_amplitude_3q_after": eda_scr_amplitude_3q_after,
            "EDA_SCR_amplitude_iqr_before": eda_scr_amplitude_iqr_before,
            "EDA_SCR_amplitude_iqr_during": eda_scr_amplitude_iqr_during,
            "EDA_SCR_amplitude_iqr_after": eda_scr_amplitude_iqr_after
        }
        

        # Append the stats to a CSV file
        df = pd.DataFrame([stats])

        # Append the IBI data if it exists
        if 'ibi_data_before' in locals():
            df = pd.concat([df, ibi_data_before], axis=1)
        if 'ibi_data_during' in locals():
            df = pd.concat([df, ibi_data_during], axis=1)
        if 'ibi_data_after' in locals():
            df = pd.concat([df, ibi_data_after], axis=1)
        # queue.put(df)
        cursor.close()
        conn.close()
        return df



if __name__ == "__main__":
    main()

