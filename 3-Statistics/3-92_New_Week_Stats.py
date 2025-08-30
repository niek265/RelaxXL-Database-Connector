import numpy as np
import pandas as pd
import neurokit2 as nk

from RXLDBC import connect


def filter_week_data_by_patient(patient):
    """
    Filters the week data for a specific patient.

    Args:
        patient (str): The ID of the patient to filter data for.

    Returns:
        Two dictionaries containing the timestamps of E4 sessions for Week 1 and Week 2.
    """
    conn = connect.Connection()
    cursor = conn.conn.cursor()

    week1_timestamp_dict = {}
    # Get all E4 measurements for the patient in the first week
    cursor.execute("SELECT id FROM measurement WHERE patient_id = %s AND week = 'Week_1' order by id", (patient,))
    week1_e4_measurements = cursor.fetchall()
    # Get all E4 sessions for the patient in the first week
    for measurement_id in week1_e4_measurements:
        for session_id in conn.get_all_measurement_session_ids_from_measurement_id(measurement_id):
            timestamps = conn.get_start_and_end_timestamps_from_measure_session_valid_data(session_id)
            for start_timestamp, end_timestamp in timestamps[session_id]:
                # Store the start and end timestamps in the dictionary
                week1_timestamp_dict[f"{measurement_id}_{session_id}"] = (start_timestamp, end_timestamp)

    week2_timestamp_dict = {}
    # Get all E4 sessions for the patient in the last week
    cursor.execute("SELECT id FROM measurement WHERE patient_id = %s AND week = 'Week_2' order by id", (patient,))
    week2_e4_measurements = cursor.fetchall()
    for measurement_id in week2_e4_measurements:
        for session_id in conn.get_all_measurement_session_ids_from_measurement_id(measurement_id):
            timestamps = conn.get_start_and_end_timestamps_from_measure_session_valid_data(session_id)
            for start_timestamp, end_timestamp in timestamps[session_id]:
                # Store the start and end timestamps in the dictionary
                week2_timestamp_dict[f"{measurement_id}_{session_id}"] = (start_timestamp, end_timestamp)

    # Get the length of the data in hours for the first week
    week1_length = sum((end - start).total_seconds() / 3600 for start, end in week1_timestamp_dict.values())
    # Get the length of the data in hours for the last week
    week2_length = sum((end - start).total_seconds() / 3600 for start, end in week2_timestamp_dict.values())

    # If the length of the data in any week is more than 70 hours, the data is considered valid
    if week1_length > 80 and week2_length > 80:
        return week1_e4_measurements, week2_e4_measurements, week1_timestamp_dict, week2_timestamp_dict
    else:
        print(f"Patient {patient} does not have enough data for both weeks.")
        return None, None, None, None


def calculate_weekly_stats(week_measurement_list):
    """
    Calculates weekly statistics from the given timestamp dictionary.

    Args:
        week_measurement_list (list) : A list of measurement session IDs for the week.

    Returns:
        A dictionary with calculated statistics.
    """
    conn = connect.Connection()
    cursor = conn.conn.cursor()

    # Initialize statistics
    week_stats = WeekStats()
    hrv_stats = {}

    sessions_x = []
    sessions_y = []
    sessions_z = []

    for measurement_id in week_measurement_list:
        measurement_id = measurement_id[0]
        measurement_type = measurement_id.split("_")[-1]
        if measurement_type == 'HR':
            # Get all sessions for this measurement
            cursor.execute("SELECT id FROM measure_session WHERE measurement_id = %s order by start_timestamp",
                           (measurement_id,))
            sessions = cursor.fetchall()
            all_hr_data = []
            for session_id in sessions:
                data = conn.get_valid_data_from_measure_session(session_id)
                for value in data.values():
                    all_hr_data.extend(value)

            # Calculate the average heart rate
            week_stats.hr_sd = np.std(all_hr_data)
            week_stats.hr_mean = np.mean(all_hr_data)
            week_stats.hr_min = np.min(all_hr_data)
            week_stats.hr_max = np.max(all_hr_data)
            week_stats.hr_range = week_stats.hr_max - week_stats.hr_min
            week_stats.hr_1q = np.percentile(all_hr_data, 25)
            week_stats.hr_3q = np.percentile(all_hr_data, 75)
            week_stats.hr_iqr = week_stats.hr_3q - week_stats.hr_1q

        elif measurement_type == 'EDA':
            # Get all sessions for this measurement
            cursor.execute("SELECT id FROM measure_session WHERE measurement_id = %s order by start_timestamp",
                           (measurement_id,))
            eda_data_count = 0
            eda_invalid_count = 0
            sessions = cursor.fetchall()
            all_eda_data = []
            for session_id in sessions:
                data = conn.get_valid_data_from_measure_session(session_id)
                for ts, values in data.items():

                    if np.all(values == 0) or len(values) < 100:
                        continue


                    nk_data = np.array(values).ravel()

                    # Filter the data
                    new_nk_data = nk.eda_clean(nk_data, sampling_rate=8, method="neurokit")

                    if np.all(new_nk_data == 0.0):
                        continue




                    signals, info = nk.eda_process(new_nk_data, sampling_rate=8)

                    scl = signals["EDA_Tonic"]
                    amplitudes = info["SCR_Amplitude"]

                    # If the average SCL is above 0.2 or the average amplitude is above 0.03 we consider the data valid
                    if np.mean(scl) > 0.2 or np.mean(amplitudes) > 0.03:
                        eda_data_count += len(nk_data)
                        for value in data.values():
                            all_eda_data.extend(value)
                    else:
                        eda_invalid_count += len(nk_data)

            if len(all_eda_data) == 0:
                print(f"No valid EDA data found for measurement {measurement_id}. Skipping EDA statistics.")
                continue

            filtered_nk_data = nk.eda_clean(all_eda_data, sampling_rate=8, method="neurokit")
            signals, info = nk.eda_process(filtered_nk_data, sampling_rate=8)

            all_scl = signals["EDA_Tonic"]
            all_amplitudes = info["SCR_Amplitude"]

            # Calculate the average EDA
            week_stats.eda_sd = np.std(all_eda_data)
            week_stats.eda_mean = np.mean(all_eda_data)
            week_stats.eda_min = np.min(all_eda_data)
            week_stats.eda_max = np.max(all_eda_data)
            week_stats.eda_range = week_stats.eda_max - week_stats.eda_min
            week_stats.eda_1q = np.percentile(all_eda_data, 25)
            week_stats.eda_3q = np.percentile(all_eda_data, 75)
            week_stats.eda_iqr = week_stats.eda_3q - week_stats.eda_1q

            week_stats.eda_scl_sd = np.std(all_scl)
            week_stats.eda_scl_mean = np.mean(all_scl)
            week_stats.eda_scl_median = np.median(all_scl)
            week_stats.eda_scl_min = np.min(all_scl)
            week_stats.eda_scl_max = np.max(all_scl)
            week_stats.eda_scl_range = week_stats.eda_scl_max - week_stats.eda_scl_min
            week_stats.eda_scl_1q = np.percentile(all_scl, 25)
            week_stats.eda_scl_3q = np.percentile(all_scl, 75)
            week_stats.eda_scl_iqr = week_stats.eda_scl_3q - week_stats.eda_scl_1q

            week_stats.eda_scr_peaks = len(np.where(signals["SCR_Peaks"])[0])

            week_stats.eda_scr_amplitude_sd = np.std(all_amplitudes)
            week_stats.eda_scr_amplitude_mean = np.mean(all_amplitudes)
            week_stats.eda_scr_amplitude_median = np.median(all_amplitudes)
            week_stats.eda_scr_amplitude_min = np.min(all_amplitudes)
            week_stats.eda_scr_amplitude_max = np.max(all_amplitudes)
            week_stats.eda_scr_amplitude_range = week_stats.eda_scr_amplitude_max - week_stats.eda_scr_amplitude_min
            week_stats.eda_scr_amplitude_1q = np.percentile(all_amplitudes, 25)
            week_stats.eda_scr_amplitude_3q = np.percentile(all_amplitudes, 75)
            week_stats.eda_scr_amplitude_iqr = week_stats.eda_scr_amplitude_3q - week_stats.eda_scr_amplitude_1q
            # Calculate the percentage of valid EDA data
            if eda_data_count > 0 and eda_invalid_count > 0:
                week_stats.eda_valid_percentage = (eda_data_count / (eda_data_count + eda_invalid_count)) * 100
            else:
                week_stats.eda_valid_percentage = 100

        elif measurement_type == 'BVP':
            # Get all sessions for this measurement
            cursor.execute("SELECT id FROM measure_session WHERE measurement_id = %s order by start_timestamp",
                           (measurement_id,))
            sessions = cursor.fetchall()
            all_bvp_data = []
            for session_id in sessions:
                data = conn.get_valid_data_from_measure_session(session_id)
                for value in data.values():
                    all_bvp_data.extend(value)

            # Calculate the average BVP
            week_stats.bvp_sd = np.std(all_bvp_data)
            week_stats.bvp_mean = np.mean(all_bvp_data)
            week_stats.bvp_min = np.min(all_bvp_data)
            week_stats.bvp_max = np.max(all_bvp_data)
            week_stats.bvp_range = week_stats.bvp_max - week_stats.bvp_min
            week_stats.bvp_1q = np.percentile(all_bvp_data, 25)
            week_stats.bvp_3q = np.percentile(all_bvp_data, 75)
            week_stats.bvp_iqr = week_stats.bvp_3q - week_stats.bvp_1q

        elif measurement_type == 'TEMP':
            # Get all sessions for this measurement
            cursor.execute("SELECT id FROM measure_session WHERE measurement_id = %s order by start_timestamp",
                           (measurement_id,))
            sessions = cursor.fetchall()
            all_temp_data = []
            for session_id in sessions:
                data = conn.get_valid_data_from_measure_session(session_id)
                for value in data.values():
                    all_temp_data.extend(value)

            # Calculate the average temperature
            week_stats.temp_sd = np.std(all_temp_data)
            week_stats.temp_mean = np.mean(all_temp_data)
            week_stats.temp_min = np.min(all_temp_data)
            week_stats.temp_max = np.max(all_temp_data)
            week_stats.temp_range = week_stats.temp_max - week_stats.temp_min
            week_stats.temp_1q = np.percentile(all_temp_data, 25)
            week_stats.temp_3q = np.percentile(all_temp_data, 75)
            week_stats.temp_iqr = week_stats.temp_3q - week_stats.temp_1q

        elif measurement_type == "X":
            cursor.execute("SELECT id FROM measure_session WHERE measurement_id = %s order by start_timestamp",
                           (measurement_id,))
            sessions = cursor.fetchall()
            for session_id in sessions:
                data = conn.get_valid_data_from_measure_session(session_id)
                for value in data.values():
                    sessions_x.extend(value)
        elif measurement_type == "Y":
            cursor.execute("SELECT id FROM measure_session WHERE measurement_id = %s order by start_timestamp",
                           (measurement_id,))
            sessions = cursor.fetchall()
            for session_id in sessions:
                data = conn.get_valid_data_from_measure_session(session_id)
                for value in data.values():
                    sessions_y.extend(value)
        elif measurement_type == "Z":
            cursor.execute("SELECT id FROM measure_session WHERE measurement_id = %s order by start_timestamp",
                           (measurement_id,))
            sessions = cursor.fetchall()
            for session_id in sessions:
                data = conn.get_valid_data_from_measure_session(session_id)
                for value in data.values():
                    sessions_z.extend(value)

            # Calculate vectors of magnitude
            acc_x = np.array(sessions_x)
            acc_y = np.array(sessions_y)
            acc_z = np.array(sessions_z)
            acc_magnitude = np.sqrt(acc_x ** 2 + acc_y ** 2 + acc_z ** 2)

            week_stats.acc_magnitude_sd = np.std(acc_magnitude)
            week_stats.acc_magnitude_mean = np.mean(acc_magnitude)
            week_stats.acc_magnitude_min = np.min(acc_magnitude)
            week_stats.acc_magnitude_max = np.max(acc_magnitude)
            week_stats.acc_magnitude_range = week_stats.acc_magnitude_max - week_stats.acc_magnitude_min
            week_stats.acc_magnitude_1q = np.percentile(acc_magnitude, 25)
            week_stats.acc_magnitude_3q = np.percentile(acc_magnitude, 75)
            week_stats.acc_magnitude_iqr = week_stats.acc_magnitude_3q - week_stats.acc_magnitude_1q
        # elif measurement_type == "IBI":
        #     ibi_stats = []
        #     # Get all sessions for this measurement
        #     for session_id in conn.get_all_measurement_session_ids_from_measurement_id(measurement_id):
        #         values = conn.get_valid_data_from_measure_session(session_id).values()
        #         for index, data in enumerate(values):
        #             print(f"Processing IBI data for session {index+1} of {len(values)} in measurement {measurement_id}")
        #             ibi_data = [round(entry[1] * 1000) for entry in data]
        #             ibi_time_data = [entry[0] for entry in data]
        #             hrv_data = nk.hrv(nk.intervals_to_peaks(ibi_data, ibi_time_data))
        #             ibi_stats.append(hrv_data)
        #     # Loop through columns and calculate the mean of all dataframes in the list of the same column
        #     for column in ibi_stats[0].columns:
        #         column_data = [df[column] for df in ibi_stats if column in df.columns]
        #         if column_data:
        #             hrv_stats[f"{column}_sd"] = np.std(column_data)
        #             hrv_stats[f"{column}_mean"] =  np.mean(column_data)
        #             hrv_stats[f"{column}_min"] = np.min(column_data)
        #             hrv_stats[f"{column}_max"] = np.max(column_data)
        #             hrv_stats[f"{column}_range"] = hrv_stats[f"{column}_max"] - hrv_stats[f"{column}_min"]
        #             hrv_stats[f"{column}_1q"] = np.percentile(column_data, 25)
        #             hrv_stats[f"{column}_3q"] = np.percentile(column_data, 75)
        #             hrv_stats[f"{column}_iqr"] = hrv_stats[f"{column}_3q"] - hrv_stats[f"{column}_1q"]

    return week_stats


class WeekStats:
    def __init__(self):
        self.hr_sd = None
        self.hr_mean = None
        self.hr_min = None
        self.hr_max = None
        self.hr_range = None
        self.hr_1q = None
        self.hr_3q = None
        self.hr_iqr = None

        self.eda_sd = None
        self.eda_mean = None
        self.eda_min = None
        self.eda_max = None
        self.eda_range = None
        self.eda_1q = None
        self.eda_3q = None
        self.eda_iqr = None
        self.eda_scl_sd = None
        self.eda_scl_mean = None
        self.eda_scl_median = None
        self.eda_scl_min = None
        self.eda_scl_max = None
        self.eda_scl_range = None
        self.eda_scl_1q = None
        self.eda_scl_3q = None
        self.eda_scl_iqr = None
        self.eda_scr_peaks = None
        self.eda_scr_amplitude_sd = None
        self.eda_scr_amplitude_mean = None
        self.eda_scr_amplitude_median = None
        self.eda_scr_amplitude_min = None
        self.eda_scr_amplitude_max = None
        self.eda_scr_amplitude_range = None
        self.eda_scr_amplitude_1q = None
        self.eda_scr_amplitude_3q = None
        self.eda_scr_amplitude_iqr = None
        self.eda_valid_percentage = None

        self.bvp_sd = None
        self.bvp_mean = None
        self.bvp_min = None
        self.bvp_max = None
        self.bvp_range = None
        self.bvp_1q = None
        self.bvp_3q = None
        self.bvp_iqr = None

        self.temp_sd = None
        self.temp_mean = None
        self.temp_min = None
        self.temp_max = None
        self.temp_range = None
        self.temp_1q = None
        self.temp_3q = None
        self.temp_iqr = None

        self.acc_magnitude_sd = None
        self.acc_magnitude_mean = None
        self.acc_magnitude_min = None
        self.acc_magnitude_max = None
        self.acc_magnitude_range = None
        self.acc_magnitude_1q = None
        self.acc_magnitude_3q = None
        self.acc_magnitude_iqr = None


def main():
    conn = connect.Connection()
    cursor = conn.conn.cursor()
    cursor.execute("SELECT id FROM patient ORDER BY id")
    patient_ids = cursor.fetchall()

    dataframes = []
    #
    # patient_ids = [("H001",)]

    for patient_id in patient_ids:
        week1, week2, week1_timestamps, week2_timestamps = filter_week_data_by_patient(patient_id[0])
        if week1 and week2:
            patient_id = week1[0][0].split("_")[0]

            # Get the earliest timestamp for Week 1
            week1_start = min(start for start, _ in week1_timestamps.values())
            # Get the latest timestamp for Week 1
            week1_end = max(end for _, end in week1_timestamps.values())
            # Get the earliest timestamp for Week 2
            week2_start = min(start for start, _ in week2_timestamps.values())
            # Get the latest timestamp for Week 2
            week2_end = max(end for _, end in week2_timestamps.values())

            # Get the patient info from the database
            cursor.execute("SELECT origin, patient_group, age, sex FROM patient WHERE id = %s", (patient_id,))
            origin, patient_group, age, sex = cursor.fetchone()

            # Get the amount of relaxation sessions for this patient
            cursor.execute("SELECT COUNT(*) FROM relax_session WHERE patient_id = %s", (patient_id,))
            relax_count = cursor.fetchone()[0]

            # Get the amount of relaxation sessions for this patient in Week 1
            cursor.execute(
                "SELECT COUNT(*) FROM relax_session WHERE patient_id = %s AND start_timestamp >= %s AND end_timestamp <= %s",
                (patient_id, week1_start, week1_end))
            week1_relax_count = cursor.fetchone()[0]

            # Get the amount of relaxation sessions for this patient in Week 2
            cursor.execute(
                "SELECT COUNT(*) FROM relax_session WHERE patient_id = %s AND start_timestamp >= %s AND end_timestamp <= %s",
                (patient_id, week2_start, week2_end))
            week2_relax_count = cursor.fetchone()[0]

            # Get the average duration of relaxation sessions for this patient in seconds
            cursor.execute("SELECT start_timestamp, end_timestamp FROM relax_session WHERE patient_id = %s",
                           (patient_id,))
            relax_sessions = cursor.fetchall()
            relax_durations = [(end - start).total_seconds() for start, end in relax_sessions]
            avg_relax_duration = sum(relax_durations) / len(relax_durations) if relax_durations else 0

            # Get the average duration of relaxation sessions for this patient in Week 1 in seconds

            cursor.execute(
                "SELECT start_timestamp, end_timestamp FROM relax_session WHERE patient_id = %s AND start_timestamp >= %s AND end_timestamp <= %s",
                (patient_id, week1_start, week1_end))
            week1_relax_sessions = cursor.fetchall()
            week1_relax_durations = [(end - start).total_seconds() for start, end in week1_relax_sessions]
            avg_week1_relax_duration = sum(week1_relax_durations) / len(
                week1_relax_durations) if week1_relax_durations else 0

            # Get the average duration of relaxation sessions for this patient in Week 2 in seconds
            cursor.execute(
                "SELECT start_timestamp, end_timestamp FROM relax_session WHERE patient_id = %s AND start_timestamp >= %s AND end_timestamp <= %s",
                (patient_id, week2_start, week2_end))
            week2_relax_sessions = cursor.fetchall()
            week2_relax_durations = [(end - start).total_seconds() for start, end in week2_relax_sessions]
            avg_week2_relax_duration = sum(week2_relax_durations) / len(
                week2_relax_durations) if week2_relax_durations else 0

            # Get the total duration of the E4 sessions for this patient in seconds
            e4_sessions = list(week1_timestamps.values()) + list(week2_timestamps.values())
            e4_durations = [(end - start).total_seconds() for start, end in e4_sessions]

            # Get the week 1 and week 2 E4 durations
            week1_e4_durations = [(end - start).total_seconds() for start, end in week1_timestamps.values()]
            week2_e4_durations = [(end - start).total_seconds() for start, end in week2_timestamps.values()]

            total_unfiltered_e4_time = conn.get_total_e4_time_from_patient_id(patient_id)
            if total_unfiltered_e4_time > 0:
                # Calculate percentage of excluded E4 data
                excluded_percentage = (total_unfiltered_e4_time / sum(e4_durations)) * 100
            else:
                excluded_percentage = 100

            print(f"Processing data for patient {patient_id}")
            week1_stats = calculate_weekly_stats(week1)
            week2_stats = calculate_weekly_stats(week2)

            # # Rename columns of week1_hrv_stats and week2_hrv_stats to include week number
            # week1_hrv_stats = {f"{key}_week1": value for key, value in week1_hrv_stats.items()}
            # week2_hrv_stats = {f"{key}_week2": value for key, value in week2_hrv_stats.items()}

            stats = {
                "patient_id": patient_id,
                "patient_group": patient_group,
                "patient_origin": origin,
                "patient_age": age,
                "patient_sex": sex,
                "patient_mean_relax_duration": avg_relax_duration,
                "patient_mean_relax_duration_week1": avg_week1_relax_duration,
                "patient_mean_relax_duration_week2": avg_week2_relax_duration,
                "patient_relax_count": relax_count,
                "patient_relax_count_week1": week1_relax_count,
                "patient_relax_count_week2": week2_relax_count,
                "patient_total_relax_duration": sum(relax_durations),
                "patient_total_week1_relax_duration": sum(week1_relax_durations),
                "patient_total_week2_relax_duration": sum(week2_relax_durations),
                "patient_e4_total_duration": sum(e4_durations),
                "patient_week1_e4_duration": sum(week1_e4_durations),
                "patient_week2_e4_duration": sum(week2_e4_durations),
                "patient_excluded_e4_percentage": excluded_percentage,

                "HR_sd_week1": week1_stats.hr_sd,
                "HR_mean_week1": week1_stats.hr_mean,
                "HR_min_week1": week1_stats.hr_min,
                "HR_max_week1": week1_stats.hr_max,
                "HR_range_week1": week1_stats.hr_range,
                "HR_1q_week1": week1_stats.hr_1q,
                "HR_3q_week1": week1_stats.hr_3q,
                "HR_iqr_week1": week1_stats.hr_iqr,
                "HR_sd_week2": week2_stats.hr_sd,
                "HR_mean_week2": week2_stats.hr_mean,
                "HR_min_week2": week2_stats.hr_min,
                "HR_max_week2": week2_stats.hr_max,
                "HR_range_week2": week2_stats.hr_range,
                "HR_1q_week2": week2_stats.hr_1q,
                "HR_3q_week2": week2_stats.hr_3q,
                "HR_iqr_week2": week2_stats.hr_iqr,

                "BVP_sd_week1": week1_stats.bvp_sd,
                "BVP_mean_week1": week1_stats.bvp_mean,
                "BVP_min_week1": week1_stats.bvp_min,
                "BVP_max_week1": week1_stats.bvp_max,
                "BVP_range_week1": week1_stats.bvp_range,
                "BVP_1q_week1": week1_stats.bvp_1q,
                "BVP_3q_week1": week1_stats.bvp_3q,
                "BVP_iqr_week1": week1_stats.bvp_iqr,
                "BVP_sd_week2": week2_stats.bvp_sd,
                "BVP_mean_week2": week2_stats.bvp_mean,
                "BVP_min_week2": week2_stats.bvp_min,
                "BVP_max_week2": week2_stats.bvp_max,
                "BVP_range_week2": week2_stats.bvp_range,
                "BVP_1q_week2": week2_stats.bvp_1q,
                "BVP_3q_week2": week2_stats.bvp_3q,
                "BVP_iqr_week2": week2_stats.bvp_iqr,

                "TEMP_sd_week1": week1_stats.temp_sd,
                "TEMP_mean_week1": week1_stats.temp_mean,
                "TEMP_min_week1": week1_stats.temp_min,
                "TEMP_max_week1": week1_stats.temp_max,
                "TEMP_range_week1": week1_stats.temp_range,
                "TEMP_1q_week1": week1_stats.temp_1q,
                "TEMP_3q_week1": week1_stats.temp_3q,
                "TEMP_iqr_week1": week1_stats.temp_iqr,
                "TEMP_sd_week2": week2_stats.temp_sd,
                "TEMP_mean_week2": week2_stats.temp_mean,
                "TEMP_min_week2": week2_stats.temp_min,
                "TEMP_max_week2": week2_stats.temp_max,
                "TEMP_range_week2": week2_stats.temp_range,
                "TEMP_1q_week2": week2_stats.temp_1q,
                "TEMP_3q_week2": week2_stats.temp_3q,
                "TEMP_iqr_week2": week2_stats.temp_iqr,

                "VM_sd_week1": week1_stats.acc_magnitude_sd,
                "VM_mean_week1": week1_stats.acc_magnitude_mean,
                "VM_min_week1": week1_stats.acc_magnitude_min,
                "VM_max_week1": week1_stats.acc_magnitude_max,
                "VM_range_week1": week1_stats.acc_magnitude_range,
                "VM_1q_week1": week1_stats.acc_magnitude_1q,
                "VM_3q_week1": week1_stats.acc_magnitude_3q,
                "VM_iqr_week1": week1_stats.acc_magnitude_iqr,
                "VM_sd_week2": week2_stats.acc_magnitude_sd,
                "VM_mean_week2": week2_stats.acc_magnitude_mean,
                "VM_min_week2": week2_stats.acc_magnitude_min,
                "VM_max_week2": week2_stats.acc_magnitude_max,
                "VM_range_week2": week2_stats.acc_magnitude_range,
                "VM_1q_week2": week2_stats.acc_magnitude_1q,
                "VM_3q_week2": week2_stats.acc_magnitude_3q,
                "VM_iqr_week2": week2_stats.acc_magnitude_iqr,

                "EDA_sd_week1": week1_stats.eda_sd,
                "EDA_mean_week1": week1_stats.eda_mean,
                "EDA_min_week1": week1_stats.eda_min,
                "EDA_max_week1": week1_stats.eda_max,
                "EDA_range_week1": week1_stats.eda_range,
                "EDA_1q_week1": week1_stats.eda_1q,
                "EDA_3q_week1": week1_stats.eda_3q,
                "EDA_iqr_week1": week1_stats.eda_iqr,
                "EDA_scl_sd_week1": week1_stats.eda_scl_sd,
                "EDA_scl_mean_week1": week1_stats.eda_scl_mean,
                "EDA_scl_median_week1": week1_stats.eda_scl_median,
                "EDA_scl_min_week1": week1_stats.eda_scl_min,
                "EDA_scl_max_week1": week1_stats.eda_scl_max,
                "EDA_scl_range_week1": week1_stats.eda_scl_range,
                "EDA_scl_1q_week1": week1_stats.eda_scl_1q,
                "EDA_scl_3q_week1": week1_stats.eda_scl_3q,
                "EDA_scl_iqr_week1": week1_stats.eda_scl_iqr,
                "EDA_scr_peaks_week1": week1_stats.eda_scr_peaks,
                "EDA_scr_peaks_per_minute_week1": week1_stats.eda_scr_peaks / (
                    (week1_end - week1_start).total_seconds() / 60) if (week1_end - week1_start).total_seconds() > 0 else 0,
                "EDA_scr_amplitude_sd_week1": week1_stats.eda_scr_amplitude_sd,
                "EDA_scr_amplitude_mean_week1": week1_stats.eda_scr_amplitude_mean,
                "EDA_scr_amplitude_median_week1": week1_stats.eda_scr_amplitude_median,
                "EDA_scr_amplitude_min_week1": week1_stats.eda_scr_amplitude_min,
                "EDA_scr_amplitude_max_week1": week1_stats.eda_scr_amplitude_max,
                "EDA_scr_amplitude_range_week1": week1_stats.eda_scr_amplitude_range,
                "EDA_scr_amplitude_1q_week1": week1_stats.eda_scr_amplitude_1q,
                "EDA_scr_amplitude_3q_week1": week1_stats.eda_scr_amplitude_3q,
                "EDA_scr_amplitude_iqr_week1": week1_stats.eda_scr_amplitude_iqr,
                "EDA_valid_percentage_week1": week1_stats.eda_valid_percentage,
                "EDA_sd_week2": week2_stats.eda_sd,
                "EDA_mean_week2": week2_stats.eda_mean,
                "EDA_min_week2": week2_stats.eda_min,
                "EDA_max_week2": week2_stats.eda_max,
                "EDA_range_week2": week2_stats.eda_range,
                "EDA_1q_week2": week2_stats.eda_1q,
                "EDA_3q_week2": week2_stats.eda_3q,
                "EDA_iqr_week2": week2_stats.eda_iqr,
                "EDA_scl_sd_week2": week2_stats.eda_scl_sd,
                "EDA_scl_mean_week2": week2_stats.eda_scl_mean,
                "EDA_scl_median_week2": week2_stats.eda_scl_median,
                "EDA_scl_min_week2": week2_stats.eda_scl_min,
                "EDA_scl_max_week2": week2_stats.eda_scl_max,
                "EDA_scl_range_week2": week2_stats.eda_scl_range,
                "EDA_scl_1q_week2": week2_stats.eda_scl_1q,
                "EDA_scl_3q_week2": week2_stats.eda_scl_3q,
                "EDA_scl_iqr_week2": week2_stats.eda_scl_iqr,
                "EDA_scr_peaks_week2": week2_stats.eda_scr_peaks,
                "EDA_scr_peaks_per_minute_week2": week2_stats.eda_scr_peaks / (
                    (week2_end - week2_start).total_seconds() / 60) if (week2_end - week2_start).total_seconds() > 0 and week2_stats.eda_scr_peaks else 0,
                "EDA_scr_amplitude_sd_week2": week2_stats.eda_scr_amplitude_sd,
                "EDA_scr_amplitude_mean_week2": week2_stats.eda_scr_amplitude_mean,
                "EDA_scr_amplitude_median_week2": week2_stats.eda_scr_amplitude_median,
                "EDA_scr_amplitude_min_week2": week2_stats.eda_scr_amplitude_min,
                "EDA_scr_amplitude_max_week2": week2_stats.eda_scr_amplitude_max,
                "EDA_scr_amplitude_range_week2": week2_stats.eda_scr_amplitude_range,
                "EDA_scr_amplitude_1q_week2": week2_stats.eda_scr_amplitude_1q,
                "EDA_scr_amplitude_3q_week2": week2_stats.eda_scr_amplitude_3q,
                "EDA_scr_amplitude_iqr_week2": week2_stats.eda_scr_amplitude_iqr,
                "EDA_valid_percentage_week2": week2_stats.eda_valid_percentage
            }

            # # Add the HRV stats to the stats dictionary
            # stats.update(week1_hrv_stats)
            # stats.update(week2_hrv_stats)
            # Append the stats to a CSV file
            df = pd.DataFrame([stats])
            dataframes.append(df)

        else:
            print(f"No valid data for patient {patient_id[0]}")
    # Concatenate all dataframes and save to a CSV file
    if dataframes:
        final_df = pd.concat(dataframes, ignore_index=True)
        final_df.to_csv("new_stats_week.csv", header=True)
        print("Weekly statistics saved to stats_week.csv")
    else:
        print("No valid data to save.")


if __name__ == "__main__":
    main()
