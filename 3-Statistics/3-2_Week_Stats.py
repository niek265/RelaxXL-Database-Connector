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
            start_timestamp, end_timestamp = conn.get_beginning_and_end_timestamp_from_measure_session(session_id)
            week1_timestamp_dict[f"{measurement_id}_{session_id}"] = (start_timestamp, end_timestamp)


    week2_timestamp_dict = {}
    # Get all E4 sessions for the patient in the last week
    cursor.execute("SELECT id FROM measurement WHERE patient_id = %s AND week = 'Week_2' order by id", (patient,))
    week2_e4_measurements = cursor.fetchall()
    for measurement_id in week2_e4_measurements:
        for session_id in conn.get_all_measurement_session_ids_from_measurement_id(measurement_id):
            start_timestamp, end_timestamp = conn.get_beginning_and_end_timestamp_from_measure_session(session_id)
            week2_timestamp_dict[f"{measurement_id}_{session_id}"] = (start_timestamp, end_timestamp)

    # Get the length of the data in hours for the first week
    week1_length = sum((end - start).total_seconds() / 3600 for start, end in week1_timestamp_dict.values())
    # Get the length of the data in hours for the last week
    week2_length = sum((end - start).total_seconds() / 3600 for start, end in week2_timestamp_dict.values())

    # If the length of the data in any week is more than 70 hours, the data is considered valid
    if week1_length > 70 and week2_length > 70:
        return week1_e4_measurements, week2_e4_measurements
    else:
        print(f"Patient {patient} does not have enough data for both weeks.")
        return None, None




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

    sessions_x = []
    sessions_y = []
    sessions_z = []

    for measurement_id in week_measurement_list:
        measurement_id = measurement_id[0]
        measurement_type = measurement_id.split("_")[-1]
        if measurement_type == 'HR':
            # Get all sessions for this measurement
            cursor.execute("SELECT id, data FROM measure_session WHERE measurement_id = %s order by start_timestamp", (measurement_id,))
            sessions = cursor.fetchall()
            all_hr_data = []
            for session in sessions:
                session_id, data = session
                # Flatten the data if it's a list of lists
                if isinstance(data[0], list):
                    data = [sublist[0] for sublist in data]
                all_hr_data.extend(data)

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
            cursor.execute("SELECT id, data FROM measure_session WHERE measurement_id = %s order by start_timestamp", (measurement_id,))
            sessions = cursor.fetchall()
            all_eda_data = []
            for session in sessions:
                session_id, data = session
                # Flatten the data if it's a list of lists
                if isinstance(data[0], list):
                    data = [sublist[0] for sublist in data]
                all_eda_data.extend(data)

            # Calculate the average EDA
            week_stats.eda_sd = np.std(all_eda_data)
            week_stats.eda_mean = np.mean(all_eda_data)
            week_stats.eda_min = np.min(all_eda_data)
            week_stats.eda_max = np.max(all_eda_data)
            week_stats.eda_range = week_stats.eda_max - week_stats.eda_min
            week_stats.eda_1q = np.percentile(all_eda_data, 25)
            week_stats.eda_3q = np.percentile(all_eda_data, 75)
            week_stats.eda_iqr = week_stats.eda_3q - week_stats.eda_1q

            nk_data = np.array(all_eda_data).ravel()

            signals, info = nk.eda_process(nk_data, sampling_rate=4)

            scl = signals["EDA_Tonic"]
            week_stats.eda_scl_sd = np.std(scl)
            week_stats.eda_scl_mean = np.mean(scl)
            week_stats.eda_scl_median = np.median(scl)
            week_stats.eda_scl_min = np.min(scl)
            week_stats.eda_scl_max = np.max(scl)
            week_stats.eda_scl_range = week_stats.eda_scl_max - week_stats.eda_scl_min
            week_stats.eda_scl_1q = np.percentile(scl, 25)
            week_stats.eda_scl_3q = np.percentile(scl, 75)
            week_stats.eda_scl_iqr = week_stats.eda_scl_3q - week_stats.eda_scl_1q

            week_stats.eda_scr_peaks = len(np.where(signals["SCR_Peaks"])[0])

            amplitudes = info["SCR_Amplitude"]
            week_stats.eda_scr_amplitude_sd = np.std(amplitudes)
            week_stats.eda_scr_amplitude_mean = np.mean(amplitudes)
            week_stats.eda_scr_amplitude_median = np.median(amplitudes)
            week_stats.eda_scr_amplitude_min = np.min(amplitudes)
            week_stats.eda_scr_amplitude_max = np.max(amplitudes)
            week_stats.eda_scr_amplitude_range = week_stats.eda_scr_amplitude_max - week_stats.eda_scr_amplitude_min
            week_stats.eda_scr_amplitude_1q = np.percentile(amplitudes, 25)
            week_stats.eda_scr_amplitude_3q = np.percentile(amplitudes, 75)
            week_stats.eda_scr_amplitude_iqr = week_stats.eda_scr_amplitude_3q - week_stats.eda_scr_amplitude_1q

        elif measurement_type == 'BVP':
            # Get all sessions for this measurement
            cursor.execute("SELECT id, data FROM measure_session WHERE measurement_id = %s order by start_timestamp", (measurement_id,))
            sessions = cursor.fetchall()
            all_bvp_data = []
            for session in sessions:
                session_id, data = session
                # Flatten the data if it's a list of lists
                if isinstance(data[0], list):
                    data = [sublist[0] for sublist in data]
                all_bvp_data.extend(data)

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
            cursor.execute("SELECT id, data FROM measure_session WHERE measurement_id = %s order by start_timestamp", (measurement_id,))
            sessions = cursor.fetchall()
            all_temp_data = []
            for session in sessions:
                session_id, data = session
                # Flatten the data if it's a list of lists
                if isinstance(data[0], list):
                    data = [sublist[0] for sublist in data]
                all_temp_data.extend(data)

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
            cursor.execute("SELECT id, data FROM measure_session WHERE measurement_id = %s order by start_timestamp", (measurement_id,))
            sessions = cursor.fetchall()
            for session in sessions:
                session_id, data = session
                # Flatten the data if it's a list of lists
                if isinstance(data[0], list):
                    data = [sublist[1] for sublist in data]
                sessions_x.extend(data)
        elif measurement_type == "Y":
            cursor.execute("SELECT id, data FROM measure_session WHERE measurement_id = %s order by start_timestamp", (measurement_id,))
            sessions = cursor.fetchall()
            for session in sessions:
                session_id, data = session
                # Flatten the data if it's a list of lists
                if isinstance(data[0], list):
                    data = [sublist[1] for sublist in data]
                sessions_y.extend(data)
        elif measurement_type == "Z":
            cursor.execute("SELECT id, data FROM measure_session WHERE measurement_id = %s order by start_timestamp", (measurement_id,))
            sessions = cursor.fetchall()
            for session in sessions:
                session_id, data = session
                # Flatten the data if it's a list of lists
                if isinstance(data[0], list):
                    data = [sublist[1] for sublist in data]
                sessions_z.extend(data)

            # Calculate vectors of magnitude
            acc_x = np.array(sessions_x)
            acc_y = np.array(sessions_y)
            acc_z = np.array(sessions_z)
            acc_magnitude = np.sqrt(acc_x**2 + acc_y**2 + acc_z**2)

            week_stats.acc_magnitude_sd = np.std(acc_magnitude)
            week_stats.acc_magnitude_mean = np.mean(acc_magnitude)
            week_stats.acc_magnitude_min = np.min(acc_magnitude)
            week_stats.acc_magnitude_max = np.max(acc_magnitude)
            week_stats.acc_magnitude_range = week_stats.acc_magnitude_max - week_stats.acc_magnitude_min
            week_stats.acc_magnitude_1q = np.percentile(acc_magnitude, 25)
            week_stats.acc_magnitude_3q = np.percentile(acc_magnitude, 75)
            week_stats.acc_magnitude_iqr = week_stats.acc_magnitude_3q - week_stats.acc_magnitude_1q


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

    for patient_id in patient_ids:
        week1, week2 = filter_week_data_by_patient(patient_id[0])
        if week1 and week2:
            patient_id = week1[0][0].split("_")[0]
            # Get the patient info from the database
            cursor.execute("SELECT origin, patient_group, age, sex FROM patient WHERE id = %s", (patient_id,))
            origin, patient_group, age, sex = cursor.fetchone()

            # Get the amount of relaxation sessions for this patient
            cursor.execute("SELECT COUNT(*) FROM relax_session WHERE patient_id = %s", (patient_id,))
            relax_count = cursor.fetchone()[0]

            # Get the average duration of relaxation sessions for this patient in seconds
            cursor.execute("SELECT start_timestamp, end_timestamp FROM relax_session WHERE patient_id = %s",
                           (patient_id,))
            relax_sessions = cursor.fetchall()
            relax_durations = [(end - start).total_seconds() for start, end in relax_sessions]
            avg_relax_duration = sum(relax_durations) / len(relax_durations) if relax_durations else 0

            print(f"Processing data for patient {patient_id}")
            week1_stats = calculate_weekly_stats(week1)
            week2_stats = calculate_weekly_stats(week2)
            
            stats = {
                "patient_id": patient_id,
                "patient_group": patient_group,
                "patient_origin": origin,
                "patient_age": age,
                "patient_sex": sex,
                "patient_mean_relax_duration": avg_relax_duration,
                "patient_relax_count": relax_count,
            
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
                "EDA_scr_amplitude_sd_week1": week1_stats.eda_scr_amplitude_sd,
                "EDA_scr_amplitude_mean_week1": week1_stats.eda_scr_amplitude_mean,
                "EDA_scr_amplitude_median_week1": week1_stats.eda_scr_amplitude_median,
                "EDA_scr_amplitude_min_week1": week1_stats.eda_scr_amplitude_min,
                "EDA_scr_amplitude_max_week1": week1_stats.eda_scr_amplitude_max,
                "EDA_scr_amplitude_range_week1": week1_stats.eda_scr_amplitude_range,
                "EDA_scr_amplitude_1q_week1": week1_stats.eda_scr_amplitude_1q,
                "EDA_scr_amplitude_3q_week1": week1_stats.eda_scr_amplitude_3q,
                "EDA_scr_amplitude_iqr_week1": week1_stats.eda_scr_amplitude_iqr,
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
                "EDA_scr_amplitude_sd_week2": week2_stats.eda_scr_amplitude_sd,
                "EDA_scr_amplitude_mean_week2": week2_stats.eda_scr_amplitude_mean,
                "EDA_scr_amplitude_median_week2": week2_stats.eda_scr_amplitude_median,
                "EDA_scr_amplitude_min_week2": week2_stats.eda_scr_amplitude_min,
                "EDA_scr_amplitude_max_week2": week2_stats.eda_scr_amplitude_max,
                "EDA_scr_amplitude_range_week2": week2_stats.eda_scr_amplitude_range,
                "EDA_scr_amplitude_1q_week2": week2_stats.eda_scr_amplitude_1q,
                "EDA_scr_amplitude_3q_week2": week2_stats.eda_scr_amplitude_3q,
                "EDA_scr_amplitude_iqr_week2": week2_stats.eda_scr_amplitude_iqr
            }
            print(stats)
            # Append the stats to a CSV file
            df = pd.DataFrame([stats])
            dataframes.append(df)
        else:
            print(f"No valid data for patient {patient_id[0]}")
    # Concatenate all dataframes and save to a CSV file
    if dataframes:
        final_df = pd.concat(dataframes, ignore_index=True)
        final_df.to_csv("stats_week.csv", header=True)
        print("Weekly statistics saved to stats_week.csv")
    else:
        print("No valid data to save.")

if __name__ == "__main__":
    main()
