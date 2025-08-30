from dataclasses import dataclass
from typing import List, Literal, Tuple, Type, Dict
from datetime import datetime, timedelta
from RXLDBC import connect

import pandas as pd
import numpy as np
import neurokit2 as nk

MEASUREMENT_TYPES = Literal["IBI", "EDA", "EDA_scl", "EDA_scr", "BVP", "VM", "TEMP", "HR"]

@dataclass
class MinuteData:
    """
    Class to hold data for a minute of measurement data.
    """
    start_timestamp: datetime = None
    end_timestamp: datetime = None
    minute: int = None
    period : int = None
    hr_data: List[float] = None
    bvp_data: List[float] = None
    temp_data: List[float] = None
    acc_x_data: List[float] = None
    acc_y_data: List[float] = None
    acc_z_data: List[float] = None
    eda_data: List[float] = None
    ibi_data: List[float] = None
    ibi_time_data: List[float] = None

@dataclass
class MinuteStats:
    """
    Class to hold statistics for a minute of measurement data.
    """
    start_timestamp: datetime = None
    end_timestamp: datetime = None
    minute: int = None
    period : int = None
    hr_stats: pd.DataFrame = None
    bvp_stats: pd.DataFrame = None
    temp_stats: pd.DataFrame = None
    vm_stats: pd.DataFrame = None
    eda_stats: pd.DataFrame = None
    ibi_stats: pd.DataFrame = None

@dataclass
class DataTimestamp:
    """
    Class to hold a data session id and its start and end timestamps.
    """
    session_id: str = None
    start_timestamp: datetime = None
    end_timestamp: datetime = None

@dataclass
class SessionData:
    """
    Class to hold statistics for a session of measurement data.
    """
    patient_id: str = None
    patient_group: str = None
    patient_origin: str = None
    patient_age: int = None
    patient_sex: str = None
    patient_mean_relax_duration: float = None
    patient_relax_count: int = None

    relax_id: str = None
    relax_start_timestamp: datetime = None
    relax_end_timestamp: datetime = None
    relax_duration: int = None
    relax_moment: str = None
    relax_week: int = None

    q_ontspanning_start: int = None
    q_ontspanning_end: int = None
    q_kalm_start: int = None
    q_kalm_end: int = None

    hr: DataTimestamp = None
    bvp: DataTimestamp = None
    temp: DataTimestamp = None
    acc_x: DataTimestamp = None
    acc_y: DataTimestamp = None
    acc_z: DataTimestamp = None
    eda: DataTimestamp = None
    ibi: DataTimestamp = None


def calculate_regular_stats(data: List,
                            measurement_type: MEASUREMENT_TYPES) -> pd.DataFrame:
    """
    Calculate regular statistics for a given measurement type and period.
    :param data: List of data points for the measurement.
    :param measurement_type: str
        Type of measurement (e.g., "IBI", "EDA", "BVP", "VM", "TEMP", "HR").
    :return: pd.DataFrame
        DataFrame containing the calculated statistics.
    """

    stats = {
        f"{measurement_type}_std": np.std(data),
        f"{measurement_type}_mean": np.mean(data),
        f"{measurement_type}_median": np.median(data),
        f"{measurement_type}_min": np.min(data),
        f"{measurement_type}_max": np.max(data),
        f"{measurement_type}_range": np.max(data) - np.min(data),
        f"{measurement_type}_var": np.var(data),
        f"{measurement_type}_1q": np.percentile(data, 25),
        f"{measurement_type}_3q": np.percentile(data, 75),
        f"{measurement_type}_iqr": np.percentile(data, 75) - np.percentile(data, 25)}

    return pd.DataFrame([stats])

def calculate_eda_stats(data: List[float]) -> pd.DataFrame:
    """
    Calculate EDA statistics for a given period.
    :param data: List of EDA data points.
    :return: pd.DataFrame
        DataFrame containing the calculated EDA statistics.
    """

    # Calculate regular statistics for EDA
    eda_stats = calculate_regular_stats(data, "EDA")

    # Ensure data is a 1D array
    data = np.array(data).ravel()

    # If the array only contains zeros, return empty statistics
    if np.all(data == 0):
        return eda_stats

    # Process EDA data using NeuroKit2
    signals, info = nk.eda_process(data, sampling_rate=8, method='neurokit')

    # Calculate statistics for EDA tonic component
    eda_scl_stats = calculate_regular_stats(signals['EDA_Tonic'], "EDA_scl")

    # Calculate statistics for EDA phasic component
    eda_scr_stats = calculate_regular_stats(info['SCR_Amplitude'], "EDA_scr")

    # Add additional statistics for EDA phasic component
    eda_scr_stats[f"EDA_scr_peaks"] = len(np.where(signals["SCR_Peaks"])[0])

    # Combine all EDA statistics into a single DataFrame
    eda_combined_stats = pd.concat([eda_stats, eda_scl_stats, eda_scr_stats], axis=1)

    return eda_combined_stats

def calculate_ibi_stats(ibi_data: List[float],
                        time_data: List[float]) -> pd.DataFrame:
    """
    Calculate IBI statistics for a given period.
    :param ibi_data: List of IBI data points.
    :param time_data: List of timestamps corresponding to the IBI data.
    :return: pd.DataFrame
        DataFrame containing the calculated IBI statistics.
    """
    # Calculate HRV from IBI data
    hrv_data = nk.hrv(nk.intervals_to_peaks(ibi_data, time_data, sampling_rate=64), sampling_rate=64)

    return hrv_data

def calculate_minute_stats(minute_data: MinuteData) -> MinuteStats:
    """
    Calculate statistics for a minute of measurement data.
    :param minute_data: MinuteStats
        Object containing lists of measurement data for the minute.
    :return: MinuteStats
        Object containing DataFrames with calculated statistics for each measurement type.
    """

    minute_stats = MinuteStats()

    if minute_data.hr_data:
        minute_stats.hr_stats = calculate_regular_stats(minute_data.hr_data, "HR")
    if minute_data.bvp_data:
        minute_stats.bvp_stats = calculate_regular_stats(minute_data.bvp_data, "BVP")
    if minute_data.temp_data:
        minute_stats.temp_stats = calculate_regular_stats(minute_data.temp_data, "TEMP")
    if minute_data.acc_x_data and minute_data.acc_y_data and minute_data.acc_z_data:
        vm_data = np.sqrt(np.array(minute_data.acc_x_data) ** 2 +
                          np.array(minute_data.acc_y_data) ** 2 +
                          np.array(minute_data.acc_z_data) ** 2)
        minute_stats.vm_stats = calculate_regular_stats(vm_data, "VM")
    if minute_data.eda_data:
        minute_stats.eda_stats = calculate_eda_stats(minute_data.eda_data)
    if minute_data.ibi_data and minute_data.ibi_time_data:
        minute_stats.ibi_stats = calculate_ibi_stats(minute_data.ibi_data, minute_data.ibi_time_data)

    # Set timestamps and minute number
    minute_stats.start_timestamp = minute_data.start_timestamp


    return minute_stats

def process_minute_data(minute_data: Dict[str, MinuteData]) -> pd.DataFrame:
    """
    Process minute data and calculate statistics for each minute.
    :param minute_data: Dict[str, MinuteData]
        Dictionary with minute indices as keys and MinuteData objects as values.
    :return: pd.DataFrame
        DataFrame containing the calculated statistics for each minute.
    """
    minute_stats = []
    dataframes = []
    for key, data in minute_data.items():
        stats = calculate_minute_stats(data)
        stats.start_timestamp = data.start_timestamp
        stats.end_timestamp = data.end_timestamp
        stats.minute = data.minute
        stats.period = data.period
        minute_stats.append(stats)

    for stats in minute_stats:
        stat = {
            "start_timestamp": stats.start_timestamp,
            "end_timestamp": stats.end_timestamp,
            "minute": stats.minute,
            "period": stats.period
        }
        df = pd.DataFrame([stat])
        for attr in ["hr_stats", "bvp_stats", "temp_stats", "vm_stats", "eda_stats", "ibi_stats"]:
            if getattr(stats, attr) is not None:
                df = pd.concat([df, getattr(stats, attr)], axis=1)
        dataframes.append(df)

    combined = pd.concat(dataframes)

    return combined

def calculate_relax_session_data(relax_session: Tuple[str, List[dict[str: tuple[datetime, datetime]]]]) -> SessionData:
    """
    Calculate statistics for a relaxation session.
    :param relax_session: Tuple containing session ID and session data.
    :return: SessionStats object
    """
    session_stats = SessionData()
    conn = connect.Connection()
    cursor = conn.conn.cursor()

    relax_session_id, session_data = relax_session

    relax_id = relax_session[0].split("_")[2]

    cursor.execute(
        "SELECT patient_id, start_timestamp, end_timestamp, ontspanning_start, ontspanning_eind, kalm_start, kalm_eind FROM relax_session WHERE id = %s",
        (relax_id,))
    patient_id, start_timestamp, end_timestamp, ontspanning_start, ontspanning_eind, kalm_start, kalm_eind = cursor.fetchone()

    session_stats.patient_id = patient_id
    session_stats.relax_id = relax_id
    session_stats.relax_week = int(str(relax_session[1][0].keys()).split("_")[3])
    session_stats.relax_start_timestamp = start_timestamp
    session_stats.relax_end_timestamp = end_timestamp
    session_stats.relax_duration = (end_timestamp - start_timestamp).total_seconds()  # in seconds
    session_stats.q_ontspanning_start = ontspanning_start
    session_stats.q_ontspanning_end = ontspanning_eind
    session_stats.q_kalm_start = kalm_start
    session_stats.q_kalm_end = kalm_eind

    # Categorize the relaxation session start timestamp into morning, afternoon, evening, or night
    if 6 < start_timestamp.hour < 12:
        relaxation_time = "morning"
    elif 12 <= start_timestamp.hour < 18:
        relaxation_time = "afternoon"
    elif 18 <= start_timestamp.hour < 24:
        relaxation_time = "evening"
    else:
        relaxation_time = "night"

    session_stats.relax_moment = relaxation_time

    # Get the patient info from the database
    cursor.execute("SELECT origin, patient_group, age, sex FROM patient WHERE id = %s", (patient_id,))
    origin, patient_group, age, sex = cursor.fetchone()

    session_stats.patient_origin = origin
    session_stats.patient_group = patient_group
    session_stats.patient_age = age
    session_stats.patient_sex = sex

    # Get the amount of relaxation sessions for this patient
    cursor.execute("SELECT COUNT(*) FROM relax_session WHERE patient_id = %s", (patient_id,))
    relax_count = cursor.fetchone()[0]

    session_stats.patient_relax_count = relax_count

    # Get the average duration of relaxation sessions for this patient in seconds
    cursor.execute("SELECT start_timestamp, end_timestamp FROM relax_session WHERE patient_id = %s", (patient_id,))
    relax_sessions = cursor.fetchall()
    relax_durations = [(end - start).total_seconds() for start, end in relax_sessions]
    avg_relax_duration = sum(relax_durations) / len(relax_durations) if relax_durations else 0

    session_stats.patient_mean_relax_duration = avg_relax_duration

    for measurement in session_data:
        for measure_id, (start, end) in measurement.items():
            measurement_type = measure_id.split("_")[-2]
            bare_id = measure_id.split("_")[-1]
            if measurement_type == "HR":
                session_stats.hr = DataTimestamp(
                    session_id=bare_id,
                    start_timestamp=start,
                    end_timestamp=end
                )
            elif measurement_type == "BVP":
                session_stats.bvp = DataTimestamp(
                    session_id=bare_id,
                    start_timestamp=start,
                    end_timestamp=end
                )
            elif measurement_type == "TEMP":
                session_stats.temp = DataTimestamp(
                    session_id=bare_id,
                    start_timestamp=start,
                    end_timestamp=end
                )
            elif measurement_type == "X":
                session_stats.acc_x = DataTimestamp(
                    session_id=bare_id,
                    start_timestamp=start,
                    end_timestamp=end
                )
            elif measurement_type == "Y":
                session_stats.acc_y = DataTimestamp(
                    session_id=bare_id,
                    start_timestamp=start,
                    end_timestamp=end
                )
            elif measurement_type == "Z":
                session_stats.acc_z = DataTimestamp(
                    session_id=bare_id,
                    start_timestamp=start,
                    end_timestamp=end
                )
            elif measurement_type == "EDA":
                session_stats.eda = DataTimestamp(
                    session_id=bare_id,
                    start_timestamp=start,
                    end_timestamp=end
                )
            elif measurement_type == "IBI":
                session_stats.ibi = DataTimestamp(
                    session_id=bare_id,
                    start_timestamp=start,
                    end_timestamp=end
                )

    conn.close()
    return session_stats

def calculate_slices(relax_start_time: datetime,
                     relax_end_time: datetime,
                     measure_start_time: datetime,
                     sample_rate: int) -> Tuple:
    """
    Calculate the slices of time for a given period.
    :param relax_start_time: datetime
        The start time of the session.
    :param relax_end_time: datetime
        The end time of the session.
    :param measure_start_time: datetime
        The start time of the measurement.
    :param sample_rate: int
        The sample rate in Hz (samples per second).
    :return: Tuple
        A tuple containing three lists: slices_before, slices_during, and slices_after.
        Each list contains indices for the respective time periods.
    """

    relax_index_start = int((relax_start_time - measure_start_time).total_seconds()) * sample_rate

    relax_index_end = int((relax_end_time - measure_start_time).total_seconds()) * sample_rate

    min_before_index = relax_index_start - (5 * 60 * sample_rate)
    min_after_index = relax_index_end + (6 * 60 * sample_rate)
    # Create a list of indices for each minute in the session
    slices_before = [i for i in range(min_before_index, relax_index_start, 60 * sample_rate)]
    slices_before.append(relax_index_start)
    # Sort the before slices in ascending order
    slices_before.sort()

    # Divide the during period into 60-second slices, add the left over part to the last slice
    # Create a list of indices for each minute in the session
    slices_during = [i for i in range(relax_index_start, relax_index_end, 60 * sample_rate)]
    slices_during.pop(-1)
    slices_during.append(relax_index_end)

    slices_after = [i for i in range(relax_index_end, min_after_index, 60 * sample_rate)]
    # Prepend the end index to the after slices
    # Sort the after slices in ascending order
    slices_after.sort()

    return slices_before, slices_during, slices_after

def find_closest_index_ibi(hr_slices: Tuple[List[int], List[int], List[int]],
                           ibi_id: str
                           ) -> tuple[list[int], list[int], list[int]] | None:

    """
    Find the closest IBI index to the HR slices.
    :param hr_slices: Tuple[List[int], List[int], List[int]]
        A tuple containing three lists: slices_before, slices_during, and slices_after.
    :param ibi_id: str
        The ID of the IBI measurement session.
    :return: Tuple
        A tuple containing three lists: slices_before, slices_during, and slices_after for IBI data.
    """
    conn = connect.Connection()
    cursor = conn.conn.cursor()

    cursor.execute("SELECT data FROM measure_session WHERE id = %s", (ibi_id,))
    ibi_data = cursor.fetchone()[0]

    time_sequence = [entry[0] for entry in ibi_data]

    # Find the closest value in time_sequence for each slice in hr_slices
    def process_slices(slices):
        result = []
        for slic in slices:
            closest_index = min(range(len(time_sequence)), key=lambda i: abs(time_sequence[i] - slic))
            if abs(time_sequence[closest_index] - slic) > 50:
                print(
                    f"\033[91mError: Closest IBI index for slice {slic} is more than 50 seconds away: {time_sequence[closest_index]}\033[0m")
                return None
            result.append(closest_index)
        return result

    slices_before = process_slices(hr_slices[0])
    if slices_before is None:
        return None

    slices_during = process_slices(hr_slices[1])
    if slices_during is None:
        return None

    slices_after = process_slices(hr_slices[2])
    if slices_after is None:
        return None

    # Check if there are duplicate indices between slices_before, slices_during, and slices_after
    all_slices = slices_before + slices_during + slices_after
    if len(all_slices) != len(set(all_slices)):
        print("\033[91Error: Duplicate indices found in IBI slices.\033[0m")
        return None

    conn.close()
    return slices_before, slices_during, slices_after

def get_minute_data(session_data: SessionData) -> Dict[str, MinuteData]:
    """
    Slice a session into minute intervals and return a list of MinuteData objects.
    :param session_data: SessionData
        Object containing session data.
    :return: Dict[str, MinuteData]
        Dictionary with minute indices as keys and MinuteData objects as values.
    """

    conn = connect.Connection()

    minutes = {}

    if session_data.hr:
        hr_slices = calculate_slices(session_data.relax_start_timestamp,
                                     session_data.relax_end_timestamp,
                                     session_data.hr.start_timestamp,
                                     1)
    if session_data.bvp:
        bvp_slices = calculate_slices(session_data.relax_start_timestamp,
                                      session_data.relax_end_timestamp,
                                      session_data.bvp.start_timestamp,
                                      64)
    if session_data.temp:
        temp_slices = calculate_slices(session_data.relax_start_timestamp,
                                       session_data.relax_end_timestamp,
                                       session_data.temp.start_timestamp,
                                       4)
    if session_data.acc_x:
        acc_x_slices = calculate_slices(session_data.relax_start_timestamp,
                                        session_data.relax_end_timestamp,
                                        session_data.acc_x.start_timestamp,
                                        32)
    if session_data.acc_y:
        acc_y_slices = calculate_slices(session_data.relax_start_timestamp,
                                        session_data.relax_end_timestamp,
                                        session_data.acc_y.start_timestamp,
                                        32)
    if session_data.acc_z:
        acc_z_slices = calculate_slices(session_data.relax_start_timestamp,
                                        session_data.relax_end_timestamp,
                                        session_data.acc_z.start_timestamp,
                                        32)
    if session_data.eda:
        eda_slices = calculate_slices(session_data.relax_start_timestamp,
                                      session_data.relax_end_timestamp,
                                      session_data.eda.start_timestamp,
                                      4)
    if session_data.ibi:
        # Print yellow warning if IBI data is not available
        if not session_data.ibi.session_id:
            print(f"\033[93mWarning: IBI data is not available for session { session_data.relax_id } .\033[0m")
        else:
            ibi_slices = find_closest_index_ibi(hr_slices,
                                                session_data.ibi.session_id)
    for hr_slice_index, hr_slice in enumerate(hr_slices):
        for index, slic in enumerate(hr_slices[hr_slice_index][:-1]):
            minute_data = MinuteData(
                start_timestamp=session_data.hr.start_timestamp + pd.Timedelta(seconds=slic),
                end_timestamp=session_data.hr.start_timestamp + pd.Timedelta(seconds=hr_slices[hr_slice_index][index+1]),
                minute=index,
                period=hr_slice_index,
                hr_data=conn.get_data_from_measure_session_with_index(session_data.hr.session_id,
                                                                      slic,
                                                                      hr_slices[hr_slice_index][index + 1]) if session_data.hr else None,
                bvp_data=conn.get_data_from_measure_session_with_index(session_data.bvp.session_id,
                                                                       bvp_slices[hr_slice_index][index],
                                                                       bvp_slices[hr_slice_index][index + 1]) if session_data.bvp else None,
                temp_data=conn.get_data_from_measure_session_with_index(session_data.temp.session_id,
                                                                        temp_slices[hr_slice_index][index],
                                                                        temp_slices[hr_slice_index][index + 1]) if session_data.temp else None,
                acc_x_data=conn.get_data_from_measure_session_with_index(session_data.acc_x.session_id,
                                                                         acc_x_slices[hr_slice_index][index],
                                                                         acc_x_slices[hr_slice_index][index + 1]) if session_data.acc_x else None,
                acc_y_data=conn.get_data_from_measure_session_with_index(session_data.acc_y.session_id,
                                                                         acc_y_slices[hr_slice_index][index],
                                                                         acc_y_slices[hr_slice_index][index + 1]) if session_data.acc_y else None,
                acc_z_data=conn.get_data_from_measure_session_with_index(session_data.acc_z.session_id,
                                                                         acc_z_slices[hr_slice_index][index],
                                                                         acc_z_slices[hr_slice_index][index + 1]) if session_data.acc_z else None,
                eda_data=conn.get_data_from_measure_session_with_index(session_data.eda.session_id,
                                                                       eda_slices[hr_slice_index][index],
                                                                       eda_slices[hr_slice_index][index + 1]) if session_data.eda else None
            )
            if session_data.ibi and ibi_slices:
                ibi = conn.get_data_from_measure_session_with_index(session_data.ibi.session_id, ibi_slices[0][index],
                                                                    ibi_slices[0][index + 1])
                if ibi:
                    minute_data.ibi_data = [round(entry[1] * 1000) for entry in ibi]
                    minute_data.ibi_time_data = [entry[0] for entry in ibi]
            minutes[f"{session_data.relax_id}_{hr_slice_index}_{index}"] = minute_data

    conn.close()
    return minutes

def filter_5min_of_e4_before_and_after_relax_sessions(e4_timestamps, relax_timestamps) -> Dict[str, List[Dict[str, Tuple[datetime, datetime]]]]:
    conn = connect.Connection()
    filtered_sessions = {}
    for session_id, (start, end) in e4_timestamps.items():
        for relax_id, (relax_start, relax_end) in relax_timestamps.items():
            print(f"\033[93mChecking session {session_id} for relax session {relax_id}\033[0m")
            if start <= relax_start - timedelta(minutes=5) and end >= relax_end + timedelta(minutes=5):
                sample_rate = conn.get_sample_rate_from_measurement_session_id(session_id.split("_")[-1])
                start_index = int((relax_start - start).total_seconds() * sample_rate)
                end_index = int((relax_end - start).total_seconds() * sample_rate)
                minus_5_mins = start_index - 5 * (60 * sample_rate)
                plus_5_mins = end_index + 5 * (60 * sample_rate)
                invalid_indices = conn.get_invalid_data_indices_from_measure_session(session_id.split("_")[-1])
                if invalid_indices:
                    total_data_points = plus_5_mins - minus_5_mins
                    invalid_data_points = 0
                    # Print all invalid indices
                    print(f"Invalid indices for {session_id}: {invalid_indices}")
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
                    print(
                        f"Invalid data points for {session_id}: {invalid_data_points} out of {total_data_points} total data points.")
                    print(
                        f"Invalid data points percentage for {session_id}: {invalid_data_points / total_data_points * 100:.2f}%")
                    if invalid_data_points / total_data_points > 0.2:
                        # Print in blue that more than 20% of the data within 5 min before and after the relaxation session is invalid
                        print(
                            f"\033[94mMore than 20% of the data within 5 min before and after the relaxation session is invalid for {session_id}. Skipping...\033[0m")
                        print(relax_id)
                        continue
                    else:
                        if relax_id not in filtered_sessions:
                            print(f"\033[92mSession {session_id} has valid data within 5 min before and after the relaxation session {relax_id}.\033[0m")
                            filtered_sessions[relax_id] = [{session_id: (start, end)}]
                        else:
                            print(f"\033[92mSession {session_id} has valid data within 5 min before and after the relaxation session {relax_id}.\033[0m")
                            filtered_sessions[relax_id].append({session_id: (start, end)})
                else:
                    if relax_id not in filtered_sessions:
                        print(f"\033[92mSession {session_id} has valid data within 5 min before and after the relaxation session {relax_id}.\033[0m")
                        filtered_sessions[relax_id] = [{session_id: (start, end)}]
                    else:
                        print(f"\033[92mSession {session_id} has valid data within 5 min before and after the relaxation session {relax_id}.\033[0m")
                        filtered_sessions[relax_id].append({session_id: (start, end)})
            else:
                print(f"\033[91mSession {session_id} does not have 5 minutes before and after the relaxation session {relax_id}. Skipping...\033[0m")
                if relax_id.split('_')[-1] == '25659' and session_id.split('_')[-1] == '3080':
                    print(f"\033[91mRelax session {relax_id} has no valid data. Skipping...\033[0m")

    # Remove relax sessions that have fewer than 7 valid measurement sessions
    filtered_sessions = {relax_id: sessions for relax_id, sessions in filtered_sessions.items() if len(sessions) >= 7}

    return filtered_sessions

def run(patient_id, relax_id, session_data) -> None:
    """
    Run the processing for a specific relaxation session.
    :param patient_id: str
        The ID of the patient.
    :param relax_id: str
        The ID of the relaxation session.
    :param session_data: List[Dict[str, Tuple[datetime, datetime]]]
        The session data containing measurement IDs and their start and end timestamps.
    """
    print(f"Processing relax session {relax_id} for patient {patient_id}")
    relax_session = (relax_id, session_data)
    print(f"Calculating statistics for relax session {relax_id} for patient {patient_id}")
    session_stats = calculate_relax_session_data(relax_session)
    print(f"Getting minute data for relax session {relax_id} for patient {patient_id}")
    minute_data = get_minute_data(session_stats)
    print(f"Processing minute data for relax session {relax_id} for patient {patient_id}")
    minute_stats = process_minute_data(minute_data)

    print(f"Saving statistics for relax session {relax_id} for patient {patient_id}")

    # Convert the session_stats to a DataFrame and save it
    session_df = pd.DataFrame([session_stats.__dict__])
    session_df.to_csv(f"minute_stats/{relax_id}_stats.csv", index=False)

    minute_stats.to_csv(f"minute_stats/{relax_id}_minutes.csv", index=False)

def main():
    """
    Main function to calculate statistics for all relaxation sessions.
    """
    conn = connect.Connection()
    cursor = conn.conn.cursor()

    cursor.execute("SET work_mem = '1GB'")

    cursor.execute("SELECT id FROM patient ORDER BY id")
    patient_ids = cursor.fetchall()

    for patient_id in patient_ids:
        patient_id = patient_id[0]
        e4_timestamps = conn.get_all_timestamps_from_patient_id(patient_id)
        relax_timestamps = conn.get_all_relax_sessions_from_patient_id(patient_id)

        filtered_relax_sessions = filter_5min_of_e4_before_and_after_relax_sessions(e4_timestamps, relax_timestamps)
        for relax_id, session_data in filtered_relax_sessions.items():
            run(patient_id, relax_id, session_data)

if __name__ == '__main__':
    main()
