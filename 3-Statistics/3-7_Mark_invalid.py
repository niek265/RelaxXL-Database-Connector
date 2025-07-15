from RXLDBC import connect

from datetime import datetime, timedelta

import pandas as pd
import numpy as np

conn = connect.Connection()

total_length = 0
invalid_length = 0

def timestamp_ranges_to_index_ranges(
    timestamp_ranges,
    stream_start,
    sample_rate_hz,
    total_samples):
    """
    Convert timestamp ranges to index ranges based on sample rate.

    Parameters:
        timestamp_ranges: list of (start_ts, end_ts) tuples
        stream_start: the datetime when the sample stream started
        sample_rate_hz: number of samples per second (e.g., 32.0, 1.0)
        total_samples: number of samples in the stream

    Returns:
        A list of [start_index, end_index] ranges (clamped to bounds)
    """
    index_ranges = []

    for ts_start, ts_end in timestamp_ranges:
        offset_start = (ts_start - stream_start).total_seconds()
        offset_end = (ts_end - stream_start).total_seconds()

        index_start = int(round(offset_start * sample_rate_hz))
        index_end = int(round(offset_end * sample_rate_hz))

        # Clamp to list bounds
        index_start = max(0, min(index_start, total_samples - 1))
        index_end = max(0, min(index_end, total_samples - 1))

        index_ranges.append([index_start, index_end])
        # Add the amount of invalid data
        global total_length, invalid_length
        invalid_length += (index_end - index_start + 1)

    return index_ranges

def ibi_timestamp_ranges_to_offset_index_ranges(
    timestamp_ranges,
    stream_start,
    offset_data):
    """
    Convert timestamp ranges to index ranges for data with explicit time offsets.

    Parameters:
        timestamp_ranges: list of (start_ts, end_ts) tuples
        stream_start: datetime of the start of the offset-based data stream
        offset_data: list of [offset_seconds, value] entries

    Returns:
        List of [start_index, end_index] index ranges into offset_data
    """
    index_ranges = []

    for ts_start, ts_end in timestamp_ranges:
        # Convert timestamps to relative offset (seconds since start)
        start_offset = (ts_start - stream_start).total_seconds()
        end_offset = (ts_end - stream_start).total_seconds()

        # Find index range in offset_data that falls within these offsets
        indices_in_range = [
            idx for idx, (offset_sec, _) in enumerate(offset_data)
            if start_offset <= offset_sec <= end_offset
        ]

        if indices_in_range:
            index_ranges.append([indices_in_range[0], indices_in_range[-1]])

    return index_ranges

def calculate_invalid_indices(sessions):
    global invalid_length
    start = None
    end = None

    x_id = None
    x_data = []

    y_id = None
    y_data = []

    z_id = None
    z_data = []

    hr_id = None
    hr_start = None
    hr_len = None

    bvp_id = None
    bvp_start = None
    bvp_len = None

    eda_id = None
    eda_start = None
    eda_len = None

    temp_id = None
    temp_start = None
    temp_len = None

    ibi_id = None
    ibi_start = None
    ibi_len = None
    ibi_data = None

    for session in sessions:
        if session[1].split("_")[-1] == "X":
            x_id = session[0]
            x_data = conn.get_data_from_measure_session(x_id)
            start = session[2]
            end = session[2] + timedelta(seconds=session[3] / 32)
            if (end - start).total_seconds() < 600:
                # If the session is shorter than 10 mins, mark all as invalid
                for sess in sessions:
                    conn.update_invalid_data_indices(sess[0], [[0,-1]])
                    invalid_length += sess[3]
                print(f"Marked all data as invalid for sessions {sessions} because it is shorter than 10 minutes.")
                return

        elif session[1].split("_")[-1] == "Y":
            y_id = session[0]
            y_data = conn.get_data_from_measure_session(y_id)
        elif session[1].split("_")[-1] == "Z":
            z_id = session[0]
            z_data = conn.get_data_from_measure_session(z_id)

        elif session[1].split("_")[-1] == "HR":
            hr_id = session[0]
            hr_start = session[2]
            hr_len = session[3]
        elif session[1].split("_")[-1] == "BVP":
            bvp_id = session[0]
            bvp_start = session[2]
            bvp_len = session[3]
        elif session[1].split("_")[-1] == "EDA":
            eda_id = session[0]
            eda_start = session[2]
            eda_len = session[3]
        elif session[1].split("_")[-1] == "TEMP":
            temp_id = session[0]
            temp_start = session[2]
            temp_len = session[3]
        elif session[1].split("_")[-1] == "IBI":
            ibi_id = session[0]
            ibi_start = session[2]
            ibi_data = conn.get_data_from_measure_session(ibi_id)


    # Calculate vector of magnitude
    magnitude = [(x ** 2 + y ** 2 + z ** 2) ** 0.5 for x, y, z in zip(x_data, y_data, z_data)]
    # Create a time series for the magnitude data
    times = pd.date_range(start=start, end=end, periods=len(magnitude))
    # Create a DataFrame for plotting
    df = pd.DataFrame({'Magnitude': magnitude}, index=times)

    # --- Identify periods of low variance and mark them as invalid ---
    window_size = 32  # number of samples
    std_threshold = 1  # std deviation threshold for flatline
    min_flat_length = 19200  # minimum length of flat segment

    # Compute rolling standard deviation with strict window
    rolling_std = df['Magnitude'].rolling(window=window_size, center=True, min_periods=window_size).std()
    flat_mask = rolling_std < std_threshold

    # Label valid and invalid (flat) periods
    df['Valid'] = True
    df.loc[flat_mask, 'Magnitude'] = np.nan  # only mark Magnitude as NaN

    # Group contiguous NaNs to find true flatline regions
    is_flat = df['Magnitude'].isna().astype(int)
    group = (is_flat.diff() != 0).cumsum()
    flat_groups = df[is_flat == 1].groupby(group)

    # Mark only sufficiently long flatline regions as invalid
    for _, grp in flat_groups:
        if len(grp) >= min_flat_length:
            df.loc[grp.index, 'Valid'] = False

    # Restore original Magnitude values
    df['Magnitude'] = df['Magnitude'].interpolate()

    flatline_ranges = []
    invalid_mask = ~df['Valid']
    invalid_indices = np.where(invalid_mask)[0]

    if len(invalid_indices) > 0:
        # Group contiguous indices
        from itertools import groupby
        from operator import itemgetter

        grouped = [list(map(itemgetter(1), g)) for k, g in groupby(enumerate(invalid_indices), lambda x: x[0] - x[1])]
        for group in grouped:
            if len(group) >= min_flat_length:
                flatline_ranges.append([df.index[group[0]], df.index[group[-1]]])


        print("Flatline index ranges:", flatline_ranges)

        conn.update_invalid_data_indices(x_id, timestamp_ranges_to_index_ranges(flatline_ranges, start, 32, len(x_data)))
        conn.update_invalid_data_indices(y_id, timestamp_ranges_to_index_ranges(flatline_ranges, start, 32, len(y_data)))
        conn.update_invalid_data_indices(z_id, timestamp_ranges_to_index_ranges(flatline_ranges, start, 32, len(z_data)))
        conn.update_invalid_data_indices(hr_id, timestamp_ranges_to_index_ranges(flatline_ranges, hr_start, 1, hr_len))
        conn.update_invalid_data_indices(bvp_id, timestamp_ranges_to_index_ranges(flatline_ranges, bvp_start, 64, bvp_len))
        conn.update_invalid_data_indices(eda_id, timestamp_ranges_to_index_ranges(flatline_ranges, eda_start, 4, eda_len))
        conn.update_invalid_data_indices(temp_id, timestamp_ranges_to_index_ranges(flatline_ranges, temp_start, 4, temp_len))
        conn.update_invalid_data_indices(ibi_id, ibi_timestamp_ranges_to_offset_index_ranges(flatline_ranges, ibi_start, ibi_data))


for patient in conn.get_all_patient_ids():
    print(f"Processing patient ID: {patient}")
    for group in conn.get_all_measurement_groups_from_patient_id(patient):
        print(f"Processing group: {group}")
        sessions = conn.get_all_measurement_sessions_from_group_id(group[0])
        for session in sessions:
            total_length += session[3]  # Add the length of the session to the total length
            # Print percentage so far
        if len(sessions) == 8:
            calculate_invalid_indices(sessions)

            print(f"Invalid data percentage: {invalid_length / total_length * 100:.2f}%")
        elif len(sessions) == 15:
            for index, session in enumerate(sessions):
                if index < 7:
                    # Mark all data as invalid for the first 7 sessions
                    conn.update_invalid_data_indices(session[0], [[0, -1]])
                    invalid_length += session[3]
                else:
                    # Process the remaining sessions normally
                    calculate_invalid_indices(sessions[7:])
                    break
        else:
            # Mark all data as invalid if the number of sessions is not 8 or 15
            for session in sessions:
                conn.update_invalid_data_indices(session[0], [[0, -1]])
                invalid_length += session[3]  # Add the length of the session to the invalid length

print(f"Total length: {total_length}")
print(f"Invalid length: {invalid_length}")
print(f"Invalid data percentage: {invalid_length / total_length * 100:.2f}%")



