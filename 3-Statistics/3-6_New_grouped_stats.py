from RXLDBC import connect

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

conn = connect.Connection()

def calculate_invalid_indices(dataframe: pd.DataFrame,
                              acc_length: int,
                              acc_start: pd.Timestamp,
                              acc_end: pd.Timestamp,
                              hr_length: int,
                              hr_start: pd.Timestamp,
                              hr_end: pd.Timestamp,
                              bvp_length: int,
                              bvp_start: pd.Timestamp,
                              bvp_end: pd.Timestamp,
                              eda_length: int,
                              eda_start: pd.Timestamp,
                              eda_end: pd.Timestamp,
                              temp_length: int,
                              temp_start: pd.Timestamp,
                              temp_end: pd.Timestamp,
                              ibi_length: int,
                              ibi_start: pd.Timestamp,
                              ibi_end: pd.Timestamp):
    # Calculate the invalid indices for all different measurements based on the "valid" column in the DataFrame
    invalid_indices = []
    if not dataframe['Valid'].all():
        # If the DataFrame has invalid data, find the indices of the invalid data
        invalid_indices = dataframe.index[~dataframe['Valid']].tolist()
    else:
        # If the DataFrame is valid, check the lengths of the measurements
        if (acc_length < 600 or hr_length < 600 or bvp_length < 600 or
            eda_length < 600 or temp_length < 600 or ibi_length < 600):
            invalid_indices = list(range(len(dataframe)))


for patient_id in conn.get_all_patient_ids():
    print(f"Processing patient ID: {patient_id}")
    for key, (session_groups) in conn.get_all_measurement_sessions_from_patient_id_with_index(patient_id).items():
        X_data = []
        Y_data = []
        Z_data = []
        HR_data_len = 0
        BVP_data_len = 0
        EDA_data_len = 0
        TEMP_data_len = 0
        IBI_data_len = 0
        start = None
        end = None

        unix = (session_groups[0]['start_timestamp'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')

        group_id = f"{patient_id}_{session_groups[0]['week']}_{unix}"
        length = session_groups[0]['end_timestamp'] - session_groups[0]['start_timestamp']
        print(f"{group_id} has {len(session_groups)} sessions.")

        for session in session_groups:
            if session["measurement_type"] == "ACC_X":
                X_id = session["session_id"]
                X_data = session["data"]
            elif session["measurement_type"] == "ACC_Y":
                Y_id = session["session_id"]
                Y_data = session["data"]
            elif session["measurement_type"] == "ACC_Z":
                Z_id = session["session_id"]
                Z_data = session["data"]
                start = pd.to_datetime(session["start_timestamp"])
                end = pd.to_datetime(session["end_timestamp"])
                week = session["week"]
            elif session["measurement_type"] == "HR":
                HR_id = session["session_id"]
                HR_data_len = len(session["data"])
                HR_start = pd.to_datetime(session["start_timestamp"])
                HR_end = pd.to_datetime(session["end_timestamp"])
            elif session["measurement_type"] == "BVP":
                BVP_id = session["session_id"]
                BVP_data_len = len(session["data"])
                BVP_start = pd.to_datetime(session["start_timestamp"])
                BVP_end = pd.to_datetime(session["end_timestamp"])
            elif session["measurement_type"] == "EDA":
                EDA_id = session["session_id"]
                EDA_data_len = len(session["data"])
                EDA_start = pd.to_datetime(session["start_timestamp"])
                EDA_end = pd.to_datetime(session["end_timestamp"])
            elif session["measurement_type"] == "TEMP":
                TEMP_id = session["session_id"]
                TEMP_data_len = len(session["data"])
                TEMP_start = pd.to_datetime(session["start_timestamp"])
                TEMP_end = pd.to_datetime(session["end_timestamp"])
            elif session["measurement_type"] == "IBI":
                IBI_id = session["session_id"]
                IBI_data_len = len(session["data"])
                IBI_start = pd.to_datetime(session["start_timestamp"])
                IBI_end = pd.to_datetime(session["end_timestamp"])



        if X_data and Y_data and Z_data and HR_data_len and BVP_data_len and EDA_data_len and TEMP_data_len and IBI_data_len:
            # If the session is shorter than 10 minutes, mark all data as invalid
            if (end - start).total_seconds() < 600:
                df['Valid'] = False
            # Calculate vector of magnitude
            magnitude = [(x**2 + y**2 + z**2)**0.5 for x, y, z in zip(X_data, Y_data, Z_data)]
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

            # Mark the invalid periods in the database for all measurements




        else:
            print(f"Skipping patient {patient_id} session {key} due to insufficient data.")
            continue


