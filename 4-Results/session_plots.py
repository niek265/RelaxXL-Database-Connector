from RXLDBC import connect

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

conn = connect.Connection()

def plot_E4_session_data(id, session_id, data, start_timestamp, end_timestamp, number):
    """
    Plots the E4 session data with start and end timestamps.

    Args:
        data (list): The E4 session data to plot.
        start_timestamp (str): The start timestamp of the session.
        end_timestamp (str): The end timestamp of the session.
    """
    # Convert timestamps to pandas datetime
    start = pd.to_datetime(start_timestamp)
    end = pd.to_datetime(end_timestamp)

    patient = id.split("_")[0]  # Extract patient ID from the measurement ID
    week = id.split("_")[2]  # Extract week from the measurement ID

    # Create a time series for the IBI data
    times = pd.date_range(start=start, end=end, periods=len(data))
    data = [d[0] for d in data]  # Assuming data is a list of lists

    # Create a DataFrame for plotting
    df = pd.DataFrame({'Time': times, 'HR': data})

    plt.figure(figsize=(10, 5))
    plt.plot(df['Time'], df['HR'], linestyle='-', label='HR Data')
    plt.title(f'{patient} BVP Session Data from week {week}, {session_id}')

    plt.xlabel('Time')
    plt.ylabel('BVP')
    plt.grid()

    # Save the plot to a fiile in the "plots" directory
    plt.savefig(f"plots/{patient}_BVP_{week}_{session_id}_{number}.png")

    plt.close()

def plot_all_E4_sessions_for_patient(patient_id: str):
    """
    Plots all E4 sessions for a given patient.

    Args:
        patient_id (str): The ID of the patient whose E4 sessions are to be plotted.
    """
    conn = connect.Connection()
    cursor = conn.conn.cursor()

    # Get all measurement sessions for the patient
    for id in conn.get_all_measurement_ids_from_patient_id(patient_id):
        print(f"Processing measurement ID: {id}")
        if id.split("_")[-1] == "BVP":
            sessions = conn.get_all_measurement_session_ids_from_measurement_id(id)

            for index, session_id in enumerate(sessions):
                if index >= 0:
                    start_timestamp, end_timestamp = conn.get_beginning_and_end_timestamp_from_measure_session(session_id)

                    # Fetch the IBI data for the session
                    cursor.execute(
                        "SELECT data FROM measure_session WHERE id = %s",
                        (session_id,),
                    )
                    data = cursor.fetchone()[0]

                    # plot the data with start and end timestamps
                    plot_E4_session_data(id, session_id, data, start_timestamp, end_timestamp, index)

# for patient in conn.get_all_patient_ids():
#     print(f"Plotting E4 sessions for patient: {patient}")
#     # Call the function to plot all E4 sessions for the patient
#     plot_all_E4_sessions_for_patient(patient)

data = conn.get_all_measurement_sessions_from_patient_id_with_index("H001")
# Print all items and their start and end timestamps
for key, (data2) in data.items():
    for item in data2:
        if item["measurement_type"] == "BVP":
            start_timestamp, end_timestamp = conn.get_beginning_and_end_timestamp_from_measure_session(item["id"])
            print(f"Session ID: {item['id']}, Start: {start_timestamp}, End: {end_timestamp}")
            plot_E4_session_data(key, item["id"], item["data"], start_timestamp, end_timestamp, 0)