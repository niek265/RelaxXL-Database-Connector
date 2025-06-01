from datetime import timedelta

from RXLDBC import connect
import matplotlib.pyplot as plt
from multiprocessing import Process

def main():
    # test = {'F003_Week_2_ACC_X_2884': 'F003_VR_19220', 'F003_Week_2_ACC_Y_2890': 'F003_VR_19220', 'F003_Week_2_ACC_Z_2893': 'F003_VR_19220', 'F003_Week_2_EDA_2899': 'F003_VR_19220', 'F003_Week_2_BVP_2906': 'F003_VR_19220', 'F003_Week_2_TEMP_2911': 'F003_VR_19220', 'F003_Week_2_IBI_2916': 'F003_VR_19220', 'F003_Week_2_HR_2918': 'F003_VR_19220'}
    # plot_filtered_relax_sessions(test)
    conn = connect.Connection()

    cursor = conn.conn.cursor()
    cursor.execute("SELECT id FROM patient ORDER BY id")
    patient_ids = cursor.fetchall()

    # Keep track od the total sessions after filtering in both cases
    total_filtered_e4_sessions = 0
    total_filtered_relax_sessions = 0

    processes = []

    for patient_id in patient_ids:
        patient_id = patient_id[0]
        e4_timestamps = conn.get_all_timestamps_from_patient_id(patient_id)
        relax_timestamps = conn.get_all_relax_sessions_from_patient_id(patient_id)

        # # Filter E4 sessions that are 10 hours or longer
        # filtered_e4_sessions = filter_10hrs_of_e4_data_on_single_day(e4_timestamps)
        # # Print the number of filtered sessions
        # print(f"Filtered E4 sessions for {patient_id}: {len(filtered_e4_sessions)}")
        # # Keep track of the total sessions after filtering
        # total_filtered_e4_sessions += len(filtered_e4_sessions)

        # Filter E4 sessions that are 5 minutes before and after relaxation sessions
        filtered_relax_sessions = filter_5min_of_e4_before_and_after_relax_sessions(e4_timestamps, relax_timestamps)
        # Print the number of filtered sessions
        print(f"Filtered Relax sessions for {patient_id}: {len(filtered_relax_sessions)}")
        # Keep track of the total sessions after filtering
        total_filtered_relax_sessions += len(filtered_relax_sessions)
        # Plot the filtered relaxation sessions
        process = Process(target=plot_filtered_relax_sessions, args=(filtered_relax_sessions,))
        process.start()
        processes.append(process)


    # Wait for all threads to finish
    for process in processes:
        process.join()

    # Print the total number of filtered sessions for all patients
    # print(f"Total filtered E4 sessions: {total_filtered_e4_sessions}")
    print(f"Total filtered Relax sessions: {total_filtered_relax_sessions}")
    conn.close()

def filter_10hrs_of_e4_data_on_single_day(e4_timestamps):
    # Return the sessions that are 10 hours or longer
    filtered_sessions = {}
    for session_id, (start, end) in e4_timestamps.items():
        duration = (end - start).total_seconds() / 3600  # Convert to hours
        if duration >= 10:
            filtered_sessions[session_id] = (start, end)
    return filtered_sessions

def filter_5min_of_e4_before_and_after_relax_sessions(e4_timestamps, relax_timestamps):
    # Filter E4 sessions that are 5 minutes before and after relaxation sessions
    filtered_sessions = {}
    for session_id, (start, end) in e4_timestamps.items():
        for relax_id, (relax_start, relax_end) in relax_timestamps.items():
            if start <= relax_start + timedelta(minutes=5) and end >= relax_end + timedelta(minutes=5):
                filtered_sessions[session_id] = relax_id
    return filtered_sessions

def plot_filtered_relax_sessions(filtered_sessions):
    # Plot the filtered relaxation sessions

    print(filtered_sessions)
    conn = connect.Connection()
    cursor = conn.conn.cursor()


    for session_id, relax_id in filtered_sessions.items():
        plt.figure(figsize=(10, 6))
        # Get the values of the filtered sessions from the database
        cursor.execute("SELECT start_timestamp, end_timestamp FROM relax_session WHERE id = %s", (relax_id.split("_")[-1],))
        start_relax, end_relax = cursor.fetchone()

        cursor.execute("SELECT measurement_id, start_timestamp, data FROM measure_session WHERE id = %s", (session_id.split("_")[-1],))
        measurement_id, start_data, data = cursor.fetchone()

        # Get the sample rate
        cursor.execute("SELECT sample_rate FROM measurement WHERE id = %s", (measurement_id,))
        sample_rate = cursor.fetchone()[0]

        # Calculate the end timestamp of the data
        end_data = start_data + timedelta(seconds=len(data)/sample_rate)

        # Trim the data to match the relax sessions time frame plus minus 5 minutes
        start_plot_x = start_relax - timedelta(minutes=5)
        end_plot_x = end_relax + timedelta(minutes=5)

        # Calculate the timestamps for the x-axis
        timestamps = [start_data + timedelta(seconds=i/sample_rate) for i in range(len(data))]

        # Plot the data, use the timestamps as x-axis
        patient = session_id.split("_")[0]
        week = session_id.split("_")[2]
        measurement_type = session_id.split("_")[3] if len(session_id.split("_")) < 6 else f"ACC_{session_id.split("_")[4]}"
        relax_type = relax_id.split("_")[1]
        relax_id_id = relax_id.split("_")[-1]

        plt.plot(timestamps, data, label=f"{measurement_id}")
        plt.axvline(x=start_relax, color='r', linestyle='--', label="Relax Start")
        plt.axvline(x=end_relax, color='g', linestyle='--', label="Relax End")
        plt.xlim(start_plot_x, end_plot_x)
        plt.xlabel("Time")
        plt.ylabel(measurement_type)
        plt.title(f"{measurement_id} in {relax_id}")
        # Make a subtitle with the patient, week and measurement type
        plt.suptitle(f"Patient: {patient}, Group {relax_type}, Week: {week}, Measurement Type: {measurement_type}")
        plt.legend()

        # Save the plot to a folder
        plt.savefig(f"plots/{session_id}_{relax_type}_{relax_id_id}.png")
        plt.show()
        plt.close()
        # Save the filtered sessions to a JSON file
        with open("plots/filtered_sessions.csv", "a") as f:
            f.write(f"{session_id}, {relax_id}\n")




if __name__ == "__main__":
    main()
