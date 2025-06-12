import numpy as np
import pandas as pd

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
        return week1_timestamp_dict, week2_timestamp_dict
    else:
        print(f"Patient {patient} does not have enough data for both weeks.")
        return None, None

conn = connect.Connection()
cursor = conn.conn.cursor()

dataframes = []

cursor.execute("SELECT id FROM patient ORDER BY id")
patient_ids = cursor.fetchall()

for patient_id in patient_ids:
    week1, week2 = filter_week_data_by_patient(patient_id[0])
    if week1 and week2:
        # Get the earliest timestamp for Week 1
        week1_start = min(start for start, _ in week1.values())
        # Get the latest timestamp for Week 1
        week1_end = max(end for _, end in week1.values())
        # Get the earliest timestamp for Week 2
        week2_start = min(start for start, _ in week2.values())
        # Get the latest timestamp for Week 2
        week2_end = max(end for _, end in week2.values())

        # Get the amount of relaxation sessions for this patient
        cursor.execute("SELECT COUNT(*) FROM relax_session WHERE patient_id = %s", (patient_id,))
        relax_count = cursor.fetchone()[0]

        # Get the amount of relaxation sessions for this patient in Week 1
        cursor.execute("SELECT COUNT(*) FROM relax_session WHERE patient_id = %s AND start_timestamp >= %s AND end_timestamp <= %s",
                       (patient_id, week1_start, week1_end))
        week1_relax_count = cursor.fetchone()[0]

        # Get the amount of relaxation sessions for this patient in Week 2
        cursor.execute("SELECT COUNT(*) FROM relax_session WHERE patient_id = %s AND start_timestamp >= %s AND end_timestamp <= %s",
                       (patient_id, week2_start, week2_end))
        week2_relax_count = cursor.fetchone()[0]

        # Get the average duration of relaxation sessions for this patient in seconds
        cursor.execute("SELECT start_timestamp, end_timestamp FROM relax_session WHERE patient_id = %s",
                       (patient_id,))
        relax_sessions = cursor.fetchall()
        relax_durations = [(end - start).total_seconds() for start, end in relax_sessions]
        avg_relax_duration = sum(relax_durations) / len(relax_durations) if relax_durations else 0

        # Get the average duration of relaxation sessions for this patient in Week 1 in seconds

        cursor.execute("SELECT start_timestamp, end_timestamp FROM relax_session WHERE patient_id = %s AND start_timestamp >= %s AND end_timestamp <= %s",
                       (patient_id, week1_start, week1_end))
        week1_relax_sessions = cursor.fetchall()
        week1_relax_durations = [(end - start).total_seconds() for start, end in week1_relax_sessions]
        avg_week1_relax_duration = sum(week1_relax_durations) / len(week1_relax_durations) if week1_relax_durations else 0

        # Get the average duration of relaxation sessions for this patient in Week 2 in seconds
        cursor.execute("SELECT start_timestamp, end_timestamp FROM relax_session WHERE patient_id = %s AND start_timestamp >= %s AND end_timestamp <= %s",
                       (patient_id, week2_start, week2_end))
        week2_relax_sessions = cursor.fetchall()
        week2_relax_durations = [(end - start).total_seconds() for start, end in week2_relax_sessions]
        avg_week2_relax_duration = sum(week2_relax_durations) / len(week2_relax_durations) if week2_relax_durations else 0

        # Get the total duration of the E4 sessions for this patient in seconds
        e4_sessions = list(week1.values()) + list(week2.values())
        e4_durations = [(end - start).total_seconds() for start, end in e4_sessions]

        # Get the week 1 and week 2 E4 durations
        week1_e4_durations = [(end - start).total_seconds() for start, end in week1.values()]
        week2_e4_durations = [(end - start).total_seconds() for start, end in week2.values()]


        stats = {
            "patient_id": patient_id,
            "relax_count": relax_count,
            "week1_relax_count": week1_relax_count,
            "week2_relax_count": week2_relax_count,
            "avg_relax_duration": avg_relax_duration,
            "avg_week1_relax_duration": avg_week1_relax_duration,
            "avg_week2_relax_duration": avg_week2_relax_duration,
            "total_relax_duration": sum(relax_durations),
            "total_week1_relax_duration": sum(week1_relax_durations),
            "total_week2_relax_duration": sum(week2_relax_durations),
            "e4_total_duration": sum(e4_durations),
            "week1_e4_duration": sum(week1_e4_durations),
            "week2_e4_duration": sum(week2_e4_durations)
        }
        print(stats)
        df = pd.DataFrame([stats])
        dataframes.append(df)
    else:
        print(f"Skipping patient {patient_id[0]} due to insufficient data.")
if dataframes:
    result_df = pd.concat(dataframes, ignore_index=True)
    result_df.to_csv("Week_relax_count.csv", header=True)
    print("Data saved to Week_relax_count.csv")
else:
    print("No valid data found for any patients.")



