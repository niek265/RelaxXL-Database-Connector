from numpy.ma.core import alltrue

from RXLDBC import connect
import pandas as pd

def main():
    conn = connect.Connection()

    print("\033[91m" + "Exercise" + "\033[0m")

    # Get the min, max and average age from all patients in the Exercise group
    cursor = conn.conn.cursor()
    cursor.execute(
        "SELECT MIN(age), MAX(age), AVG(age) FROM patient WHERE patient_group = 'Exercise'"
    )

    min_age, max_age, avg_age = cursor.fetchone()
    print(f"Exercise: Min age: {min_age}, Max age: {max_age}, Avg age: {avg_age}")



    # Count the number of patients in each group
    cursor.execute(
        "SELECT COUNT(*) FROM patient WHERE patient_group = 'Exercise'"
    )
    exercise_count = cursor.fetchone()[0]
    cursor.execute(
        "SELECT COUNT(*) FROM patient WHERE patient_group = 'Exercise' AND sex = 'Female'"
    )
    exercise_count_female = cursor.fetchone()[0]

    exercise_count_male = exercise_count - exercise_count_female

    print(f"Exercise participants count total: {exercise_count}, Female: {exercise_count_female}, Male: {exercise_count_male}")


    # Get all patient IDs from the Exercise group
    cursor.execute(
        "SELECT id FROM patient WHERE patient_group = 'Exercise'"
    )
    vr_patients = cursor.fetchall()
    vr_patients = [patient[0] for patient in vr_patients]

    sessions_exercise = []
    # Get all relax sessions from the Exercise group
    for patient_id in vr_patients:
        sessions_exercise.append(conn.get_all_relax_sessions_from_patient_id(patient_id))

    # Calculate the average duration of the relaxation sessions
    vr_total_duration = 0
    vr_total_sessions = 0
    for sessions in sessions_exercise:
        for session in sessions.values():
            start_timestamp, end_timestamp = session
            duration = (end_timestamp - start_timestamp).total_seconds()
            # If the duration is larger than half an hour, we consider it invalid
            if 0 < duration < 3500:
                vr_total_duration += duration
                vr_total_duration += duration
                vr_total_sessions += 1
            else:
                continue


    # Seconds to minutes
    vr_total_duration = vr_total_duration / 60
    print(f"Exercise total duration: {vr_total_duration} minutes")
    print(f"Exercise total sessions: {vr_total_sessions}")

    vr_average_duration = vr_total_duration / vr_total_sessions
    print(f"Exercise average duration: {vr_average_duration} minutes")



    print("\033[91m" + "VR" + "\033[0m")

    # Get the min, max and average age from all patients in the VR group
    cursor.execute(
        "SELECT MIN(age), MAX(age), AVG(age) FROM patient WHERE patient_group = 'VR'"
    )
    min_age, max_age, avg_age = cursor.fetchone()
    print(f"VR: Min age: {min_age}, Max age: {max_age}, Avg age: {avg_age}")

    cursor.execute(
        "SELECT COUNT(*) FROM patient WHERE patient_group = 'VR'"
    )
    exercise_count = cursor.fetchone()[0]
    cursor.execute(
        "SELECT COUNT(*) FROM patient WHERE patient_group = 'VR' AND sex = 'Female'"
    )
    exercise_count_female = cursor.fetchone()[0]

    exercise_count_male = exercise_count - exercise_count_female

    print(f"VR participants count total: {exercise_count}, Female: {exercise_count_female}, Male: {exercise_count_male}")

    # Get all patient IDs from the VR group
    cursor.execute(
        "SELECT id FROM patient WHERE patient_group = 'VR'"
    )
    vr_patients = cursor.fetchall()
    vr_patients = [patient[0] for patient in vr_patients]

    sessions_exercise = []
    # Get all relax sessions from the VR group
    for patient_id in vr_patients:
        sessions_exercise.append(conn.get_all_relax_sessions_from_patient_id(patient_id))

    # Calculate the average duration of the relaxation sessions
    vr_total_duration = 0
    vr_total_sessions = 0
    for sessions in sessions_exercise:
        for session in sessions.values():
            start_timestamp, end_timestamp = session
            duration = (end_timestamp - start_timestamp).total_seconds()
            # If the duration is larger than half an hour, we consider it invalid
            if 0 < duration < 3500:
                vr_total_duration += duration
                vr_total_duration += duration
                vr_total_sessions += 1
            else:
                continue

    # Seconds to minutes
    vr_total_duration = vr_total_duration / 60
    print(f"VR total duration: {vr_total_duration} minutes")
    print(f"VR total sessions: {vr_total_sessions}")

    vr_average_duration = vr_total_duration / vr_total_sessions
    print(f"VR average duration: {vr_average_duration} minutes")



    print("\033[91m" + "Group 1" + "\033[0m")
    # Get the amount of patients, Exercise and VR, in the Group 1 group
    cursor.execute(
        "SELECT COUNT(*) FROM patient WHERE group_1 = True"
    )
    patient_count_group1 = cursor.fetchone()[0]

    cursor.execute(
        "SELECT COUNT(*) FROM patient WHERE group_1 = True AND patient_group = 'Exercise'"
    )
    exercise_count_group1 = cursor.fetchone()[0]

    cursor.execute(
        "SELECT COUNT(*) FROM patient WHERE group_1 = True AND patient_group = 'VR'"
    )
    vr_count_group1 = cursor.fetchone()[0]

    print(f"Group 1 participants count total: {patient_count_group1}, Exercise: {exercise_count_group1}, VR: {vr_count_group1}")

    # Count the number of patients in each group
    cursor.execute(
        "SELECT COUNT(*) FROM patient WHERE group_1 = True"
    )
    group1_count = cursor.fetchone()[0]
    cursor.execute(
        "SELECT COUNT(*) FROM patient WHERE group_1 = True AND sex = 'Female'"
    )
    group1_count_female = cursor.fetchone()[0]

    group1_count_male = group1_count - group1_count_female

    print(f"Group 1 sex: Female: {group1_count_female}, Male: {group1_count_male}")


    # Get the min, max and average age from all patients in the Group 1 group
    cursor.execute(
        "SELECT MIN(age), MAX(age), AVG(age) FROM patient WHERE group_1 = True"
    )

    min_age, max_age, avg_age = cursor.fetchone()
    print(f"Group 1: Min age: {min_age}, Max age: {max_age}, Avg age: {avg_age}")
    # Get all patient IDs from the Group 1 group
    cursor.execute(
        "SELECT id FROM patient WHERE group_1 = True"
    )
    group1_patients = cursor.fetchall()
    group1_patients = [patient[0] for patient in group1_patients]
    sessions_group1 = []
    # Get all relax sessions from the Group 1 group
    for patient_id in group1_patients:
        sessions_group1.append(conn.get_all_relax_sessions_from_patient_id(patient_id))
    # Calculate the average duration of the relaxation sessions

    group1_total_duration = 0
    group1_total_sessions = 0
    for sessions in sessions_group1:
        for session in sessions.values():
            start_timestamp, end_timestamp = session
            duration = (end_timestamp - start_timestamp).total_seconds()
            # If the duration is larger than half an hour, we consider it invalid
            if 0 < duration < 3500:
                group1_total_duration += duration
                group1_total_duration += duration
                group1_total_sessions += 1
            else:
                continue
    group1_total_duration = group1_total_duration / 60
    print(f"Group 1 total duration: {group1_total_duration} minutes")
    print(f"Group 1 total sessions: {group1_total_sessions}")
    group1_average_duration = group1_total_duration / group1_total_sessions
    print(f"Group 1 average duration: {group1_average_duration} minutes")


    print("\033[91m" + "Group 2" + "\033[0m")
    # Get the amount of patients, Exercise and VR, in the Group 2 group
    cursor.execute(
        "SELECT COUNT(*) FROM patient WHERE group_2 = True"
    )
    patient_count_group2 = cursor.fetchone()[0]
    cursor.execute(
        "SELECT COUNT(*) FROM patient WHERE group_2 = True AND patient_group = 'Exercise'"
    )
    exercise_count_group2 = cursor.fetchone()[0]
    cursor.execute(
        "SELECT COUNT(*) FROM patient WHERE group_2 = True AND patient_group = 'VR'"
    )
    vr_count_group2 = cursor.fetchone()[0]
    print(f"Group 2 participants count total: {patient_count_group2}, Exercise: {exercise_count_group2}, VR: {vr_count_group2}")

    # Count the number of patients in each group
    cursor.execute(
        "SELECT COUNT(*) FROM patient WHERE group_2 = True"
    )
    group2_count = cursor.fetchone()[0]
    cursor.execute(
        "SELECT COUNT(*) FROM patient WHERE group_2 = True AND sex = 'Female'"
    )
    group2_count_female = cursor.fetchone()[0]

    group2_count_male = group2_count - group2_count_female

    print(f"Group 2 sex: Female: {group2_count_female}, Male: {group2_count_male}")

    # Get the min, max and average age from all patients in the Group 2 group
    cursor.execute(
        "SELECT MIN(age), MAX(age), AVG(age) FROM patient WHERE group_2 = True"
    )
    min_age, max_age, avg_age = cursor.fetchone()
    print(f"Group 2: Min age: {min_age}, Max age: {max_age}, Avg age: {avg_age}")
    # Get all patient IDs from the Group 2 group
    cursor.execute(
        "SELECT id FROM patient WHERE group_2 = True"
    )
    group2_patients = cursor.fetchall()
    group2_patients = [patient[0] for patient in group2_patients]
    sessions_group2 = []
    # Get all relax sessions from the Group 2 group
    for patient_id in group2_patients:
        sessions_group2.append(conn.get_all_relax_sessions_from_patient_id(patient_id))
    # Calculate the average duration of the relaxation sessions
    group2_total_duration = 0
    group2_total_sessions = 0
    for sessions in sessions_group2:
        for session in sessions.values():
            start_timestamp, end_timestamp = session
            duration = (end_timestamp - start_timestamp).total_seconds()
            # If the duration is larger than half an hour, we consider it invalid
            if 0 < duration < 3500:
                group2_total_duration += duration
                group2_total_duration += duration
                group2_total_sessions += 1
            else:
                continue
    group2_total_duration = group2_total_duration / 60
    print(f"Group 2 total duration: {group2_total_duration} minutes")
    print(f"Group 2 total sessions: {group2_total_sessions}")
    group2_average_duration = group2_total_duration / group2_total_sessions
    print(f"Group 2 average duration: {group2_average_duration} minutes")


    print("\033[91m" + "Group 3" + "\033[0m")
    # Get the amount of patients, Exercise and VR, in the Group 3 group
    cursor.execute(
        "SELECT COUNT(*) FROM patient WHERE group_3 = True"
    )
    patient_count_group3 = cursor.fetchone()[0]
    cursor.execute(
        "SELECT COUNT(*) FROM patient WHERE group_3 = True AND patient_group = 'Exercise'"
    )
    exercise_count_group3 = cursor.fetchone()[0]
    cursor.execute(
        "SELECT COUNT(*) FROM patient WHERE group_3 = True AND patient_group = 'VR'"
    )
    vr_count_group3 = cursor.fetchone()[0]
    print(f"Group 3 participants count total: {patient_count_group3}, Exercise: {exercise_count_group3}, VR: {vr_count_group3}")

    # Count the number of patients in each group
    cursor.execute(
        "SELECT COUNT(*) FROM patient WHERE group_3 = True"
    )
    group3_count = cursor.fetchone()[0]
    cursor.execute(
        "SELECT COUNT(*) FROM patient WHERE group_3 = True AND sex = 'Female'"
    )
    group3_count_female = cursor.fetchone()[0]

    group3_count_male = group3_count - group3_count_female

    print(f"Group 3 sex: Female: {group3_count_female}, Male: {group3_count_male}")


    # Get the min, max and average age from all patients in the Group 3 group
    cursor.execute(
        "SELECT MIN(age), MAX(age), AVG(age) FROM patient WHERE group_3 = True"
    )
    min_age, max_age, avg_age = cursor.fetchone()
    print(f"Group 3: Min age: {min_age}, Max age: {max_age}, Avg age: {avg_age}")
    # Get all patient IDs from the Group 3 group
    cursor.execute(
        "SELECT id FROM patient WHERE group_3 = True"
    )
    all_patients = cursor.fetchall()
    all_patients = [patient[0] for patient in all_patients]
    sessions_group3 = []
    # Get all relax sessions from the Group 3 group
    for patient_id in all_patients:
        sessions_group3.append(conn.get_all_relax_sessions_from_patient_id(patient_id))
    # Calculate the average duration of the relaxation sessions
    all_total_duration = 0
    all_total_sessions = 0
    for sessions in sessions_group3:
        for session in sessions.values():
            start_timestamp, end_timestamp = session
            duration = (end_timestamp - start_timestamp).total_seconds()
            # If the duration is larger than half an hour, we consider it invalid
            if 0 < duration < 3500:
                all_total_duration += duration
                all_total_duration += duration
                all_total_sessions += 1
            else:
                continue
    all_total_duration = all_total_duration / 60
    print(f"Group 3 total duration: {all_total_duration} minutes")
    print(f"Group 3 total sessions: {all_total_sessions}")
    group3_average_duration = all_total_duration / all_total_sessions
    print(f"Group 3 average duration: {group3_average_duration} minutes")


    print("\033[91m" + "All participants" + "\033[0m")
    # Get the min, max and average age from all patients in the database

    cursor.execute(
        "SELECT MIN(age), MAX(age), AVG(age) FROM patient"
    )
    min_age, max_age, avg_age = cursor.fetchone()
    print(f"All participants: Min age: {min_age}, Max age: {max_age}, Avg age: {avg_age}")
    # Count the number of patients in each group
    cursor.execute(
        "SELECT COUNT(*) FROM patient"
    )
    all_count = cursor.fetchone()[0]

    print(f"Total amount of participants: {all_count}")

    cursor.execute(
        "SELECT COUNT(*) FROM patient WHERE sex = 'Female'"
    )
    all_count_female = cursor.fetchone()[0]

    all_count_male = all_count - all_count_female

    print(f"All participants sex: Female: {all_count_female}, Male: {all_count_male}")

    # Get all patient IDs from the Group 3 group
    cursor.execute(
        "SELECT id FROM patient"
    )
    all_patients = cursor.fetchall()
    all_patients = [patient[0] for patient in all_patients]
    sessions_group3 = []
    # Get all relax sessions from the Group 3 group
    for patient_id in all_patients:
        sessions_group3.append(conn.get_all_relax_sessions_from_patient_id(patient_id))
    # Calculate the average duration of the relaxation sessions
    all_total_duration = 0
    all_total_sessions = 0
    for sessions in sessions_group3:
        for session in sessions.values():
            start_timestamp, end_timestamp = session
            duration = (end_timestamp - start_timestamp).total_seconds()
            # If the duration is larger than half an hour, we consider it invalid
            if 0 < duration < 3500:
                all_total_duration += duration
                all_total_duration += duration
                all_total_sessions += 1
            else:
                continue
    all_total_duration = all_total_duration / 60
    print(f"All participants total duration: {all_total_duration} minutes")
    print(f"All participants total sessions: {all_total_sessions}")
    group3_average_duration = all_total_duration / all_total_sessions
    print(f"All participants average duration: {group3_average_duration} minutes")

    conn.close()

if __name__ == "__main__":
    main()