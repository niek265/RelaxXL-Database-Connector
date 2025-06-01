import os
import zipfile
from datetime import datetime
import pandas as pd
import concurrent.futures
from RXLDBC import connect

DATA_FOLDER = "C:/Users/niek2/Documents/Data/Data_Empatica_E4"
GROUP_MAPPING = {
    '2': "Exercise",
    '1': "VR"
}
ORIGIN_MAPPING = {
    'U': 'UMCG',
    'F': 'Forte GGZ',
    'L': 'Lentis',
    'A': 'Argo GGZ',
    'M': 'Mediant GGZ',
    'H': 'Huisartsenpraktijk'
}

MEASUREMENT_TYPES = ["ACC", "BVP", "EDA", "HR", "IBI", "TEMP"]


def main():
    # Create a connection to the database
    conn = connect.Connection()

    # Put all the patients in the database
    print("Loading patients...")
    participants_df = pd.read_csv(f"{DATA_FOLDER}/Participants_study_18122024.csv", delimiter=';')

    participants_lookup = participants_df.set_index("UserKey")["ID"].to_dict()

    print("Loading relaxation sessions...")
    relax_df = pd.read_csv(f"{DATA_FOLDER}/researchdata_VRelax_15112024.csv", delimiter=';')

    # Define potential datetime formats encountered in the CSV.
    datetime_formats = ["%d-%m-%Y %H:%M", "%m/%d/%Y %H:%M:%S"]

    def parse_datetime(dt_str):
        for fmt in datetime_formats:
            try:
                return datetime.strptime(dt_str, fmt)
            except Exception:
                continue
        return None

    for _, row in relax_df.iterrows():
        user_key = row["UserKey"]
        patient_id = participants_lookup.get(user_key)
        if not patient_id:
            print(f"Warning: No patient found for UserKey {user_key}. Skipping row.")
            continue

        start_dt = parse_datetime(row["StartSessionDT"])
        end_dt = parse_datetime(row["EndSessionDT"])
        if not start_dt or not end_dt:
            print(f"Warning: Failed to parse datetime for row with UserKey {user_key}.")
            continue

        # Check if the end date is after the start date
        if start_dt and end_dt and start_dt > end_dt:
            print(
                f"Warning: Start date {start_dt} is after end date {end_dt} for UserKey {user_key}. Skipping row.")
            continue

        # Check is the session is longer than 2 hours
        if start_dt and end_dt and (end_dt - start_dt).total_seconds() > 7200:
            print(f"Warning: Session duration exceeds 2 hours for UserKey {user_key}. Skipping row.")
            continue

        # Check if the start date is before 2022-07-04
        if start_dt and start_dt < datetime(2022, 7, 4):
            print(f"Warning: Start date {start_dt} is before 2022-07-04 for UserKey {user_key}. Skipping row.")
            continue

        if not start_dt or not end_dt:
            print(f"Warning: Failed to parse datetime for row with UserKey {user_key}.")
            continue

        # Parse the questions
        start_question_1 = row["StartQuestion1"]
        end_question_1 = row["EndQuestion1"]
        start_question_2 = row["StartQuestion2"]
        end_question_2 = row["EndQuestion2"]
        modifier = row["IsSleepSession"]

        conn.insert_relax_session(patient_id, start_dt, end_dt, start_question_2, end_question_2, start_question_1, end_question_1, modifier)
    print("Relaxation sessions loaded.\n")

    # Define potential datetime formats encountered in the CSV.
    datetime_formats = ["%Y-%m-%d %H:%M:%S"]

    def parse_datetime(dt_str):
        for fmt in datetime_formats:
            try:
                return datetime.strptime(dt_str, fmt)
            except Exception:
                continue
        return None


    relax_ex_df = pd.read_csv(f"{DATA_FOLDER}/Relaxation_excercises_30102024.csv", delimiter=',')
    for _, row in relax_ex_df.iterrows():
        user_key = f"{row["oo_id"]}".strip().upper().replace(" ", "")
        patient_id = participants_lookup.get(user_key)
        if not patient_id:
            print(f"Warning: No patient found for UserKey {user_key}. Skipping row.")
            continue

        start_dt = parse_datetime(row["oo_ss"])
        end_dt = parse_datetime(row["oo_es"])

        # Check if the end date is after the start date
        if start_dt and end_dt and start_dt > end_dt:
            print(f"Warning: Start date {start_dt} is after end date {end_dt} for UserKey {user_key}. Skipping row.")
            continue

        # Check is the session is longer than 2 hours
        if start_dt and end_dt and (end_dt - start_dt).total_seconds() > 7200:
            print(f"Warning: Session duration exceeds 2 hours for UserKey {user_key}. Skipping row.")
            continue

        # Check if the start date is before 2022-07-04
        if start_dt and start_dt < datetime(2022, 7, 4):
            print(f"Warning: Start date {start_dt} is before 2022-07-04 for UserKey {user_key}. Skipping row.")
            continue

        if not start_dt or not end_dt:
            print(f"Warning: Failed to parse datetime for row with UserKey {user_key}.")
            continue

        # Parse the questions
        start_question_1 = row["oo_sq1"]
        end_question_1 = row["oo_eq1"]
        start_question_2 = row["oo_sq2"]
        end_question_2 = row["oo_eq2"]
        modifier = row["ontspanningsoefening_complete"]


        conn.insert_relax_session(patient_id, start_dt, end_dt, start_question_1, end_question_1, start_question_2, end_question_2, modifier)
    print("Relaxation sessions loaded.\n")
if __name__ == "__main__":
    main()
