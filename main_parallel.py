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


def process_patient(patient_dir):
    patient_path = os.path.join(DATA_FOLDER, patient_dir)
    if not os.path.isdir(patient_path):
        return

    # Create a new database connection for this thread
    conn = connect.Connection()

    for week_folder in os.listdir(patient_path):
        week_path = os.path.join(patient_path, week_folder)
        if not os.path.isdir(week_path):
            continue

        # Determine the week enumeration based on the folder name
        week_enum = 'Week_1' if '1' in week_folder else 'Week_2'

        for filename in os.listdir(week_path):
            if not filename.endswith(".zip"):
                continue
            zip_path = os.path.join(week_path, filename)
            print(f"Processing {patient_dir} - {week_enum}/{filename}...")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for contained_file in zip_ref.namelist():
                    if not contained_file.endswith(".csv"):
                        continue
                    measurement_type = contained_file.split(".")[0]
                    if measurement_type not in MEASUREMENT_TYPES:
                        print(f"Skipping unsupported measurement type: {measurement_type}")
                        continue
                    with zip_ref.open(contained_file) as f:
                        try:
                            df = pd.read_csv(f, header=None)
                        except Exception as e:
                            print(f"Error reading CSV from {contained_file} in {zip_path}: {e}")
                            continue

                        start_timestamp = datetime.fromtimestamp(df.iloc[0, 0])
                        sample_rate = df.iloc[1, 0]

                        if measurement_type == "ACC":
                            for index, axis in enumerate(["ACC_X", "ACC_Y", "ACC_Z"]):
                                measurement_id = f"{patient_dir}_{week_enum}_{axis}"
                                data = df.iloc[2:, index].values.tolist()
                                if conn.check_if_measurement_exists(measurement_id, patient_dir, week_enum, axis):
                                    print(f"Measurement {measurement_id} already exists.")
                                    conn.insert_measure_session(measurement_id, start_timestamp, data)
                                else:
                                    conn.insert_measurement(measurement_id, patient_dir, week_enum, axis, sample_rate)
                                    conn.insert_measure_session(measurement_id, start_timestamp, data)
                        else:
                            measurement_id = f"{patient_dir}_{week_enum}_{measurement_type}"
                            data = df.iloc[2:, :].values.tolist()
                            if conn.check_if_measurement_exists(measurement_id, patient_dir, week_enum,
                                                                measurement_type):
                                print(f"Measurement {measurement_id} already exists.")
                                conn.insert_measure_session(measurement_id, start_timestamp, data)
                            else:
                                conn.insert_measurement(measurement_id, patient_dir, week_enum, measurement_type,
                                                        sample_rate)
                                conn.insert_measure_session(measurement_id, start_timestamp, data)

    conn.close()


def main():
    # Create a connection to the database
    conn = connect.Connection()

    # Put all the patients in the database
    print("Loading patients...")
    participants_df = pd.read_csv(f"{DATA_FOLDER}/Participants_study_18122024.csv", delimiter=';')
    # We assume the CSV has columns like: ID, Age, Sex, Group, UserKey, etc.
    for _, row in participants_df.iterrows():
        patient_id = row["ID"]
        user_key = row["UserKey"]  # May be used to match with relax data
        group_num = str(row["Group"]).strip()
        patient_group = GROUP_MAPPING.get(group_num, 'Exercise')
        origin = ORIGIN_MAPPING[patient_id[0]]
        conn.insert_patient(patient_id, origin, patient_group)
    print("Patients loaded.\n")

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

        conn.insert_relax_session(patient_id, start_dt, end_dt)
    print("Relaxation sessions loaded.\n")

    print("Loading measure sessions...")
    patient_dirs = [d for d in os.listdir(DATA_FOLDER) if os.path.isdir(os.path.join(DATA_FOLDER, d))]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(process_patient, patient_dirs)


if __name__ == "__main__":
    main()
