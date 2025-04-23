import zipfile

from RXLDBC import connect
from datetime import datetime
import pandas as pd
import os

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

    for patient_dir in os.listdir(DATA_FOLDER):
        print(f"Processing patient {patient_dir}...")
        patient_path = os.path.join(DATA_FOLDER, patient_dir)
        if not os.path.isdir(patient_path):
            continue

        # Each patient folder should contain week folders (e.g., "Week1" or "Week2").
        for week_folder in os.listdir(patient_path):
            week_path = os.path.join(patient_path, week_folder)
            if not os.path.isdir(week_path):
                continue

            # Determine the week_enum value; here we assume folder names like "Week1" map to 'Week 1'
            week_enum = 'Week_1' if '1' in week_folder else 'Week_2'

            # Process each ZIP file in the week folder.
            for filename in os.listdir(week_path):
                print(f"Processing week {week_enum}/{filename}...")
                if not filename.endswith(".zip"):
                    continue
                zip_path = os.path.join(week_path, filename)
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    # Assume each ZIP contains one CSV file with measurement data.
                    for contained_file in zip_ref.namelist():
                        if not contained_file.endswith(".csv"):
                            continue
                        # Strip the filename to get the measurement type
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

                            # Start timestamp is the first entry in the first column of the dataframe
                            start_timestamp = df.iloc[0, 0]
                            # Convert the unix timestamp to a datetime object
                            start_timestamp = datetime.fromtimestamp(start_timestamp)

                            # Sample rate is the second entry in the first column of the dataframe
                            sample_rate = df.iloc[1, 0]
                            if measurement_type == "ACC":
                                # For ACC, the first column contains the X coordinate, the second the Y coordinate and the third the Z coordinate
                                for index, axis in enumerate(["ACC_X", "ACC_Y", "ACC_Z"]):
                                    measurement_id = f"{patient_dir}_{week_enum}_{axis}"

                                    # The data is in the rest of the dataframe
                                    data = df.iloc[2:, index].values.tolist()

                                    # Check if the measurement already exists
                                    if conn.check_if_measurement_exists(measurement_id, patient_dir, week_enum, axis):
                                        print(f"Measurement {measurement_id} already exists.")
                                        conn.insert_measure_session(measurement_id, start_timestamp, data)
                                    else:
                                        conn.insert_measurement(measurement_id, patient_dir, week_enum, axis, sample_rate)
                                        conn.insert_measure_session(measurement_id, start_timestamp, data)
                            else:

                                measurement_id = f"{patient_dir}_{week_enum}_{measurement_type}"

                                # The data is in the rest of the dataframe
                                data = df.iloc[2:, :].values.tolist()

                                # Check if the measurement already exists
                                if conn.check_if_measurement_exists(measurement_id, patient_dir, week_enum, measurement_type):
                                    print(f"Measurement {measurement_id} already exists.")
                                    conn.insert_measure_session(measurement_id, start_timestamp, data)
                                else:
                                    conn.insert_measurement(measurement_id, patient_dir, week_enum, measurement_type, sample_rate)
                                    conn.insert_measure_session(measurement_id, start_timestamp, data)

    # Close the connection
    conn.close()

if __name__ == "__main__":
    main()


