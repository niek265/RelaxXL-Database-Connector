import pandas as pd
from RXLDBC import connect

SEX_MAPPING = {
    '2' : "Male",
    '1' : "Female"
}

def main():
    conn = connect.Connection()

    # Load the participants data
    participants_df = pd.read_csv("C:/Users/niek2/Documents/Data/Data_Empatica_E4/Participants_study_18122024.csv", delimiter=';')

    for _, row in participants_df.iterrows():
        age = row["Age"]
        sex = SEX_MAPPING.get(row["Sex"])
        patient_id = row["ID"]

        print(f"{sex}")
        # convert to literal

        if sex is None:
            continue

        conn.add_age_sex_to_patient(patient_id, age, sex)

    conn.close()


if __name__ == "__main__":
    main()