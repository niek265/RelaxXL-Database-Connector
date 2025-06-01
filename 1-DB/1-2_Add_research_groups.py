import pandas as pd
from RXLDBC import connect

def main():
    conn = connect.Connection()

    # Load the participants data
    participants_df = pd.read_csv("C:/Users/niek2/Documents/Afstudeerstage/Literatuur/Overzicht data_LR.csv", delimiter=';')

    for _, row in participants_df.iterrows():
        patient_id = row["Participant"]
        gr1 = row["GR1"]
        gr2 = row["GR2"]
        gr3 = row["GR3"]

        # Convert none to False and 1 to True
        gr1 = True if gr1 == 1 else False
        gr2 = True if gr2 == 1 else False
        gr3 = True if gr3 == 1 else False


        conn.add_research_group_to_patient(patient_id, gr1, gr2, gr3)

    conn.close()


if __name__ == "__main__":
    main()