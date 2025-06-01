import os

from dotenv import load_dotenv, find_dotenv
from typing import Literal
from datetime import datetime, timedelta
import psycopg2

TABLES = Literal["measure_session", "measurement", "patient", "relax_session"]
MEASUREMENT_TYPES = Literal["ACC", "BVP", "EDA", "HR", "IBI", "TEMP"]
WEEK = Literal["Week_1", "Week_2"]
GROUP = Literal["Exercise", "VR"]
ORIGIN = Literal["UMCG", "Forte GGZ", "Lentis", "Argo GGZ", "Mediant GGZ", "Huisartsenpraktijk"]
SEX = Literal["Male", "Female"]

class Connection:
    def __init__(self):
        load_dotenv(find_dotenv())
        self.conn = psycopg2.connect(
            host=os.getenv("HOST"),
            database=os.getenv("DATABASE"),
            user=os.getenv("USER"),
            password=os.getenv("PASSWORD"),
            port=os.getenv("PORT"),
        )
        self.cursor = self.conn.cursor()

    def fetch_all_from_table(self, table: TABLES):
        self.cursor.execute(f"SELECT * FROM {table}")
        rows = self.cursor.fetchall()
        return rows

    def insert_patient(self, patient_id: str, origin: ORIGIN, patient_group: GROUP):
        self.cursor.execute(
            "INSERT INTO patient (id, origin, patient_group) VALUES (%s, %s, %s)",
            (patient_id, origin, patient_group),
        )
        self.conn.commit()

    def insert_measurement(self,measurement_id: str, patient_id: str, week: WEEK , measurement_type: MEASUREMENT_TYPES, sample_rate: float):
        self.cursor.execute(
            "INSERT INTO measurement (id, patient_id, week, measurement_type, sample_rate) VALUES (%s, %s, %s, %s, %s)",
            (measurement_id, patient_id, week, measurement_type, float(sample_rate)),
        )
        self.conn.commit()

    def insert_measure_session(self, measurement_id: str, start_timestamp: datetime, data: list):
        self.cursor.execute(
            "INSERT INTO measure_session (measurement_id, start_timestamp, data) VALUES (%s, %s, %s)",
            (measurement_id, start_timestamp, data),
        )
        self.conn.commit()

    def insert_relax_session(self, patient_id: str, start_timestamp: datetime, end_timestamp: datetime, start_question_1: int = None, end_question_1: int = None, start_question_2: int = None, end_question_2: int = None, modifier: str = None):
        self.cursor.execute(
            "INSERT INTO relax_session (patient_id, start_timestamp, end_timestamp, start_question_1, end_question_1, start_question_2, end_question_2, modifier) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (patient_id, start_timestamp, end_timestamp, start_question_1, end_question_1, start_question_2, end_question_2, modifier),
        )
        self.conn.commit()

    def check_if_measurement_exists(self, measurement_id: str, patient_id: str, week: WEEK, measurement_type: MEASUREMENT_TYPES):
        self.cursor.execute(
            "SELECT COUNT(*) FROM measurement WHERE id = %s AND patient_id = %s AND week = %s AND measurement_type = %s",
            (measurement_id, patient_id, week, measurement_type),
        )
        return self.cursor.fetchone()[0]

    def add_age_sex_to_patient(self, patient_id: str, age: int, sex: SEX):
        self.cursor.execute(
            "UPDATE patient SET age = %s, sex = %s WHERE id = %s",
            (age, sex, patient_id),
        )
        self.conn.commit()

    def add_research_group_to_patient(self, patient_id: str, group_1: bool, group_2: bool, group_3: bool):
        self.cursor.execute(
            "UPDATE patient SET group_1 = %s, group_2 = %s, group_3 = %s WHERE id = %s",
            (group_1, group_2, group_3, patient_id),
        )
        self.conn.commit()

    def drop_all_rows(self, table: TABLES):
        self.cursor.execute(f"DELETE FROM {table}")
        self.conn.commit()

    def get_beginning_and_end_timestamp_from_measure_session(self, session_id: int):
        # Get the measurement ID from the session ID
        self.cursor.execute(
            "SELECT measurement_id FROM measure_session WHERE id = %s",
            (session_id,),
        )
        measurement_id = self.cursor.fetchone()[0]

        # Get the sample rate, skip this if the measurement type is IBI
        if measurement_id.split("_")[-1] == "IBI":
            # Select all the data from the measure session
            self.cursor.execute(
                "SELECT data FROM measure_session WHERE id = %s",
                (session_id,),
            )
            data = self.cursor.fetchone()[0]

            if isinstance(data[0], list):
                data = [sublist[1] for sublist in data]

            # Add all the data points together to get the total amount of milliseconds, convert to seconds and round to a whole number
            total_seconds = round(sum(data))

            # Get the start timestamp
            self.cursor.execute(
                "SELECT start_timestamp FROM measure_session WHERE id = %s",
                (session_id,),
            )
            start_timestamp = self.cursor.fetchone()[0]

            # Calculate the end timestamp by adding the total seconds to the start timestamp
            end_timestamp = start_timestamp + timedelta(seconds=total_seconds)


        else:
            # Get the start timestamp and the count of the data points
            self.cursor.execute(
                "SELECT ms.start_timestamp, COUNT(u.data_element) "
                "FROM measure_session ms, LATERAL unnest(ms.data) "
                "AS u(data_element) "
                "WHERE ms.id = %s "
                "GROUP BY ms.start_timestamp;",
                (session_id,),
            )
            start_timestamp, count = self.cursor.fetchone()
            if start_timestamp is None or count is None:
                print(f"Warning: No data found for session ID {session_id}.")
                return

            # Get the sample rate
            self.cursor.execute(
                "SELECT sample_rate FROM measurement WHERE id = %s",
                (measurement_id,),
            )
            sample_rate = self.cursor.fetchone()[0]

            # Determine the end timestamp by adding the count of data points divided by the sample rate to the start timestamp
            end_timestamp = start_timestamp + timedelta(seconds=round(count / sample_rate))

        return start_timestamp, end_timestamp

    def get_all_measurement_session_ids_from_measurement_id(self, measurement_id: str):
        self.cursor.execute(
            "SELECT id FROM measure_session WHERE measurement_id = %s ORDER BY start_timestamp",
            (measurement_id,),
        )
        return [row[0] for row in self.cursor.fetchall()]

    def get_all_measurement_ids_from_patient_id(self, patient_id: str):
        self.cursor.execute(
            "SELECT id FROM measurement WHERE patient_id = %s",
            (patient_id,),
        )
        return [row[0] for row in self.cursor.fetchall()]

    def get_all_timestamps_from_patient_id(self, patient_id: str):
        timestamp_dict = {}
        for measurement_id in self.get_all_measurement_ids_from_patient_id(patient_id):
            for session_id in self.get_all_measurement_session_ids_from_measurement_id(measurement_id):
                start_timestamp, end_timestamp = self.get_beginning_and_end_timestamp_from_measure_session(session_id)
                timestamp_dict[f"{measurement_id}_{session_id}"] = (start_timestamp, end_timestamp)
        return timestamp_dict

    def get_all_relax_sessions_from_patient_id(self, patient_id: str):
        # Get the patient group from the patient ID
        self.cursor.execute(
            "SELECT patient_group FROM patient WHERE id = %s",
            (patient_id,),
        )
        patient_group = self.cursor.fetchone()[0]

        relax_sessions_dict = {}
        self.cursor.execute(
            "SELECT id, start_timestamp, end_timestamp FROM relax_session WHERE patient_id = %s",
            (patient_id,),
        )
        for relax_id, start_timestamp, end_timestamp in self.cursor.fetchall():
            relax_sessions_dict[f"{patient_id}_{patient_group}_{relax_id}"] = (start_timestamp, end_timestamp)
        return relax_sessions_dict

    def get_all_sessions_from_patient_id(self, patient_id: str):
        vitals = self.get_all_timestamps_from_patient_id(patient_id)
        relax_sessions = self.get_all_relax_sessions_from_patient_id(patient_id)
        return vitals, relax_sessions

    def get_all_patient_ids(self):
        self.cursor.execute(
            "SELECT id FROM patient ORDER BY id DESC",
        )
        return [row[0] for row in self.cursor.fetchall()]

    def close(self):
        self.cursor.close()
        self.conn.close()

