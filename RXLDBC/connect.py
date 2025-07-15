import os

from dotenv import load_dotenv, find_dotenv
from typing import Literal
from datetime import datetime, timedelta
import psycopg2
from pandas.core.indexers import validate_indices

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
                data = [sublist[0] for sublist in data]

            # Add all the data points together to get the total amount of milliseconds, convert to seconds and round to a whole number
            total_seconds = round(data[-1])

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

    def get_all_ibi_from_patient_id(self, patient_id: str):
        # Get all IBI measurements for the patient
        self.cursor.execute(
            "SELECT id FROM measurement WHERE patient_id = %s AND measurement_type = 'IBI'",
            (patient_id,),
        )
        ibi_measurements = self.cursor.fetchall()

        ibi_data = {}
        for measurement_id in ibi_measurements:
            for session_id in self.get_all_measurement_session_ids_from_measurement_id(measurement_id[0]):
                self.cursor.execute(
                    "SELECT start_timestamp, data FROM measure_session WHERE id = %s",
                    (session_id,),
                )
                # Fetch the start timestamp and data for the session
                start_timestamp, data = self.cursor.fetchone()
                end_timestamp = start_timestamp + timedelta(seconds=data[-1][0])
                ibi_data[f"{measurement_id[0]}_{session_id}"] = (start_timestamp, end_timestamp, data)

        return ibi_data

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
            "SELECT id FROM patient ORDER BY id",
        )
        return [row[0] for row in self.cursor.fetchall()]

    def get_all_measurement_sessions_from_patient_id_with_index(self, patient_id: str):
        measurement_types = ["ACC_X", "ACC_Y", "ACC_Z", "BVP", "EDA", "HR", "IBI", "TEMP"]
        session_list = []
        for measurement_type in measurement_types:
            self.cursor.execute(
                "SELECT id, measurement_id, data "
                "FROM measure_session "
                "WHERE measurement_id "
                "IN (SELECT id FROM measurement WHERE patient_id = %s AND measurement_type = %s) "
                "ORDER BY id;",
                (patient_id,measurement_type,),
            )
            sessions = self.cursor.fetchall()
            for session in sessions:
                session_id, measurement_id, data = session
                start_timestamp, end_timestamp = self.get_beginning_and_end_timestamp_from_measure_session(session_id)
                session_list.append({
                    "patient_id": patient_id,
                    "week": measurement_id.split("_")[2],  # Assuming week is part of the measurement ID
                    "session_id": session_id,
                    "measurement_id": measurement_id,
                    "measurement_type": measurement_type,
                    "start_timestamp": start_timestamp,
                    "end_timestamp": end_timestamp,
                    "data": data
                })
        # Group the sessions by start timestamp, they must have the same date, hour and minute
        grouped_sessions = {}
        for session in session_list:
            start_time = session["start_timestamp"].strftime("%Y-%m-%d %H:%M")
            if start_time not in grouped_sessions:
                # Check if the next minute is already in the dictionary
                next_minute = (session["start_timestamp"] + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M")
                # Check if the previous minute is already in the dictionary
                previous_minute = (session["start_timestamp"] - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M")
                if next_minute in grouped_sessions:
                    # If the next minute is already in the dictionary, append the session to that minute
                    grouped_sessions[next_minute].append(session)
                    continue
                elif previous_minute in grouped_sessions:
                    # If the previous minute is already in the dictionary, append the session to that minute
                    grouped_sessions[previous_minute].append(session)
                    continue
                grouped_sessions[start_time] = []
            grouped_sessions[start_time].append(session)
        return grouped_sessions

    def set_invalid_data_indices(self, measurement_session_id: str, invalid_indices: list):
        """
        Sets the invalid data indices for a measurement session.

        Args:
            measurement_session_id (str): The ID of the measurement session.
            invalid_indices (list): A list of indices that are considered invalid.
        """
        # The SQL field `invalid_data_indices` should be integer[][]
        self.cursor.execute(
            "UPDATE measure_session SET invalid_data_indices = %s WHERE id = %s",
            (invalid_indices, measurement_session_id),
        )
        self.conn.commit()

    def mark_session_as_group(self, measurement_session_id: str, group: str, patient_id: str, week: int, length: int):
        """
        Marks a measurement session as belonging to a specific group.

        Args:
            measurement_session_id (str): The ID of the measurement session.
            group (GROUP): The group to which the session belongs.
        """

        if week == '1':
            week = "Week_1"
        elif week == '2':
            week = "Week_2"


        # If the group does not exist in measure_group, it will be created
        self.cursor.execute(
            "INSERT INTO measure_group (id, patient_id, week, length  ) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
            (group, patient_id, week, length),
        )

        self.cursor.execute(
            "UPDATE measure_session SET measure_group_id = %s WHERE id = %s",
            (group, measurement_session_id),
        )
        self.conn.commit()

    def get_all_measurement_groups_from_patient_id(self, patient_id: str):
        """
        Retrieves all measurement groups for a given patient ID.

        Args:
            patient_id (str): The ID of the patient.

        Returns:
            list: A list of tuples containing measurement group IDs and their associated data.
        """
        self.cursor.execute(
            "SELECT id, week, length FROM measure_group WHERE patient_id = %s",
            (patient_id,),
        )
        return self.cursor.fetchall()

    def get_all_measurement_sessions_from_group_id(self, group_id: str):
        """
        Retrieves all measurement sessions associated with a specific group ID.

        Args:
            group_id (str): The ID of the measurement group.

        Returns:
            list: A list of tuples containing measurement session IDs and their associated data.
        """
        # Get the id, measurement_id, start_timestamp, count of data and the invalid data indices for each session in the group
        self.cursor.execute(
            "SELECT ms.id, ms.measurement_id, ms.start_timestamp, COUNT(u.data_element), ms.invalid_data_indices "
            "FROM measure_session ms, LATERAL unnest(ms.data) AS u(data_element) "
            "WHERE ms.measure_group_id = %s "
            "GROUP BY ms.id, ms.measurement_id, ms.start_timestamp, ms.invalid_data_indices "
            "ORDER BY ms.start_timestamp;",
            (group_id,),
        )
        return self.cursor.fetchall()

    def update_invalid_data_indices(self, measurement_session_id: str, invalid_indices: list):
        """
        Updates the invalid data indices for a measurement session.

        Args:
            measurement_session_id (str): The ID of the measurement session.
            invalid_indices (list): A list of lists containing 2 integer indices that are considered invalid.
        """
        self.cursor.execute(
            "UPDATE measure_session SET invalid_data_indices = %s WHERE id = %s",
            (invalid_indices, measurement_session_id),
        )
        self.conn.commit()

    def get_data_from_measure_session(self, session_id: str):
        """
        Retrieves the data from a specific measurement session.

        Args:
            session_id (str): The ID of the measurement session.

        Returns:
            list: The data associated with the measurement session.
        """
        self.cursor.execute(
            "SELECT data FROM measure_session WHERE id = %s",
            (session_id,),
        )
        return self.cursor.fetchone()[0]

    def get_invalid_data_indices_from_measure_session(self, session_id: str):
        """
        Retrieves the invalid data indices from a specific measurement session.

        Args:
            session_id (str): The ID of the measurement session.

        Returns:
            list: A list of lists containing 2 integer indices that are considered invalid.
        """
        self.cursor.execute(
            "SELECT invalid_data_indices FROM measure_session WHERE id = %s",
            (session_id,),
        )
        result = self.cursor.fetchone()
        if result and result[0] is not None:
            return result[0]
        return []

    def get_valid_data_from_measure_session(self, session_id: str):
        """
        Retrieves the valid data from a specific measurement session, excluding invalid indices.

        Args:
            session_id (str): The ID of the measurement session.

        Returns:
            dictionary: A dictionary containing the start timestamp as the key and a list of valid data points as the value.
        """

        def calculate_timestamp_segment(start, offset, measurement_type, data):
            """
            Calculate the start timestamp for a segment based on the segment length.
            """
            if measurement_type == 'X' or measurement_type == 'Y' or measurement_type == 'Z':
                return start + timedelta(seconds=offset/32)
            elif measurement_type == 'BVP':
                return start + timedelta(seconds=offset/64)
            elif measurement_type == 'HR':
                return start + timedelta(seconds=offset)
            elif measurement_type == 'EDA':
                return start + timedelta(seconds=offset/4)
            elif measurement_type == 'TEMP':
                return start + timedelta(seconds=offset/4)
            elif measurement_type == 'IBI':
                return start + timedelta(seconds=data[offset][0])

        self.cursor.execute(
            "SELECT measurement_id, start_timestamp, data, invalid_data_indices FROM measure_session WHERE id = %s",
            (session_id,),
        )
        result = self.cursor.fetchone()
        if not result:
            return {}

        measurement_id, start_timestamp, data, invalid_indices = result

        # Use the invalid indices to split the data into valid segments
        if invalid_indices:
            valid_data = {}
            measurement_type = measurement_id.split("_")[-1]
            if invalid_indices[0][0] == 0 and invalid_indices[0][1] == -1:
                # If the invalid indices cover the entire data, return an empty dictionary
                return {}
            for index, indices in enumerate(invalid_indices):
                start_index, end_index = indices

                if index == 0:
                    # If this is the first invalid segment, add data before the first invalid index
                    valid_data[start_timestamp] = data[:start_index]

                elif index == len(invalid_indices) - 1:
                    # If this is the last invalid segment, add the remaining data
                    valid_data[calculate_timestamp_segment(start_timestamp, invalid_indices[index-1][1], measurement_type, data)] = data[invalid_indices[index-1][1]:]
                else:
                    # For all other segments, add the data between the previous invalid index and the current one
                    valid_data[calculate_timestamp_segment(start_timestamp, invalid_indices[index-1][1], measurement_type, data)] = data[invalid_indices[index-1][1]:start_index]
        else:
            valid_data = {start_timestamp: data}

        return valid_data

    def get_start_and_end_timestamps_from_measure_session_valid_data(self, session_id: str):
        """
        Retrieves the start and end timestamps from a specific measurement session's valid data.

        Args:
            session_id (str): The ID of the measurement session.

        Returns:
            tuple: A tuple containing the start timestamp and end timestamp of the valid data.
        """
        def calculate_valid_indices_from_invalid_indices(invalid_indices, data_length):
            """
            Calculate the valid indices from the invalid indices.
            """
            valid_indices = []
            if not invalid_indices:
                return [(0, data_length - 1)]

            # If the first invalid index is 0 and the last is -1, return an empty list
            if invalid_indices[0] == [0, -1]:
                return []

            # Add the valid indices before the first invalid index
            if invalid_indices[0][0] > 0:
                valid_indices.append((0, invalid_indices[0][0] - 1))

            # Add the valid indices between the invalid indices
            for i in range(len(invalid_indices) - 1):
                valid_indices.append((invalid_indices[i][1] + 1, invalid_indices[i + 1][0] - 1))

            # Add the valid indices after the last invalid index
            if invalid_indices[-1][1] < data_length - 1:
                valid_indices.append((invalid_indices[-1][1] + 1, data_length - 1))

            return valid_indices


        def calculate_timestamp_for_index(session_start_timestamp, index, measurement_type, data = None):
            """
            Calculate the timestamp for a segment based on the index and measurement type.
            """
            if measurement_type in ['X', 'Y', 'Z']:
                return session_start_timestamp + timedelta(seconds=index / 32)
            elif measurement_type == 'BVP':
                return session_start_timestamp + timedelta(seconds=index / 64)
            elif measurement_type == 'HR':
                return session_start_timestamp + timedelta(seconds=index)
            elif measurement_type == 'EDA':
                return session_start_timestamp + timedelta(seconds=index / 4)
            elif measurement_type == 'TEMP':
                return session_start_timestamp + timedelta(seconds=index / 4)
            elif measurement_type == 'IBI':
                return session_start_timestamp + timedelta(seconds=data[index][0])



        invalid_indices = self.get_invalid_data_indices_from_measure_session(session_id)
        timestamps = {session_id: []}

        if not invalid_indices:
            # Get the start timestamp and the count of the data points
            self.cursor.execute(
                "SELECT ms.start_timestamp, COUNT(u.data_element), ms.measurement_id "
                "FROM measure_session ms, LATERAL unnest(ms.data) "
                "AS u(data_element) "
                "WHERE ms.id = %s "
                "GROUP BY ms.start_timestamp, ms.measurement_id;",
                (session_id,),
            )
            start_timestamp, count, measurement_id = self.cursor.fetchone()
            if start_timestamp is None or count is None:
                print(f"Warning: No data found for session ID {session_id}.")
                return timestamps
            else:
                # Calculate the end timestamp by adding the count of data points divided by the sample rate to the start timestamp
                measurement_type = measurement_id.split("_")[-1]
                if measurement_type == 'IBI':
                    # If the measurement type is IBI, we need to fetch the data to calculate the end timestamp
                    self.cursor.execute(
                        "SELECT data FROM measure_session WHERE id = %s",
                        (session_id,),
                    )
                    data = self.cursor.fetchone()[0]
                    end_timestamp = start_timestamp + timedelta(seconds=data[-1][0])
                    return {session_id: [(start_timestamp, end_timestamp)]}
                else:
                    end_timestamp = calculate_timestamp_for_index(start_timestamp, count, measurement_type)
                    return {session_id: [(start_timestamp, end_timestamp)]}
        elif invalid_indices[0] == [0,-1]:
            return timestamps
        else:
            # Get the start timestamp and the count of the data points
            self.cursor.execute(
                "SELECT ms.start_timestamp, COUNT(u.data_element), ms.measurement_id "
                "FROM measure_session ms, LATERAL unnest(ms.data) "
                "AS u(data_element) "
                "WHERE ms.id = %s "
                "GROUP BY ms.start_timestamp, ms.measurement_id;",
                (session_id,),
            )
            start_timestamp, count, measurement_id = self.cursor.fetchone()
            measurement_type = measurement_id.split("_")[-1]

            if measurement_type == 'IBI':
                valid_indices = calculate_valid_indices_from_invalid_indices(invalid_indices, round(count/2))
                # If the measurement type is IBI, we need to fetch the data to calculate the end timestamp
                self.cursor.execute(
                    "SELECT data FROM measure_session WHERE id = %s",
                    (session_id,),
                )
                data = self.cursor.fetchone()[0]
                for start_index, end_index in valid_indices:
                    start_time = calculate_timestamp_for_index(start_timestamp, start_index, measurement_type, data)
                    end_time = calculate_timestamp_for_index(start_timestamp, end_index, measurement_type, data)
                    timestamps[session_id].append((start_time, end_time))
            else:
                valid_indices = calculate_valid_indices_from_invalid_indices(invalid_indices, count)
                for start_index, end_index in valid_indices:
                    start_time = calculate_timestamp_for_index(start_timestamp, start_index, measurement_type)
                    end_time = calculate_timestamp_for_index(start_timestamp, end_index, measurement_type)
                    timestamps[session_id].append((start_time, end_time))
            return timestamps

    def get_total_e4_time_from_patient_id(self, patient_id: str):
        """
        Retrieves the total E4 time for a specific patient ID.

        Args:
            patient_id (str): The ID of the patient.

        Returns:
            int: The total E4 time in seconds.
        """
        self.get_all_measurement_ids_from_patient_id(patient_id)

        for measurement_id in self.get_all_measurement_ids_from_patient_id(patient_id):
            # Get all measurement sessions for the measurement ID
            self.cursor.execute(
                "SELECT id FROM measure_session WHERE measurement_id = %s",
                (measurement_id,),
            )
            session_ids = self.cursor.fetchall()
            total_time = 0
            for session_id in session_ids:
                start_timestamp, end_timestamp = self.get_beginning_and_end_timestamp_from_measure_session(session_id[0])
                if start_timestamp and end_timestamp:
                    total_time += (end_timestamp - start_timestamp).total_seconds()
            return int(total_time)


    def get_data_from_measure_session_with_index(self, measure_id: str, start: int, stop: int):
        """
        Retrieves the data from a specific measurement session, including the start timestamp and index.
        """
        self.cursor.execute("SELECT data[%s:%s] FROM measure_session WHERE id = %s",
                       (start, stop, measure_id))
        result = self.cursor.fetchone()
        if result and result[0] is not None:
            return result[0]

    def get_sample_rate_from_measurement_session_id(self, measure_id: str):
        """
        Retrieves the sample rate from a specific measurement session ID.

        Args:
            measure_id (str): The ID of the measurement session.

        Returns:
            float: The sample rate of the measurement session.
        """
        self.cursor.execute("SELECT measurement_id FROM measure_session WHERE id = %s", (measure_id,))
        result = self.cursor.fetchone()
        self.cursor.execute("SELECT sample_rate FROM measurement WHERE id = %s", (result[0],))
        sample_rate = self.cursor.fetchone()
        if sample_rate and sample_rate[0] is not None:
            return int(sample_rate[0])

    def close(self):
        self.cursor.close()
        self.conn.close()

