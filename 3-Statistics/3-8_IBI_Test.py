from RXLDBC import connect

import datetime
import pandas as pd
import numpy as np
import neurokit2 as nk



conn = connect.Connection()

for patient in conn.get_all_patient_ids():
    print(f"Processing patient ID: {patient}")
    for group in conn.get_all_measurement_groups_from_patient_id(patient):
        print(group)
        sessions = conn.get_all_measurement_sessions_from_group_id(group[0])
        for session in sessions:
            if session[1].split("_")[-1] == "IBI":
                valid_sessions = conn.get_valid_data_from_measure_session(session[0])

                if valid_sessions:
                    for start_time, data in valid_sessions.items():
                        time_sequence = [entry[0] for entry in data][0:1000]
                        ibi_sequence = [round(entry[1] * 1000) for entry in data][0:1000]

                        # Only process if there are at least 1000 IBI points
                        if len(ibi_sequence) < 1000:
                            print(f"Skipping session at {start_time}: not enough IBI data ({len(ibi_sequence)} points)")
                            continue

                        rr_dict = nk.intervals_to_peaks(ibi_sequence, time_sequence)
                        signals = nk.hrv(rr_dict)
                        signals2 = nk.hrv_frequency(rr_dict)
                        print(signals)