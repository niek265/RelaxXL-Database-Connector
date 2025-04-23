import concurrent.futures
import os

from RXLDBC import connect, plot

def make_plot_of_patient(patient_id):
    print(f"Patient ID: {patient_id}")
    conn = connect.Connection()
    (vitals, sessions) = conn.get_all_sessions_from_patient_id(patient_id)
    conn.close()
    try:
        stats = plot.plot_weekly_gantt(vitals, sessions)
        return stats
    except Exception as e:
        print(f"Error plotting for patient {patient_id}: {e}")
        return

def main():
    # Create a connection to the database
    conn = connect.Connection()
    patients = conn.get_all_patient_ids()
    conn.close()

    statistics = []


    # Use ThreadPoolExecutor for parallel execution
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit tasks for each patient
        future_to_patient = {executor.submit(make_plot_of_patient, patient_id): patient_id for patient_id in patients}

        # Collect results as they complete
        for future in concurrent.futures.as_completed(future_to_patient):
            patient_id = future_to_patient[future]
            try:
                stats = future.result()
                statistics.append(stats)
            except Exception as e:
                print(f"Error processing patient {patient_id}: {e}")


    # Remove all None values from the statistics list
    statistics = [stat for stat in statistics if stat is not None]

    # Sort the statistics by patient ID
    statistics.sort(key=lambda x: x[0])


    # Save the statistics to a CSV file
    with open("patient_statistics.csv", "w") as f:
        f.write("Patient ID,Relaxation Sessions,Measurement Sessions\n")
        for stat in statistics:
            f.write(f"{stat[0]},{stat[1]},{stat[2]},{stat[3]}\n")

if __name__ == "__main__":
    main()