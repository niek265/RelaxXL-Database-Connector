from RXLDBC import connect
import flirt
import matplotlib.pyplot as plt

path = "C:/Users/niek2/Documents/Data/Data_Empatica_E4/H001/Week1/1676699894_A00F3D.zip"

ibi_df = flirt.reader.empatica.read_ibi_file_into_df("C:/Users/niek2/Documents/Data/IBI.csv")

hrv_features = flirt.get_hrv_features(ibi_df['ibi'],
                                      window_length = 180,
                                      window_step_size = 1,
                                      domains = ['td', 'fd', 'stat'],
                                      threshold = 0.2,
                                      clean_data = True)

# Select sensor modality and plot some features
fig, axs = plt.subplots(2, 1, sharex=True, figsize=(24, 12))
axs = axs.ravel()

axs[0].plot(ibi_df['ibi'], color = "blue")
axs[0].set_title('IBI series')
axs[0].set_ylabel('ms', weight = 'bold')

axs[1].plot(hrv_features['hrv_rmssd'], color = "red")
axs[1].set_title('HRV - RMSSD')
axs[1].set_ylabel('ms', weight = 'bold')

# Show the plot
plt.tight_layout()
plt.show()

#_ = hrv_features[['hrv_mean_hr', 'hrv_rmssd']].plot(figsize=(24, 18))


conn = connect.Connection()
cursor = conn.conn.cursor()

ibi = []

ibi = (conn.get_all_ibi_from_patient_id("H001"))

# Plot the IBI data individually for each session
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

for key, (start_timestamp, end_timestamp, data) in ibi.items():
    # Convert timestamps to pandas datetime
    start = pd.to_datetime(start_timestamp)
    end = pd.to_datetime(end_timestamp)

    # Create a time series for the IBI data
    times = pd.date_range(start=start, end=end, periods=len(data))
    ibi_values = [d[1] for d in data]  # Assuming data is a list of tuples (timestamp, ibi_value)

    # Create a DataFrame for plotting
    df = pd.DataFrame({'Time': times, 'IBI': ibi_values})

    plt.figure(figsize=(10, 5))
    plt.plot(df['Time'], df['IBI'], marker='o', linestyle='-', label=key)
    plt.title(f'IBI Data for Session {key}')
    plt.xlabel('Time')
    plt.ylabel('IBI (s)')
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.show()

