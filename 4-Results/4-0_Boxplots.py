import pandas as pd
import plotly.graph_objects as go

SESSION_CSV = "C:/Users/niek2/PycharmProjects/RelaxXL-Database-Connector/RelaxXL_Session_Statistics.csv"

session_df = pd.read_csv(SESSION_CSV, delimiter=';', header=0, decimal=',')

VR_HR_Before = session_df[session_df["patient_group"] == "VR"]["HR_mean_before"]
VR_HR_During = session_df[session_df["patient_group"] == "VR"]["HR_mean_during"]
VR_HR_After = session_df[session_df["patient_group"] == "VR"]["HR_mean_after"]

Exercise_HR_Before = session_df[session_df["patient_group"] == "Exercise"]["HR_mean_before"]
Exercise_HR_During = session_df[session_df["patient_group"] == "Exercise"]["HR_mean_during"]
Exercise_HR_After = session_df[session_df["patient_group"] == "Exercise"]["HR_mean_after"]

fig = go.Figure()

# Make a grouped boxplot
fig.add_trace(go.Box(
    y=VR_HR_Before,
    name='VR Before',
    boxmean='sd',
    marker_color='blue',
    boxpoints='suspectedoutliers',
))
fig.add_trace(go.Box(
    y=VR_HR_During,
    name='VR During',
    boxmean='sd',
    marker_color='blue',
    boxpoints='suspectedoutliers',
    opacity=0.5
))
fig.add_trace(go.Box(
    y=VR_HR_After,
    name='VR After',
    boxmean='sd',
    marker_color='blue',
    boxpoints='suspectedoutliers',
    opacity=0.2
))
fig.add_trace(go.Box(
    y=Exercise_HR_Before,
    name='Exercise Before',
    boxmean='sd',
    marker_color='red',
    boxpoints='suspectedoutliers'
))
fig.add_trace(go.Box(
    y=Exercise_HR_During,
    name='Exercise During',
    boxmean='sd',
    marker_color='red',
    boxpoints='suspectedoutliers',
    opacity=0.5
))
fig.add_trace(go.Box(
    y=Exercise_HR_After,
    name='Exercise After',
    boxmean='sd',
    marker_color='red',
    boxpoints='suspectedoutliers',
    opacity=0.2,

))
fig.update_layout(
    title='Average Heart Rate Before, During, and After VR and Exercise Sessions',
    yaxis_title='Heart Rate (bpm)',
    xaxis_title='Session Type',
    boxmode='group',
    font = dict(
    size=22,
    color='black'
    )
)
fig.update_layout(
    width=1200,
    height=1000,
    template='plotly_white'
)
# make the boxes wider
fig.update_traces(width=0.7)

# Add a line through the mean of each group seperately
fig.add_trace(go.Scatter(
    x=['VR Before', 'VR During', 'VR After'],
    y=[VR_HR_Before.mean(), VR_HR_During.mean(), VR_HR_After.mean()],
    mode='lines+markers',
    name='Mean',
    line=dict(color='black', width=2, dash='dash')
))

fig.add_trace(go.Scatter(
    x=['Exercise Before', 'Exercise During', 'Exercise After'],
    y=[Exercise_HR_Before.mean(), Exercise_HR_During.mean(), Exercise_HR_After.mean()],
    mode='lines+markers',
    name='Mean',
    line=dict(color='black', width=2, dash='dash'),
    marker=dict(symbol='diamond'),
))

# Show the plot
fig.show()

VR_SCR_Before = session_df[session_df["patient_group"] == "VR"]["SCR_per_minute_before"]
VR_SCR_During = session_df[session_df["patient_group"] == "VR"]["SCR_per_minute_during"]
VR_SCR_After = session_df[session_df["patient_group"] == "VR"]["SCR_per_minute_after"]
Exercise_SCR_Before = session_df[session_df["patient_group"] == "Exercise"]["SCR_per_minute_before"]
Exercise_SCR_During = session_df[session_df["patient_group"] == "Exercise"]["SCR_per_minute_during"]
Exercise_SCR_After = session_df[session_df["patient_group"] == "Exercise"]["SCR_per_minute_after"]

fig2 = go.Figure()

# Make a grouped boxplot for SCR peaks
fig2.add_trace(go.Box(
    y=VR_SCR_Before,
    name='VR Before',
    boxmean='sd',
    marker_color='blue',
    boxpoints='suspectedoutliers'
))
fig2.add_trace(go.Box(
    y=VR_SCR_During,
    name='VR During',
    boxmean='sd',
    marker_color='blue',
    boxpoints='suspectedoutliers',
    opacity=0.5
))
fig2.add_trace(go.Box(
    y=VR_SCR_After,
    name='VR After',
    boxmean='sd',
    marker_color='blue',
    boxpoints='suspectedoutliers',
    opacity=0.2
))
fig2.add_trace(go.Box(
    y=Exercise_SCR_Before,
    name='Exercise Before',
    boxmean='sd',
    marker_color='red',
    boxpoints='suspectedoutliers'
))
fig2.add_trace(go.Box(
    y=Exercise_SCR_During,
    name='Exercise During',
    boxmean='sd',
    marker_color='red',
    boxpoints='suspectedoutliers',
    opacity=0.5
))
fig2.add_trace(go.Box(
    y=Exercise_SCR_After,
    name='Exercise After',
    boxmean='sd',
    marker_color='red',
    boxpoints='suspectedoutliers',
    opacity=0.2
))
fig2.update_layout(
    title='SCR Peaks per minute Before, During, and After VR and Exercise Sessions',
    yaxis_title='SCR Peaks',
    xaxis_title='Session Type',
    boxmode='group'
)
fig2.update_layout(
    width=1200,
    height=1000,
    template='plotly_white',
    font = dict(
    size=22,
    color='black'
    )
)

# make the boxes wider
fig2.update_traces(width=0.7)
# Add a line through the mean of each group seperately
fig2.add_trace(go.Scatter(
    x=['VR Before', 'VR During', 'VR After'],
    y=[VR_SCR_Before.mean(), VR_SCR_During.mean(), VR_SCR_After.mean()],
    mode='lines+markers',
    name='Mean',
    line=dict(color='black', width=2, dash='dash')
))
fig2.add_trace(go.Scatter(
    x=['Exercise Before', 'Exercise During', 'Exercise After'],
    y=[Exercise_SCR_Before.mean(), Exercise_SCR_During.mean(), Exercise_SCR_After.mean()],
    mode='lines+markers',
    name='Mean',
    line=dict(color='black', width=2, dash='dash'),
    marker=dict(symbol='diamond'),
    # Add a label to each marker with the mean value

))

# Show the plot
fig2.show()

VR_Q_ontspanning_start = session_df[session_df["patient_group"] == "VR"]["Q_ontspanning_start"]
VR_Q_ontspanning_eind = session_df[session_df["patient_group"] == "VR"]["Q_ontspanning_eind"]
VR_Q_kalm_start = session_df[session_df["patient_group"] == "VR"]["Q_kalm_start"]
VR_Q_kalm_eind = session_df[session_df["patient_group"] == "VR"]["Q_kalm_eind"]

Exercise_Q_ontspanning_start = session_df[session_df["patient_group"] == "Exercise"]["Q_ontspanning_start"]
Exercise_Q_ontspanning_eind = session_df[session_df["patient_group"] == "Exercise"]["Q_ontspanning_eind"]
Exercise_Q_kalm_start = session_df[session_df["patient_group"] == "Exercise"]["Q_kalm_start"]
Exercise_Q_kalm_eind = session_df[session_df["patient_group"] == "Exercise"]["Q_kalm_eind"]

fig4 = go.Figure()

# Make a grouped boxplot for Q scores
fig4.add_trace(go.Box(
    y=VR_Q_ontspanning_start,
    name='VR Relaxed Start',
    boxmean='sd',
    marker_color='blue',
    boxpoints='suspectedoutliers'
))
fig4.add_trace(go.Box(
    y=VR_Q_ontspanning_eind,
    name='VR Relaxed End',
    boxmean='sd',
    marker_color='blue',
    boxpoints='suspectedoutliers',
    opacity=0.5
))
fig4.add_trace(go.Box(
    y=VR_Q_kalm_start,
    name='VR Calm Start',
    boxmean='sd',
    marker_color='lightblue',
    boxpoints='suspectedoutliers'
))
fig4.add_trace(go.Box(
    y=VR_Q_kalm_eind,
    name='VR Calm End',
    boxmean='sd',
    marker_color='lightblue',
    boxpoints='suspectedoutliers',
    opacity=0.5
))
fig4.add_trace(go.Box(
    y=Exercise_Q_ontspanning_start,
    name='Exercise Relaxed Start',
    boxmean='sd',
    marker_color='red',
    boxpoints='suspectedoutliers'
))
fig4.add_trace(go.Box(
    y=Exercise_Q_ontspanning_eind,
    name='Exercise Relaxed End',
    boxmean='sd',
    marker_color='red',
    opacity=0.5,
    boxpoints='suspectedoutliers'
))
fig4.add_trace(go.Box(
    y=Exercise_Q_kalm_start,
    name='Exercise Calm Start',
    boxmean='sd',
    marker_color='pink',
    boxpoints='suspectedoutliers',
))
fig4.add_trace(go.Box(
    y=Exercise_Q_kalm_eind,
    name='Exercise Calm End',
    boxmean='sd',
    marker_color='pink',
    boxpoints='suspectedoutliers',
    opacity=0.5
))
fig4.update_layout(
    title='Q Scores Before and After VR and Exercise Sessions',
    yaxis_title='Q Score',
    xaxis_title='Session Type',
    boxmode='group',
    font = dict(
    size=22,
    color='black'
    )
)
fig4.update_layout(
    width=1200,
    height=1000,
    template='plotly_white'
)
# make the boxes wider
fig4.update_traces(width=0.7)
# Add a line through the mean of each group seperately
fig4.add_trace(go.Scatter(
    x=['VR Relaxed Start', 'VR Relaxed End'],
    y=[VR_Q_ontspanning_start.mean(), VR_Q_ontspanning_eind.mean()],
    mode='lines+markers',
    name='Mean',
    line=dict(color='black', width=2, dash='dash')
))
fig4.add_trace(go.Scatter(
    x=['VR Calm Start', 'VR Calm End'],
    y=[VR_Q_kalm_start.mean(), VR_Q_kalm_eind.mean()],
    mode='lines+markers',
    name='Mean',
    line=dict(color='black', width=2, dash='dash'),
    marker=dict(symbol='diamond'),
))
fig4.add_trace(go.Scatter(
    x=['Exercise Relaxed Start', 'Exercise Relaxed End'],
    y=[Exercise_Q_ontspanning_start.mean(), Exercise_Q_ontspanning_eind.mean()],
    mode='lines+markers',
    name='Mean',
    line=dict(color='black', width=2, dash='dash')
))
fig4.add_trace(go.Scatter(
    x=['Exercise Calm Start', 'Exercise Calm End'],
    y=[Exercise_Q_kalm_start.mean(), Exercise_Q_kalm_eind.mean()],
    mode='lines+markers',
    name='Mean',
    line=dict(color='black', width=2, dash='dash'),
    marker=dict(symbol='diamond'),
))
# Show the plot
fig4.show()

# Make a grouped boxplot for session vector of magnitude
VR_VM_Before = session_df[session_df["patient_group"] == "VR"]["VM_mean_before"]
VR_VM_During = session_df[session_df["patient_group"] == "VR"]["VM_mean_during"]
VR_VM_After = session_df[session_df["patient_group"] == "VR"]["VM_mean_after"]
Exercise_VM_Before = session_df[session_df["patient_group"] == "Exercise"]["VM_mean_before"]
Exercise_VM_During = session_df[session_df["patient_group"] == "Exercise"]["VM_mean_during"]
Exercise_VM_After = session_df[session_df["patient_group"] == "Exercise"]["VM_mean_after"]

VMfig = go.Figure()

# Make a grouped boxplot for session vector of magnitude
VMfig.add_trace(go.Box(
    y=VR_VM_Before,
    name='VR Before',
    boxmean='sd',
    marker_color='blue',
    boxpoints='suspectedoutliers'
))

VMfig.add_trace(go.Box(
    y=VR_VM_During,
    name='VR During',
    boxmean='sd',
    marker_color='blue',
    boxpoints='suspectedoutliers',
    opacity=0.5
))
VMfig.add_trace(go.Box(
    y=VR_VM_After,
    name='VR After',
    boxmean='sd',
    marker_color='blue',
    boxpoints='suspectedoutliers',
    opacity=0.2
))
VMfig.add_trace(go.Box(
    y=Exercise_VM_Before,
    name='Exercise Before',
    boxmean='sd',
    marker_color='red',
    boxpoints='suspectedoutliers'
))
VMfig.add_trace(go.Box(
    y=Exercise_VM_During,
    name='Exercise During',
    boxmean='sd',
    marker_color='red',
    boxpoints='suspectedoutliers',
    opacity=0.5
))
VMfig.add_trace(go.Box(
    y=Exercise_VM_After,
    name='Exercise After',
    boxmean='sd',
    marker_color='red',
    boxpoints='suspectedoutliers',
    opacity=0.2
))
VMfig.update_layout(
    title='Vector of Magnitude Before, During, and After VR and Exercise sessions',
    yaxis_title='Session Vector of Magnitude',
    xaxis_title='Session Type',
    boxmode='group',
    font = dict(
    size=22,
    color='black'
    )
)
VMfig.update_layout(
    width=1200,
    height=1000,
    template='plotly_white'
)
# make the boxes wider
VMfig.update_traces(width=0.7)
# Add a line through the mean of each group seperately
VMfig.add_trace(go.Scatter(
    x=['VR Before', 'VR During', 'VR After'],
    y=[VR_VM_Before.mean(), VR_VM_During.mean(), VR_VM_After.mean()],
    mode='lines+markers',
    name='Mean',
    line=dict(color='black', width=2, dash='dash')
))
VMfig.add_trace(go.Scatter(
    x=['Exercise Before', 'Exercise During', 'Exercise After'],
    y=[Exercise_VM_Before.mean(), Exercise_VM_During.mean(), Exercise_VM_After.mean()],
    mode='lines+markers',
    name='Mean',
    line=dict(color='black', width=2, dash='dash'),
    marker=dict(symbol='diamond'),
))
# Show the plot
VMfig.show()















WEEK_CSV = "C:/Users/niek2/PycharmProjects/RelaxXL-Database-Connector/RelaxXL_Week_Statistics.csv"

week_df = pd.read_csv(WEEK_CSV, delimiter=';', header=0, decimal=',')

VR_HR_Mean_Week1 = week_df[week_df["patient_group"] == "VR"]["HR_mean_week1"]
VR_HR_Mean_Week2 = week_df[week_df["patient_group"] == "VR"]["HR_mean_week2"]

Exercise_HR_Mean_Week1 = week_df[week_df["patient_group"] == "Exercise"]["HR_mean_week1"]
Exercise_HR_Mean_Week2 = week_df[week_df["patient_group"] == "Exercise"]["HR_mean_week2"]

fig3 = go.Figure()

# Make a grouped boxplot for weekly HR means
fig3.add_trace(go.Box(
    y=VR_HR_Mean_Week1,
    name='VR Week 1',
    boxmean='sd',
    marker_color='blue',
    boxpoints='suspectedoutliers'
))
fig3.add_trace(go.Box(
    y=VR_HR_Mean_Week2,
    name='VR Week 2',
    boxmean='sd',
    marker_color='blue',
    boxpoints='suspectedoutliers',
    opacity=0.5
))

fig3.add_trace(go.Box(
    y=Exercise_HR_Mean_Week1,
    name='Exercise Week 1',
    boxmean='sd',
    marker_color='red',
    boxpoints='suspectedoutliers'
))
fig3.add_trace(go.Box(
    y=Exercise_HR_Mean_Week2,
    name='Exercise Week 2',
    boxmean='sd',
    marker_color='red',
    boxpoints='suspectedoutliers',
    opacity=0.5
))

fig3.update_layout(
    title='Average Heart Rate per Week for VR and Exercise Sessions',
    yaxis_title='Heart Rate (bpm)',
    xaxis_title='Session Type',
    boxmode='group',
    font = dict(
    size=22,
    color='black'
    )
)
fig3.update_layout(
    width=1200,
    height=1000,
    template='plotly_white'
)
# make the boxes wider
fig3.update_traces(width=0.7)
# Add a line through the mean of each group seperately
fig3.add_trace(go.Scatter(
    x=['VR Week 1', 'VR Week 2'],
    y=[VR_HR_Mean_Week1.mean(), VR_HR_Mean_Week2.mean()],
    mode='lines+markers',
    name='Mean',
    line=dict(color='black', width=2, dash='dash')
))
fig3.add_trace(go.Scatter(
    x=['Exercise Week 1', 'Exercise Week 2'],
    y=[Exercise_HR_Mean_Week1.mean(), Exercise_HR_Mean_Week2.mean()],
    mode='lines+markers',
    name='Mean',
    line=dict(color='black', width=2, dash='dash'),
    marker=dict(symbol='diamond'),
))

# Show the plot
fig3.show()


VR_SCR_Peaks_Week1 = week_df[week_df["patient_group"] == "VR"]["EDA_scr_peaks_week1"]
VR_SCR_Peaks_Week2 = week_df[week_df["patient_group"] == "VR"]["EDA_scr_peaks_week2"]

Exercise_SCR_Peaks_Week1 = week_df[week_df["patient_group"] == "Exercise"]["EDA_scr_peaks_week1"]
Exercise_SCR_Peaks_Week2 = week_df[week_df["patient_group"] == "Exercise"]["EDA_scr_peaks_week2"]

E4_Time_VR_Week1 = week_df[week_df["patient_group"] == "VR"]["week1_e4_duration"]
E4_Time_VR_Week2 = week_df[week_df["patient_group"] == "VR"]["week2_e4_duration"]

E4_Time_Ex_Week1 = week_df[week_df["patient_group"] == "Exercise"]["week1_e4_duration"]
E4_Time_Ex_Week2 = week_df[week_df["patient_group"] == "Exercise"]["week2_e4_duration"]

# Divide the SCR peaks by the E4 time to get the SCR peaks per minute
VR_SCR_Peaks_Week1 = VR_SCR_Peaks_Week1 / (E4_Time_VR_Week1 / 60)
VR_SCR_Peaks_Week2 = VR_SCR_Peaks_Week2 / (E4_Time_VR_Week2 / 60)
Exercise_SCR_Peaks_Week1 = Exercise_SCR_Peaks_Week1 / (E4_Time_Ex_Week1 / 60)
Exercise_SCR_Peaks_Week2 = Exercise_SCR_Peaks_Week2 / (E4_Time_Ex_Week2 / 60)





fig5 = go.Figure()
# Make a grouped boxplot for weekly SCR peaks
fig5.add_trace(go.Box(
    y=VR_SCR_Peaks_Week1,
    name='VR Week 1',
    boxmean='sd',
    marker_color='blue',
    boxpoints='suspectedoutliers'
))
fig5.add_trace(go.Box(
    y=VR_SCR_Peaks_Week2,
    name='VR Week 2',
    boxmean='sd',
    marker_color='blue',
    boxpoints='suspectedoutliers',
    opacity=0.5
))

fig5.add_trace(go.Box(
    y=Exercise_SCR_Peaks_Week1,
    name='Exercise Week 1',
    boxmean='sd',
    marker_color='red',
    boxpoints='suspectedoutliers'
))
fig5.add_trace(go.Box(
    y=Exercise_SCR_Peaks_Week2,
    name='Exercise Week 2',
    boxmean='sd',
    marker_color='red',
    boxpoints='suspectedoutliers',
    opacity=0.5
))
fig5.update_layout(
    title='SCR Peaks per minute per Week for VR and Exercise Sessions',
    yaxis_title='SCR Peaks per minute',
    xaxis_title='Session Type',
    boxmode='group',
    font = dict(
    size=22,
    color='black'
    )
)
fig5.update_layout(
    width=1200,
    height=1000,
    template='plotly_white'
)
# make the boxes wider
fig5.update_traces(width=0.7)
# Add a line through the mean of each group seperately
fig5.add_trace(go.Scatter(
    x=['VR Week 1', 'VR Week 2'],
    y=[VR_SCR_Peaks_Week1.mean(), VR_SCR_Peaks_Week2.mean()],
    mode='lines+markers',
    name='Mean',
    line=dict(color='black', width=2, dash='dash')
))
fig5.add_trace(go.Scatter(
    x=['Exercise Week 1', 'Exercise Week 2'],
    y=[Exercise_SCR_Peaks_Week1.mean(), Exercise_SCR_Peaks_Week2.mean()],
    mode='lines+markers',
    name='Mean',
    line=dict(color='black', width=2, dash='dash'),
    marker=dict(symbol='diamond'),
))
# Show the plot
fig5.show()

VR_Relax_Count = week_df[week_df["patient_group"] == "VR"]["patient_relax_count"]
VR_HR_Mean_Week2 = week_df[week_df["patient_group"] == "VR"]["HR_mean_week2"]




