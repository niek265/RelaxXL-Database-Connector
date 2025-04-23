import matplotlib.pyplot as plt
import datetime

def get_measurement_type(name):
    """
    Determine a simplified measurement type from the measurement name.
    Measurements containing:
    - "ACC" are grouped together (combining all acceleration signals),
    - "EDA" are grouped as EDA,
    - "BVP" as BVP,
    - "TEMP" as TEMP,
    - "IBI" as IBI,
    - "HR" as HR.
    Otherwise, the original name is returned.
    """
    if "ACC" in name:
        return "ACC"
    if "EDA" in name:
        return "EDA"
    if "BVP" in name:
        return "BVP"
    if "TEMP" in name:
        return "TEMP"
    if "IBI" in name:
        return "IBI"
    if "HR" in name:
        return "HR"
    if "VR" in name:
        return "*VR"
    if "Exercise" in name:
        return "*Exercise"
    return name


def plot_weekly_gantt(vitals, relax_sessions):
    """
    Create a Gantt-like horizontal bar chart over 10 weeks.
    Each week is represented by a 168-hour time block and is subdivided by measurement type.
    """
    all_sessions_dict = vitals | relax_sessions

    # Find the earliest timestamp from both vitals and vr_sessions
    earliest_timestamp = min([start for start, _ in vitals.values()])

    def get_monday_of_week(date):
        return date - datetime.timedelta(days=date.weekday())

    week_data = {get_monday_of_week(earliest_timestamp).date() + datetime.timedelta(weeks=i): [] for i in range(11)}
    for key, (start, end) in all_sessions_dict.items():
        # Place the session in the correct week bucket where it matches
        week_start = get_monday_of_week(start)
        week_end = get_monday_of_week(end)
        for week in week_data.keys():
            if week_start.date() == week:
                if week_start.date() == week_end.date():
                    # Perfect scenario, start and end are in the same week
                    week_data[week].append({key: (start, end)})
                else:
                    week_data[week].append({key: (start, datetime.datetime.combine(week,
                                                                                   datetime.time.min) + datetime.timedelta(
                        days=6, hours=23, minutes=59))})
                    week_data[week + datetime.timedelta(days=7)].append(
                        {key: (datetime.datetime.combine(week, datetime.time.min) + datetime.timedelta(days=7), end)})

    week_data_hours = {}
    # Calculate how many hours each session starts and stops after the beginning of the week
    for index, (week, sessions) in enumerate(week_data.items()):
        # Get the midnight time of the start of the week
        week_start = datetime.datetime.combine(week, datetime.time.min)
        for session in sessions:
            for key, (start, end) in session.items():
                # Calculate the duration in hours since the start of the week
                start_time = (start - week_start).total_seconds() / 3600
                end_time = (end - week_start).total_seconds() / 3600
                # Add the session to the week_data_hours dictionary
                if index not in week_data_hours:
                    week_data_hours[index] = []
                    week_data_hours[index].append({key: (start_time, end_time)})
                else:
                    week_data_hours[index].append({key: (start_time, end_time)})

    # Initialize a container for 10 weeks (keys 0 through 9)
    weeks = {i: {} for i in range(10)}

    # Extract the patient ID from the first value of the first key
    patient = list(all_sessions_dict.keys())[0].split("_")[0]

    patient_group = ""

    # Organize the data by week and by measurement group
    for week_key, entries in week_data_hours.items():
        if week_key not in weeks:
            continue
        for entry in entries:
            for meas_name, (start, end) in entry.items():
                group = get_measurement_type(meas_name)
                weeks[week_key].setdefault(group, []).append((start, end))
                if group == "*VR":
                    patient_group = "VR"
                elif group == "*Exercise":
                    patient_group = "Exercise"


    # Collect all unique measurement groups for legend/color mapping
    unique_groups = sorted({grp for week in weeks.values() for grp in week.keys()})

    # Define a list of colors and assign one to each group
    colors = ['tab:blue', 'tab:orange', 'tab:green', 'tab:red', 'tab:purple',
              'tab:brown', 'tab:pink', 'tab:gray', 'tab:olive', 'tab:cyan']
    group_color = {grp: colors[i % len(colors)] for i, grp in enumerate(unique_groups)}

    # Create the plot
    fig, ax = plt.subplots(figsize=(16, 8))

    # Each week gets a vertical slot. Here we reserve 10 units per week.
    week_slot = 10
    for week in range(10):
        y_base = week * week_slot
        groups = weeks[week]
        n_groups = len(groups)
        if n_groups == 0:
            # Optional: Display an empty background if no measurements for the week
            ax.broken_barh([], (y_base + 1, week_slot - 2), facecolors='none', edgecolors='lightgray')
        else:
            # Subdivide each week’s slot equally among the measurement groups
            seg_height = (week_slot - 2) / n_groups
            for idx, (grp, intervals) in enumerate(groups.items()):
                y_pos = y_base + 1 + idx * seg_height
                for start, end in intervals:
                    duration = end - start
                    ax.broken_barh([(start, duration)], (y_pos, seg_height * 0.8),
                                   facecolors=group_color[grp], edgecolors='black')

    # Configure the axes
    ax.set_xlim(0, 168)
    ax.set_ylim(0, 10 * week_slot)
    ax.set_xlabel("Hours of Week (0-168)")
    ax.set_ylabel("Weeks")
    # Set y-ticks to be in the middle of each week's vertical slot and label them accordingly
    ax.set_yticks([i * week_slot + week_slot / 2 for i in range(10)])
    ax.set_yticklabels([f"Week {i + 1}" for i in range(10)])

    # Create a legend for the measurement types
    legend_handles = [plt.Rectangle((0, 0), 1, 1, color=group_color[grp])
                      for grp in unique_groups]
    ax.legend(legend_handles, unique_groups, title="Measurement Type", bbox_to_anchor=(1.05, 1), loc='upper left')

    # Add some statistics underneath the legend
    ax.text(183, 70, "Statistics", ha='center', va='center', fontsize=12, fontweight='bold')
    ax.text(183, 68, f"Relax sessions: {len(relax_sessions)}", ha='center', va='center', fontsize=10)
    ax.text(183, 66, f"Time Relaxed: {sum([(end - start).total_seconds() / 3600 for start, end in relax_sessions.values()]):.2f} hours", ha='center', va='center', fontsize=10)

    # Make a list with statistics
    stats = [patient, patient_group, len(relax_sessions), round(sum([(end - start).total_seconds() / 3600 for start, end in relax_sessions.values()]),ndigits=3)]
    ax.set_title(f"Data for patient {patient} ({patient_group})", fontsize=16)
    plt.tight_layout()

    # Save the plot to a file
    plt.savefig(f"{patient}_{patient_group}.svg", bbox_inches='tight')
    plt.savefig(f"{patient}_{patient_group}.png", bbox_inches='tight')
    plt.close()
    return stats



