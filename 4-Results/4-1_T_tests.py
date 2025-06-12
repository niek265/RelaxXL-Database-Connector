import pandas as pd
from scipy.stats import ttest_rel, ttest_ind
from statsmodels.stats.multitest import multipletests

SESSION_CSV = "C:/Users/niek2/PycharmProjects/RelaxXL-Database-Connector/RelaxXL_Session_Statistics.csv"

session_df = pd.read_csv(SESSION_CSV, delimiter=';', header=0, decimal=',')

vr_before = session_df[session_df["patient_group"] == "VR"]["HR_mean_before"]
vr_during = session_df[session_df["patient_group"] == "VR"]["HR_mean_during"]
vr_after = session_df[session_df["patient_group"] == "VR"]["HR_mean_after"]

vr_before = session_df[session_df["patient_group"] == "VR"]["SCR_per_minute_before"]
vr_during = session_df[session_df["patient_group"] == "VR"]["SCR_per_minute_during"]
vr_after = session_df[session_df["patient_group"] == "VR"]["SCR_per_minute_after"]

ex_before = session_df[session_df["patient_group"] == "Exercise"]["HR_mean_before"]
ex_during = session_df[session_df["patient_group"] == "Exercise"]["HR_mean_during"]
ex_after = session_df[session_df["patient_group"] == "Exercise"]["HR_mean_after"]

ex_before = session_df[session_df["patient_group"] == "Exercise"]["SCR_per_minute_before"]
ex_during = session_df[session_df["patient_group"] == "Exercise"]["SCR_per_minute_during"]
ex_after = session_df[session_df["patient_group"] == "Exercise"]["SCR_per_minute_after"]


# ─── WITHIN‐GROUP (paired) T‐TESTS ───
stat1, p1 = ttest_rel(vr_before, vr_during)
stat2, p2 = ttest_rel(vr_before, vr_after)
stat3, p3 = ttest_rel(vr_during, vr_after)

raw_p_vr = [p1, p2, p3]
_, adj_p_vr, _, _ = multipletests(raw_p_vr, alpha=0.05, method="bonferroni")

print("VR within‐group (paired) t‐tests:")
for (comp, rawp, adjp) in zip(
    [("Before", "During"), ("Before", "After"), ("During", "After")],
    raw_p_vr,
    adj_p_vr
):
    print(f"  {comp[0]} vs {comp[1]}: raw p = {rawp:.4f}, Bonferroni p = {adjp:.4f}")

#    Exercise:  Before vs During, Before vs After, During vs After
stat4, p4 = ttest_rel(ex_before, ex_during)
stat5, p5 = ttest_rel(ex_before, ex_after)
stat6, p6 = ttest_rel(ex_during, ex_after)

raw_p_ex = [p4, p5, p6]
_, adj_p_ex, _, _ = multipletests(raw_p_ex, alpha=0.05, method="bonferroni")

print("\nExercise within‐group (paired) t‐tests:")
for (comp, rawp, adjp) in zip(
    [("Before", "During"), ("Before", "After"), ("During", "After")],
    raw_p_ex,
    adj_p_ex
):
    print(f"  {comp[0]} vs {comp[1]}: raw p = {rawp:.10f}, Bonferroni p = {adjp:.10f}")


WEEK_CSV = "C:/Users/niek2/PycharmProjects/RelaxXL-Database-Connector/RelaxXL_Week_Statistics.csv"
week_df = pd.read_csv(WEEK_CSV, delimiter=';', header=0, decimal=',')


VR_HR_Mean_Week1 = week_df[week_df["patient_group"] == "VR"]["HR_mean_week1"]
VR_HR_Mean_Week2 = week_df[week_df["patient_group"] == "VR"]["HR_mean_week2"]

Exercise_HR_Mean_Week1 = week_df[week_df["patient_group"] == "Exercise"]["HR_mean_week1"]
Exercise_HR_Mean_Week2 = week_df[week_df["patient_group"] == "Exercise"]["HR_mean_week2"]

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



# ─── WITHIN‐GROUP (paired) T‐TESTS ───
stat1, p1 = ttest_rel(VR_SCR_Peaks_Week1, VR_SCR_Peaks_Week2)

raw_p_vr = [p1]
_, adj_p_vr, _, _ = multipletests(raw_p_vr, alpha=0.05, method="bonferroni")

print("VR week (paired) t‐tests:")
for (comp, rawp, adjp) in zip(
        [("Before", "During"), ("Before", "After"), ("During", "After")],
        raw_p_vr,
        adj_p_vr
):
    print(f"  {comp[0]} vs {comp[1]}: raw p = {rawp:.4f}, Bonferroni p = {adjp:.4f}")

#    Exercise:  Before vs During, Before vs After, During vs After
stat4, p4 = ttest_rel(Exercise_SCR_Peaks_Week1, Exercise_SCR_Peaks_Week2)

raw_p_ex = [p4]
_, adj_p_ex, _, _ = multipletests(raw_p_ex, alpha=0.05, method="bonferroni")

print("\nExercise week (paired) t‐tests:")
for (comp, rawp, adjp) in zip(
        [("Before", "During"), ("Before", "After"), ("During", "After")],
        raw_p_ex,
        adj_p_ex
):
    print(f"  {comp[0]} vs {comp[1]}: raw p = {rawp:.10f}, Bonferroni p = {adjp:.10f}")






