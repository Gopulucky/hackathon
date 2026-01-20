import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from matplotlib.dates import DateFormatter
import matplotlib.dates as mdates


# --- SETUP & DATA LOADING ---
import os
import glob


print("Loading data for Trilateral Analysis...")

def load_split_csv(base_name):
    # Construct searching pattern
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, 'cleaned_data')
    pattern = os.path.join(data_dir, f"{base_name}_part*.csv")
    
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files found for {base_name} in {data_dir}")
        
    print(f"Loading {len(files)} parts for {base_name}...")
    dfs = [pd.read_csv(f) for f in files]
    return pd.concat(dfs, ignore_index=True)

biometric_df = load_split_csv('biometric_cleaned')
demographic_df = load_split_csv('demographic_cleaned')
enrolment_df = load_split_csv('enrolment_cleaned')

# Convert date columns
biometric_df['date'] = pd.to_datetime(biometric_df['date'])
demographic_df['date'] = pd.to_datetime(demographic_df['date'])
enrolment_df['date'] = pd.to_datetime(enrolment_df['date'])

# Calculate totals
biometric_df['total_updates'] = biometric_df['bio_age_5_17'] + biometric_df['bio_age_17_']
enrolment_df['total_enrolment'] = enrolment_df['age_0_5'] + enrolment_df['age_5_17'] + enrolment_df['age_18_greater']

sns.set_style("whitegrid")

# Date formatter for month names
date_format = DateFormatter("%b %Y")  # e.g., "Jan 2024"

# --- BIOMETRIC VISUALIZATIONS ---
print("Generating Biometric Visualizations...")

# Figure 1: Biometric - Top 10 States
plt.figure(figsize=(12, 6))
state_bio = biometric_df.groupby('state')['total_updates'].sum().sort_values(ascending=False).head(10)
sns.barplot(x=state_bio.values, y=state_bio.index, palette='viridis')
plt.title('Biometric: Top 10 States by Total Updates', fontsize=14, fontweight='bold')
plt.xlabel('Total Updates')
plt.ylabel('State')
plt.tight_layout()
plt.show()

# Figure 2: Biometric - All States
plt.figure(figsize=(12, 16))
state_bio_all = biometric_df.groupby('state')['total_updates'].sum().sort_values(ascending=False)
sns.barplot(x=state_bio_all.values, y=state_bio_all.index, palette='coolwarm')
plt.title('Biometric: All States by Total Updates', fontsize=14, fontweight='bold')
plt.xlabel('Total Updates')
plt.ylabel('State')
plt.tight_layout()
plt.show()

# Figure 3: Biometric - Age Group Distribution
plt.figure(figsize=(10, 6))
bio_age_totals = {
    'Age 5-17': biometric_df['bio_age_5_17'].sum(),
    'Age 17+': biometric_df['bio_age_17_'].sum()
}
plt.pie(bio_age_totals.values(), labels=bio_age_totals.keys(), autopct='%1.1f%%', 
        colors=['#66b3ff', '#ff9999'], startangle=90)
plt.title('Biometric: Updates Distribution by Age Group', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()

# Figure 4: Biometric - Age Group Comparison (Stacked)
plt.figure(figsize=(12, 6))
top_states = biometric_df.groupby('state')['total_updates'].sum().sort_values(ascending=False).head(10).index
bio_state_age = biometric_df[biometric_df['state'].isin(top_states)].groupby('state')[['bio_age_5_17', 'bio_age_17_']].sum()
bio_state_age.plot(kind='bar', stacked=True, color=['#8dd3c7', '#fb8072'])
plt.title('Biometric: Age Group Distribution (Top 10 States)', fontsize=14, fontweight='bold')
plt.xlabel('State')
plt.ylabel('Total Updates')
plt.legend(['Age 5-17', 'Age 17+'])
plt.tight_layout()
plt.show()

# Figure 5: Biometric - Daily Trend
fig, ax = plt.subplots(figsize=(14, 6))
daily_bio = biometric_df.groupby('date')['total_updates'].sum()
ax.plot(daily_bio.index, daily_bio.values, color='blue', linewidth=2, marker='o', markersize=4)
ax.xaxis.set_major_formatter(date_format)
ax.xaxis.set_major_locator(mdates.MonthLocator())
plt.xticks(rotation=45, ha='right')
plt.title('Biometric: Daily Updates Trend', fontsize=14, fontweight='bold')
plt.xlabel('Month')
plt.ylabel('Total Updates')
plt.grid(alpha=0.3)
plt.tight_layout()
plt.show()

# Figure 6: Biometric - Age Group Trends
fig, ax = plt.subplots(figsize=(14, 6))
daily_bio_age = biometric_df.groupby('date')[['bio_age_5_17', 'bio_age_17_']].sum()
ax.plot(daily_bio_age.index, daily_bio_age['bio_age_5_17'], label='Age 5-17', linewidth=2)
ax.plot(daily_bio_age.index, daily_bio_age['bio_age_17_'], label='Age 17+', linewidth=2)
ax.xaxis.set_major_formatter(date_format)
ax.xaxis.set_major_locator(mdates.MonthLocator())
plt.xticks(rotation=45, ha='right')
plt.title('Biometric: Daily Updates by Age Group', fontsize=14, fontweight='bold')
plt.xlabel('Month')
plt.ylabel('Updates')
plt.legend()
plt.tight_layout()
plt.show()

# Figure 7: Biometric - Cumulative Updates
fig, ax = plt.subplots(figsize=(14, 6))
ax.fill_between(daily_bio_age.index, 0, daily_bio_age['bio_age_5_17'], alpha=0.5, label='Age 5-17')
ax.fill_between(daily_bio_age.index, daily_bio_age['bio_age_5_17'], 
                 daily_bio_age['bio_age_5_17'] + daily_bio_age['bio_age_17_'], alpha=0.5, label='Age 17+')
ax.xaxis.set_major_formatter(date_format)
ax.xaxis.set_major_locator(mdates.MonthLocator())
plt.xticks(rotation=45, ha='right')
plt.title('Biometric: Cumulative Daily Updates by Age Group', fontsize=14, fontweight='bold')
plt.xlabel('Month')
plt.ylabel('Updates')
plt.legend()
plt.tight_layout()
plt.show()

# --- DEMOGRAPHIC VISUALIZATIONS ---
print("Generating Demographic Visualizations...")

# Figure 8: Demographic - Top 10 States
plt.figure(figsize=(12, 6))
state_demo = demographic_df.groupby('state')['demo_age_5_17'].sum().sort_values(ascending=False).head(10)
sns.barplot(x=state_demo.values, y=state_demo.index, palette='plasma')
plt.title('Demographic: Top 10 States by Updates (Age 5-17)', fontsize=14, fontweight='bold')
plt.xlabel('Total Updates')
plt.ylabel('State')
plt.tight_layout()
plt.show()

# Figure 9: Demographic - All States
plt.figure(figsize=(12, 16))
state_demo_all = demographic_df.groupby('state')['demo_age_5_17'].sum().sort_values(ascending=False)
sns.barplot(x=state_demo_all.values, y=state_demo_all.index, palette='YlOrRd')
plt.title('Demographic: All States by Updates', fontsize=14, fontweight='bold')
plt.xlabel('Total Updates')
plt.ylabel('State')
plt.tight_layout()
plt.show()

# Figure 10: Demographic - Daily Trend
fig, ax = plt.subplots(figsize=(14, 6))
daily_demo = demographic_df.groupby('date')['demo_age_5_17'].sum()
ax.plot(daily_demo.index, daily_demo.values, color='green', linewidth=2, marker='o')
ax.xaxis.set_major_formatter(date_format)
ax.xaxis.set_major_locator(mdates.MonthLocator())
plt.xticks(rotation=45, ha='right')
plt.title('Demographic: Daily Updates Trend', fontsize=14, fontweight='bold')
plt.xlabel('Month')
plt.ylabel('Updates')
plt.tight_layout()
plt.show()

# Figure 11: Demographic - Area Chart
fig, ax = plt.subplots(figsize=(14, 6))
ax.fill_between(daily_demo.index, daily_demo.values, alpha=0.5, color='lightgreen')
ax.plot(daily_demo.index, daily_demo.values, color='darkgreen', linewidth=2)
ax.xaxis.set_major_formatter(date_format)
ax.xaxis.set_major_locator(mdates.MonthLocator())
plt.xticks(rotation=45, ha='right')
plt.title('Demographic: Cumulative Daily Updates', fontsize=14, fontweight='bold')
plt.xlabel('Month')
plt.ylabel('Updates')
plt.tight_layout()
plt.show()

# --- ENROLMENT VISUALIZATIONS ---
print("Generating Enrolment Visualizations...")

# Figure 12: Enrolment - Top 10 States
plt.figure(figsize=(12, 6))
state_enrol = enrolment_df.groupby('state')['total_enrolment'].sum().sort_values(ascending=False).head(10)
sns.barplot(x=state_enrol.values, y=state_enrol.index, palette='rocket')
plt.title('Enrolment: Top 10 States', fontsize=14, fontweight='bold')
plt.xlabel('Total Enrolment')
plt.ylabel('State')
plt.tight_layout()
plt.show()

# Figure 13: Enrolment - All States
plt.figure(figsize=(12, 16))
state_enrol_all = enrolment_df.groupby('state')['total_enrolment'].sum().sort_values(ascending=False)
sns.barplot(x=state_enrol_all.values, y=state_enrol_all.index, palette='mako')
plt.title('Enrolment: All States', fontsize=14, fontweight='bold')
plt.xlabel('Total Enrolment')
plt.ylabel('State')
plt.tight_layout()
plt.show()

# Figure 14: Enrolment - Age Group Distribution
plt.figure(figsize=(10, 6))
enrol_age_totals = {
    'Age 0-5': enrolment_df['age_0_5'].sum(),
    'Age 5-17': enrolment_df['age_5_17'].sum(),
    'Age 18+': enrolment_df['age_18_greater'].sum()
}
plt.pie(enrol_age_totals.values(), labels=enrol_age_totals.keys(), autopct='%1.1f%%', 
        colors=['#ff9999', '#66b3ff', '#99ff99'], startangle=90)
plt.title('Enrolment: Distribution by Age Group', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()

# Figure 15: Enrolment - Age Group Comparison
plt.figure(figsize=(12, 6))
top_enrol_states = enrolment_df.groupby('state')['total_enrolment'].sum().sort_values(ascending=False).head(10).index
enrol_state_age = enrolment_df[enrolment_df['state'].isin(top_enrol_states)].groupby('state')[['age_0_5', 'age_5_17', 'age_18_greater']].sum()
enrol_state_age.plot(kind='bar', stacked=True, color=['#ffd700', '#87ceeb', '#98fb98'])
plt.title('Enrolment: Age Group Distribution (Top 10 States)', fontsize=14, fontweight='bold')
plt.xlabel('State')
plt.ylabel('Total Enrolment')
plt.legend(['Age 0-5', 'Age 5-17', 'Age 18+'])
plt.tight_layout()
plt.show()

# Figure 16: Enrolment - Daily Trend
fig, ax = plt.subplots(figsize=(14, 6))
daily_enrolment = enrolment_df.groupby('date')['total_enrolment'].sum()
ax.plot(daily_enrolment.index, daily_enrolment.values, color='coral', linewidth=2, marker='o')
ax.xaxis.set_major_formatter(date_format)
ax.xaxis.set_major_locator(mdates.MonthLocator())
plt.xticks(rotation=45, ha='right')
plt.title('Enrolment: Daily Total Enrolment Trend', fontsize=14, fontweight='bold')
plt.xlabel('Month')
plt.ylabel('Total Enrolment')
plt.tight_layout()
plt.show()

# Figure 17: Enrolment - Age Group Trends
fig, ax = plt.subplots(figsize=(14, 6))
age_daily = enrolment_df.groupby('date')[['age_0_5', 'age_5_17', 'age_18_greater']].sum()
ax.plot(age_daily.index, age_daily['age_0_5'], label='Age 0-5', linewidth=2)
ax.plot(age_daily.index, age_daily['age_5_17'], label='Age 5-17', linewidth=2)
ax.plot(age_daily.index, age_daily['age_18_greater'], label='Age 18+', linewidth=2)
ax.xaxis.set_major_formatter(date_format)
ax.xaxis.set_major_locator(mdates.MonthLocator())
plt.xticks(rotation=45, ha='right')
plt.title('Enrolment: Daily Trends by Age Group', fontsize=14, fontweight='bold')
plt.xlabel('Month')
plt.ylabel('Enrolment')
plt.legend()
plt.tight_layout()
plt.show()

# Figure 18: Enrolment - Cumulative Area Chart
fig, ax = plt.subplots(figsize=(14, 6))
ax.fill_between(age_daily.index, 0, age_daily['age_0_5'], alpha=0.5, label='Age 0-5', color='gold')
ax.fill_between(age_daily.index, age_daily['age_0_5'], 
                 age_daily['age_0_5'] + age_daily['age_5_17'], alpha=0.5, label='Age 5-17', color='skyblue')
ax.fill_between(age_daily.index, age_daily['age_0_5'] + age_daily['age_5_17'],
                 age_daily['age_0_5'] + age_daily['age_5_17'] + age_daily['age_18_greater'], alpha=0.5, label='Age 18+', color='lightgreen')
ax.xaxis.set_major_formatter(date_format)
ax.xaxis.set_major_locator(mdates.MonthLocator())
plt.xticks(rotation=45, ha='right')
plt.title('Enrolment: Cumulative Daily Enrolment', fontsize=14, fontweight='bold')
plt.xlabel('Month')
plt.ylabel('Enrolment')
plt.legend()
plt.tight_layout()
plt.show()

print("Unilateral Analysis Complete.")