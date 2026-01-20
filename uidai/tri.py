import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from math import pi
from mpl_toolkits.mplot3d import Axes3D

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

# Prepare Aggregations (State-wise)
bio_by_state = biometric_df.groupby('state')['total_updates'].sum()
demo_by_state = demographic_df.groupby('state')['demo_age_5_17'].sum()
enrol_by_state = enrolment_df.groupby('state')['total_enrolment'].sum()

# --- CRITICAL FIX: ALIGN DATES FOR ALL 3 DATASETS ---
# 1. Get raw daily totals (these have different lengths!)
raw_bio = biometric_df.groupby('date')['total_updates'].sum()
raw_demo = demographic_df.groupby('date')['demo_age_5_17'].sum()
raw_enrol = enrolment_df.groupby('date')['total_enrolment'].sum()

# 2. Create a master list of all unique dates existing in ANY of the 3 datasets
all_dates = raw_bio.index.union(raw_demo.index).union(raw_enrol.index)

# 3. Reindex all series to this master list, filling missing days with 0
# We use these '_aligned' variables for ALL time-series plots to prevent shape errors
daily_bio_aligned = raw_bio.reindex(all_dates, fill_value=0)
daily_demo_aligned = raw_demo.reindex(all_dates, fill_value=0)
daily_enrol_aligned = raw_enrol.reindex(all_dates, fill_value=0)

# Merge state totals for scatter/bar plots (State-wise data doesn't need date alignment)
merged_all = pd.merge(bio_by_state, demo_by_state, left_index=True, right_index=True, how='inner')
merged_all = pd.merge(merged_all, enrol_by_state, left_index=True, right_index=True, how='inner')
merged_all.columns = ['Biometric', 'Demographic', 'Enrolment']

# --- TRILATERAL VISUALIZATIONS ---
print("Generating Trilateral Visualizations...")

# Figure 43: Top 10 States Grouped Bar
plt.figure(figsize=(16, 6))
top_10_all = merged_all.nlargest(10, 'Enrolment')
x = np.arange(len(top_10_all))
width = 0.25
plt.bar(x - width, top_10_all['Biometric'], width, label='Biometric', color='skyblue')
plt.bar(x, top_10_all['Demographic'], width, label='Demographic', color='lightgreen')
plt.bar(x + width, top_10_all['Enrolment'], width, label='Enrolment', color='coral')
plt.title('Trilateral Comparison: Top 10 States', fontsize=14, fontweight='bold')
plt.xticks(x, top_10_all.index, rotation=45)
plt.legend()
plt.tight_layout()
plt.show()

# Figure 44: Daily Trend Comparison (Using Aligned Data)
plt.figure(figsize=(16, 6))
plt.plot(daily_bio_aligned.index, daily_bio_aligned.values, label='Biometric', linewidth=2.5)
plt.plot(daily_demo_aligned.index, daily_demo_aligned.values, label='Demographic', linewidth=2.5)
plt.plot(daily_enrol_aligned.index, daily_enrol_aligned.values, label='Enrolment', linewidth=2.5)
plt.title('Trilateral Comparison: Daily Trends', fontsize=14, fontweight='bold')
plt.legend()
plt.tight_layout()
plt.show()

# Figure 45: Stacked Area (FIXED with Aligned Data)
plt.figure(figsize=(16, 6))
# Only use the _aligned variables here to ensure matching shapes
plt.fill_between(daily_bio_aligned.index, 0, daily_bio_aligned.values, 
                 alpha=0.6, label='Biometric', color='skyblue')

plt.fill_between(daily_demo_aligned.index, daily_bio_aligned.values, 
                 daily_bio_aligned.values + daily_demo_aligned.values, 
                 alpha=0.6, label='Demographic', color='lightgreen')

plt.fill_between(daily_enrol_aligned.index, 
                 daily_bio_aligned.values + daily_demo_aligned.values,
                 daily_bio_aligned.values + daily_demo_aligned.values + daily_enrol_aligned.values, 
                 alpha=0.6, label='Enrolment', color='peachpuff')

plt.title('Trilateral Comparison: Cumulative Daily Trends', fontsize=14, fontweight='bold')
plt.legend()
plt.tight_layout()
plt.show()

# Figure 46: 3D Scatter
fig = plt.figure(figsize=(14, 10))
ax = fig.add_subplot(111, projection='3d')
ax.scatter(merged_all['Biometric'], merged_all['Demographic'], merged_all['Enrolment'],
           c='purple', s=100, alpha=0.7, edgecolors='black')
for state in top_10_all.index:
    ax.text(merged_all.loc[state, 'Biometric'], 
            merged_all.loc[state, 'Demographic'], 
            merged_all.loc[state, 'Enrolment'], state, fontsize=8)
ax.set_xlabel('Biometric')
ax.set_ylabel('Demographic')
ax.set_zlabel('Enrolment')
ax.set_title('Trilateral 3D Scatter: State-wise Comparison', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()

# Figure 47: Radar Chart
fig = plt.figure(figsize=(14, 14))
top_5_states = merged_all.nlargest(5, 'Enrolment')
if not top_5_states.empty:
    top_5_normalized = top_5_states.div(top_5_states.max(axis=0))
    categories = list(top_5_normalized.columns)
    N = len(categories)

    for idx, state in enumerate(top_5_normalized.index):
        ax = fig.add_subplot(3, 2, idx+1, projection='polar')
        values = top_5_normalized.loc[state].values.flatten().tolist()
        values += values[:1]
        angles = [n / float(N) * 2 * pi for n in range(N)]
        angles += angles[:1]
        
        ax.plot(angles, values, 'o-', linewidth=2, label=state)
        ax.fill(angles, values, alpha=0.25)
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(categories)
        ax.set_title(f'State: {state}', fontsize=12, fontweight='bold')

    plt.suptitle('Trilateral Radar Charts: Top 5 States (Normalized)', fontsize=16)
    plt.tight_layout()
    plt.show()
else:
    print("Not enough data for Radar Chart.")

# Figure 48: Heatmap
plt.figure(figsize=(14, 18))
merged_all_norm = merged_all.div(merged_all.max(axis=0))
merged_all_sorted = merged_all_norm.sort_values(by='Enrolment', ascending=False)
sns.heatmap(merged_all_sorted, cmap='YlOrRd', cbar_kws={'label': 'Normalized Value'})
plt.title('Trilateral Heatmap: All States', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()

# Figure 49: Pie Charts (Total Distribution)
fig, axes = plt.subplots(1, 3, figsize=(18, 6))
total_bio = biometric_df['total_updates'].sum()
total_demo = demographic_df['demo_age_5_17'].sum()
total_enrol = enrolment_df['total_enrolment'].sum()

dataset_totals = {'Biometric': total_bio, 'Demographic': total_demo, 'Enrolment': total_enrol}
axes[0].pie(dataset_totals.values(), labels=dataset_totals.keys(), autopct='%1.1f%%', startangle=90)
axes[0].set_title('Overall Distribution')

# Top 10 vs Remaining logic skipped for brevity, showing totals only
plt.suptitle('Trilateral Pie Charts', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()

# Figure 50: Bubble Chart
plt.figure(figsize=(14, 10))
state_records = biometric_df.groupby('state').size() + demographic_df.groupby('state').size() + enrolment_df.groupby('state').size()
merged_with_size = pd.merge(merged_all, state_records.rename('records'), left_index=True, right_index=True)
if not merged_with_size.empty:
    bubble_sizes = merged_with_size['records'] / merged_with_size['records'].max() * 1000

    scatter = plt.scatter(merged_with_size['Biometric'], merged_with_size['Demographic'], 
                          s=bubble_sizes, c=merged_with_size['Enrolment'], cmap='viridis', alpha=0.6)
    plt.colorbar(scatter, label='Enrolment Count')
    plt.title('Trilateral Bubble Chart (Size = Total Records)', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.show()

# Figure 51: Stacked Bar Top 15
plt.figure(figsize=(16, 8))
top_15_all = merged_all.nlargest(15, 'Enrolment')
top_15_all.plot(kind='bar', stacked=True, color=['skyblue', 'lightgreen', 'coral'], figsize=(16, 8))
plt.title('Trilateral Stacked Bar: Top 15 States', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()

# Figure 52: Normalized Trends (Using Aligned Data)
plt.figure(figsize=(16, 6))
# Normalize using the aligned series
daily_bio_norm = (daily_bio_aligned - daily_bio_aligned.min()) / (daily_bio_aligned.max() - daily_bio_aligned.min())
daily_demo_norm = (daily_demo_aligned - daily_demo_aligned.min()) / (daily_demo_aligned.max() - daily_demo_aligned.min())
daily_enrol_norm = (daily_enrol_aligned - daily_enrol_aligned.min()) / (daily_enrol_aligned.max() - daily_enrol_aligned.min())

plt.plot(daily_bio_norm.index, daily_bio_norm.values, label='Biometric (Norm)')
plt.plot(daily_demo_norm.index, daily_demo_norm.values, label='Demographic (Norm)')
plt.plot(daily_enrol_norm.index, daily_enrol_norm.values, label='Enrolment (Norm)')
plt.title('Trilateral Normalized Trends (0-1 Scale)', fontsize=14, fontweight='bold')
plt.legend()
plt.tight_layout()
plt.show()

print("Trilateral Analysis Complete.")