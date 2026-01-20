import pandas as pd
import plotly.graph_objects as go
import os
import glob

# --- 1. ROBUST DATA LOADING ---
print("Initializing Spatiotemporal Analytics Engine...")

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "cleaned_data")

def load_dataset(pattern, name):
    full_pattern = os.path.join(DATA_DIR, pattern)
    files = sorted(glob.glob(full_pattern, recursive=True))
    if not files:
        print(f"Warning: No files found for {name}")
        return None
    print(f"  - Loading {name} ({len(files)} files)...")
    dfs = [pd.read_csv(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    df['Dataset_Type'] = name
    return df

# Load all three datasets
# The glob pattern *_cleaned_part*.csv matches ALL part files (part1, part2, etc.)
# The load_dataset function automatically concatenates them
df_enrol = load_dataset("enrolment_cleaned_part*.csv", "Enrolment")
df_bio = load_dataset("biometric_cleaned_part*.csv", "Biometric")
df_demo = load_dataset("demographic_cleaned_part*.csv", "Demographic")

valid_dfs = [d for d in [df_enrol, df_bio, df_demo] if d is not None]
if not valid_dfs:
    raise FileNotFoundError("No data files found!")

# --- 2. DATA NORMALIZATION & INSIGHT CALCULATION ---
print("Calculating Hotspots and Temporal Peaks...")

# Define standard metrics we want to analyze
# Format: (InternalKey, DisplayName, ColorScale)
METRICS_CONFIG = [
    ('norm_total', 'Total Activity', 'Viridis'),
    ('norm_0_5', 'Age 0-5', 'RdPu'),
    ('norm_5_17', 'Age 5-17', 'Blues'),
    ('norm_18_plus', 'Age 18+', 'Oranges')
]

# Column Mappings for each file type
COLUMN_MAPS = {
    'Enrolment': {'total': 'total_enrolment', '0-5': 'age_0_5', '5-17': 'age_5_17', '18+': 'age_18_greater'},
    'Biometric': {'total': 'total_updates', '0-5': None, '5-17': 'bio_age_5_17', '18+': 'bio_age_17_'},
    'Demographic': {'total': 'total_updates', '0-5': None, '5-17': 'demo_age_5_17', '18+': 'demo_age_17_'}
}

STATE_NAME_MAPPING = {
    # Data file names -> GeoJSON feature keys (jbrobst map)
    'Andaman and Nicobar Islands': 'Andaman & Nicobar',
    'Dadra and Nagar Haveli and Daman and Diu': 'Dadra and Nagar Haveli and Daman and Diu',
    'Jammu and Kashmir': 'Jammu & Kashmir',
    'Odisha': 'Odisha',
    'Uttarakhand': 'Uttarakhand',
    'Ladakh': 'Ladakh',
    'Delhi': 'Delhi',
    'Telangana': 'Telangana',
    'INVALID': 'INVALID_EXCLUDE'
}

# Dictionary to store pre-calculated arrays for the plot
# Structure: DATA_CACHE[DatasetName][MetricName] = { 'z': [], 'customdata': [] }
DATA_CACHE = {}
ALL_STATES = []

# --- PROCESSING LOOP ---
for df_raw in valid_dfs:
    dtype = df_raw['Dataset_Type'].iloc[0]
    mapping = COLUMN_MAPS[dtype]
    DATA_CACHE[dtype] = {}
    
    # A. Pre-processing
    df = df_raw.copy()
    df = df[df['state'] != '100000']
    
    # Normalize Columns
    # 1. Total
    if dtype == 'Enrolment':
        df['norm_total'] = df['age_0_5'] + df['age_5_17'] + df['age_18_greater']
    else:
        # Calculate total if not present, else use map
        total_col = mapping['total']
        if total_col not in df.columns:
            df['norm_total'] = df[mapping['5-17']] + df[mapping['18+']]
        else:
            df['norm_total'] = df[total_col]

    # 2. Age Groups
    df['norm_5_17'] = df[mapping['5-17']]
    df['norm_18_plus'] = df[mapping['18+']]
    df['norm_0_5'] = df[mapping['0-5']] if mapping['0-5'] else 0

    # 3. State & Date
    df['state_mapped'] = df['state'].replace(STATE_NAME_MAPPING)
    df['date'] = pd.to_datetime(df['date'])
    
    # Initialize States list (from first dataset)
    if not ALL_STATES:
        # Get unique states from the mapped data
        ALL_STATES = sorted(df['state_mapped'].unique())

    # --- INSIGHT ALGORITHMS ---
    
    # Algorithm 1: State Totals (The 'Z' Value)
    state_agg = df.groupby('state_mapped')[
        ['norm_total', 'norm_0_5', 'norm_5_17', 'norm_18_plus']
    ].sum().reset_index()
    # Align to ALL_STATES to ensure index match
    state_agg = state_agg.set_index('state_mapped').reindex(ALL_STATES, fill_value=0).reset_index()

    # Algorithm 2: Temporal Peaks (Busiest Date)
    # We calculate the peak date for the TOTAL metric and apply it to others for simplicity.
    peak_data_map = {}
    for m_col, _, _ in METRICS_CONFIG:
        # Group by State+Date, sum, find max index
        daily = df.groupby(['state_mapped', 'date'])[m_col].sum().reset_index()
        idx = daily.groupby('state_mapped')[m_col].idxmax()
        peaks = daily.loc[idx, ['state_mapped', 'date']]
        peaks['date_str'] = peaks['date'].dt.strftime('%Y-%m-%d')
        peak_data_map[m_col] = peaks.set_index('state_mapped')['date_str'].to_dict()

    # Algorithm 3: Hyper-Local Hotspots (Busiest Pincode)
    hotspot_data_map = {}
    for m_col, _, _ in METRICS_CONFIG:
        pin_agg = df.groupby(['state_mapped', 'district', 'pincode'])[m_col].sum().reset_index()
        idx = pin_agg.groupby('state_mapped')[m_col].idxmax()
        hotspots = pin_agg.loc[idx]
        hotspot_data_map[m_col] = hotspots.set_index('state_mapped')[['pincode', 'district', m_col]].to_dict('index')

    # --- STORE RESULTS ---
    for m_col, _, _ in METRICS_CONFIG:
        z_values = state_agg[m_col].tolist()
        
        # Build Custom Data: [Pincode, District, HotspotVal, PeakDate]
        custom_data = []
        for state in ALL_STATES:
            # Hotspots
            hs = hotspot_data_map[m_col].get(state, {'pincode': 'N/A', 'district': 'N/A', m_col: 0})
            # Peaks
            pk = peak_data_map[m_col].get(state, 'N/A')
            
            custom_data.append([
                hs['pincode'],
                hs['district'],
                hs[m_col],
                pk
            ])
            
        DATA_CACHE[dtype][m_col] = {
            'z': z_values,
            'customdata': custom_data
        }

# --- 3. MAP CONFIGURATION ---
# Reverted to Original Map (jbrobst) as requested
geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"

fig = go.Figure()

# Initial Data (First Dataset, All Metrics)
init_dataset = list(DATA_CACHE.keys())[0] # Usually Enrolment

# Create 4 Traces (One for each metric). 
# We update these traces when Dataset changes.
for i, (m_col, label, color_scale) in enumerate(METRICS_CONFIG):
    data = DATA_CACHE[init_dataset][m_col]
    
    fig.add_trace(go.Choropleth(
        geojson=geojson_url,
        featureidkey='properties.ST_NM', # Original key for jbrobst
        locations=ALL_STATES,
        z=data['z'],
        customdata=data['customdata'],
        colorscale=color_scale,
        colorbar_title=label,
        name=label,
        visible=(i == 0), # Only first metric visible initially
        hovertemplate=(
            '<b>%{location}</b><br>' +
            f'{label}: ' + '%{z:,.0f}<br>' +
            '<br><b>📅 Peak Date:</b> %{customdata[3]}<br>' +
            '<b>🔥 Hotspot:</b> %{customdata[1]} (%{customdata[0]})<br>' +
            '<b>💥 Max Vol:</b> %{customdata[2]:,.0f}' +
            '<extra></extra>'
        )
    ))

# Add Labels
state_coords = {
    'Andhra Pradesh': (15.91, 79.74), 'Arunachal Pradesh': (28.21, 94.72), 'Assam': (26.20, 92.93),
    'Bihar': (25.09, 85.31), 'Chhattisgarh': (21.27, 81.86), 'Goa': (15.29, 74.12),
    'Gujarat': (22.25, 71.19), 'Haryana': (29.05, 76.08), 'Himachal Pradesh': (31.10, 77.17),
    'Jharkhand': (23.61, 85.27), 'Karnataka': (15.31, 75.71), 'Kerala': (10.85, 76.27),
    'Madhya Pradesh': (22.97, 78.65), 'Maharashtra': (19.75, 75.71), 'Manipur': (24.66, 93.90),
    'Meghalaya': (25.46, 91.36), 'Mizoram': (23.16, 92.93), 'Nagaland': (26.15, 94.56),
    'Odisha': (20.95, 85.09), 'Punjab': (31.14, 75.34), 'Rajasthan': (27.02, 74.21),
    'Sikkim': (27.53, 88.51), 'Tamil Nadu': (11.12, 78.65), 'Telangana': (18.11, 79.01),
    'Tripura': (23.94, 91.98), 'Uttar Pradesh': (26.84, 80.94), 'Uttarakhand': (30.06, 79.01),
    'West Bengal': (22.98, 87.85), 'Delhi': (28.70, 77.10), 'Jammu & Kashmir': (33.77, 76.57),
    'Ladakh': (34.15, 77.57), 'Puducherry': (11.94, 79.80), 'Andaman & Nicobar': (11.74, 92.65),
    'Chandigarh': (30.73, 76.77), 'Dadra and Nagar Haveli': (20.18, 73.01),
    'Lakshadweep': (10.56, 72.64)
}
lats = [state_coords.get(s, (None, None))[0] for s in ALL_STATES]
lons = [state_coords.get(s, (None, None))[1] for s in ALL_STATES]

fig.add_trace(go.Scattergeo(
    locationmode='country names',
    lon=lons, lat=lats,
    text=ALL_STATES, mode='text',
    name='Labels', 
    textfont=dict(
        size=11,           # Larger font for PDF clarity
        color='#1a1a2e',   # Dark color for visibility
        family='Arial',
        weight='bold'      # Bold for clarity
    ),
    visible=True, 
    hoverinfo='skip'
))

# --- 4. DROPDOWN LOGIC ---

# Dropdown A: Dataset Selector
# This updates the Z (values) and CustomData (Insights) for ALL 4 metric traces
dataset_buttons = []
for dtype in list(DATA_CACHE.keys()):
    # Collect new data for all 4 metrics
    new_z = [DATA_CACHE[dtype][m[0]]['z'] for m in METRICS_CONFIG]
    new_custom = [DATA_CACHE[dtype][m[0]]['customdata'] for m in METRICS_CONFIG]
    
    # We must also preserve the Label trace (which is the 5th trace, index 4)
    # The 'update' method accepts arrays for properties. 
    # Providing 4 items updates traces 0,1,2,3. Trace 4 (labels) remains untouched.
    dataset_buttons.append(dict(
        label=dtype,
        method="update",
        args=[
            {'z': new_z, 'customdata': new_custom}, # Update Data
            {"title": f"{dtype}: Spatiotemporal Analysis"}, # Update Title
            [0, 1, 2, 3] # Apply to the first 4 traces only
        ]
    ))

# Dropdown B: Metric Selector
# This toggles visibility.
metric_buttons = []
for i, (_, label, _) in enumerate(METRICS_CONFIG):
    # Visible: [Metric1, Metric2, Metric3, Metric4, Labels]
    vis = [False] * 4
    vis[i] = True
    vis.append(True) # Keep Labels
    
    metric_buttons.append(dict(
        label=label,
        method="update",
        args=[{"visible": vis}]
    ))

# --- 5. FINAL FLUID LAYOUT (RESPONSIVE) ---
fig.update_layout(
    autosize=True, # Enable responsive sizing
    # Do NOT set fixed width/height for the interactive view
    
    title=dict(
        text=f"<b>🇮🇳 {init_dataset}: Spatiotemporal Analysis</b>",
        x=0.02, # Left aligned for dashboard look
        y=0.98,
        xanchor='left',
        yanchor='top',
        font=dict(size=20, color='#1a1a2e', family='Arial Black')
    ),
    
    # Clean background
    paper_bgcolor='#ffffff',
    plot_bgcolor='#ffffff',
    
    # Map container - Fills the available space
    geo=dict(
        fitbounds="locations",
        showframe=False, # Removed border for cleaner "floating" look
        bgcolor='rgba(0,0,0,0)', # Transparent
        
        showland=True,
        landcolor='#f4f6f8',
        showocean=False,
        
        showcoastlines=False,
        showcountries=False,
        visible=True,
        
        # Use entire container (leave space at top for controls via margins)
        domain=dict(x=[0.0, 1.0], y=[0.0, 1.0])
    ),
    
    # Tight margins to prevent scrolling (only top needs space for title/controls)
    margin=dict(l=0, r=0, t=60, b=0),
    
    # Controls - Compact and positioned top-right
    updatemenus=[
        # Dataset Dropdown
        dict(
            active=0,
            buttons=dataset_buttons,
            x=0.65, y=0.98,
            xanchor='left', yanchor='top',
            bgcolor='#ffffff', 
            bordercolor='#e0e0e0',
            borderwidth=1,
            pad=dict(r=10, t=10),
            font=dict(size=12, color='#1a1a2e', family='Arial')
        ),
        # Metric Dropdown
        dict(
            active=0,
            buttons=metric_buttons,
            x=0.85, y=0.98,
            xanchor='left', yanchor='top',
            bgcolor='#ffffff',
            bordercolor='#e0e0e0',
            borderwidth=1,
            pad=dict(r=10, t=10),
            font=dict(size=12, color='#1a1a2e', family='Arial')
        )
    ],
    
    # Colorbar - Floating on right
    coloraxis_colorbar=dict(
        title=dict(font=dict(size=12)),
        tickfont=dict(size=10),
        len=0.5,
        thickness=15,
        x=0.98,
        xanchor='right',
        y=0.5,
        yanchor='middle',
        bgcolor='rgba(255,255,255,0.8)'
    )
)

# Clean Annotations (removed old ones, new compact label)
fig.data[0].update(name="") # Clear weird trace names if any

# Footer annotation - Floating bottom right
fig.add_annotation(
    text="<i>Aadhaar Spatiotemporal Analytics</i>",
    x=0.98, y=0.02,
    showarrow=False,
    xref="paper", yref="paper",
    xanchor='right',
    font=dict(size=10, color='#9ca3af', family='Arial')
)

# Save as high-quality image for PDF (Fixed resolution for Print)
output_path = os.path.join(SCRIPT_DIR, "india_map_visualization.png")
try:
    fig.write_image(output_path, scale=2, width=1600, height=1200)
    print(f"\n[OK] Image saved for PDF: {output_path}")
except Exception as e:
    print(f"\n[INFO] Image export skipped (install kaleido: pip install kaleido)")

# Save as interactive HTML with responsive config
html_path = os.path.join(SCRIPT_DIR, "india_map_visualization.html")
fig.write_html(
    html_path, 
    config={'responsive': True, 'displayModeBar': False} # Essential for no-scroll responsiveness
)
print(f"[OK] Interactive HTML saved: {html_path}")

fig.show(config={'responsive': True})