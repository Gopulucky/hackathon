import pandas as pd
import plotly.graph_objects as go
import glob
import os

# --- 1. ROBUST DATA LOADING ---
print("Initializing Border Radar System...")

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

# Load All Datasets
df_enrol = load_dataset("enrolment_cleaned_part*.csv", "Enrolment")
df_bio = load_dataset("biometric_cleaned_part*.csv", "Biometric")
df_demo = load_dataset("demographic_cleaned_part*.csv", "Demographic")

valid_dfs = [d for d in [df_enrol, df_bio, df_demo] if d is not None]
if not valid_dfs:
    raise FileNotFoundError("Critical Error: No Data Found!")

# --- 2. CONFIGURATION & MAPPINGS ---
METRICS_CONFIG = [
    ('norm_total', 'Total Activity', 'Reds'), # Renamed for generic support
    ('norm_0_5', 'Age 0-5', 'RdPu'),
    ('norm_5_17', 'Age 5-17', 'Blues'),
    ('norm_18_plus', 'Age 18+', 'Oranges')
]

COLUMN_MAPS = {
    'Enrolment': {'total': 'total_enrolment', '0-5': 'age_0_5', '5-17': 'age_5_17', '18+': 'age_18_greater'},
    'Biometric': {'total': 'total_updates', '0-5': None, '5-17': 'bio_age_5_17', '18+': 'bio_age_17_'},
    'Demographic': {'total': 'total_updates', '0-5': None, '5-17': 'demo_age_5_17', '18+': 'demo_age_17_'}
}

STATE_NAME_MAPPING = {
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

# Border Districts Filter
border_districts_list = [
    'Kachchh', 'Kutch', 'Banas Kantha', 'Banaskantha', 'Barmer', 'Jaisalmer', 'Bikaner', 'Ganganagar', 
    'Fazilka', 'Firozpur', 'Ferozepur', 'Tarn Taran', 'Amritsar', 'Gurdaspur', 'Pathankot',
    'Kathua', 'Samba', 'Jammu', 'Rajouri', 'Poonch', 'Baramulla', 'Kupwara', 'Bandipore', 'Kargil', 'Leh', 'Ladakh',
    'Lahaul And Spiti', 'Kinnaur', 'Uttarkashi', 'Chamoli', 'Pithoragarh', 'Champawat', 'Udham Singh Nagar',
    'Pilibhit', 'Lakhimpur Kheri', 'Bahraich', 'Shrawasti', 'Balrampur', 'Siddharthnagar', 'Maharajganj',
    'West Champaran', 'East Champaran', 'Sitamarhi', 'Madhubani', 'Supaul', 'Araria', 'Kishanganj',
    'Darjeeling', 'Jalpaiguri', 'Cooch Behar', 'Alipurduar', 'Uttar Dinajpur', 'Dakshin Dinajpur', 
    'Maldah', 'Murshidabad', 'Nadia', 'North 24 Parganas', 'South 24 Parganas',
    'North Sikkim', 'East Sikkim', 'West Sikkim', 'South Sikkim', 'Sikkim',
    'Dhubri', 'Kokrajhar', 'Chirang', 'Baksa', 'Udalguri', 'Cachar', 'Karimganj', 'Hailakandi',
    'West Garo Hills', 'South Garo Hills', 'East Khasi Hills', 'West Jaintia Hills',
    'West Tripura', 'Khowai', 'Sepahijala', 'South Tripura', 'Dhalai', 'Unakoti', 'North Tripura',
    'Mamit', 'Lunglei', 'Lawngtlai', 'Saiha', 'Champhai', 'Serchhip',
    'Churachandpur', 'Chandel', 'Tengnoupal', 'Kamjong', 'Ukhrul',
    'Mon', 'Tuensang', 'Kiphire', 'Phek', 'Noklak',
    'Tawang', 'West Kameng', 'Upper Subansiri', 'West Siang', 'Upper Siang', 'Anjaw', 'Changlang', 'Tirap', 'Longding'
]
border_list_norm = [x.title() for x in border_districts_list]

state_coords = {
    'Gujarat': (23.25, 71.19), 'Rajasthan': (27.02, 74.21), 'Punjab': (31.14, 75.34),
    'Jammu & Kashmir': (33.77, 76.57), 'Ladakh': (34.15, 77.57), 'Himachal Pradesh': (31.10, 77.17),
    'Uttarakhand': (30.06, 79.01), 'Uttar Pradesh': (27.84, 80.94), 'Bihar': (25.59, 85.31),
    'West Bengal': (24.00, 88.00), 'Sikkim': (27.53, 88.51), 'Assam': (26.20, 92.93),
    'Arunachal Pradesh': (28.21, 94.72), 'Nagaland': (26.15, 94.56), 'Manipur': (24.66, 93.90),
    'Mizoram': (23.16, 92.93), 'Tripura': (23.94, 91.98), 'Meghalaya': (25.46, 91.36)
}

# --- 3. PROCESSING & CACHING ---
DATA_CACHE = {}
ALL_BORDER_STATES = sorted(list(state_coords.keys()))

for df_raw in valid_dfs:
    dtype = df_raw['Dataset_Type'].iloc[0]
    mapping = COLUMN_MAPS[dtype]
    DATA_CACHE[dtype] = {}
    
    # Pre-process
    df = df_raw.copy()
    df = df[df['state'] != '100000']
    
    # Normalize Columns
    if dtype == 'Enrolment':
        df['norm_total'] = df['age_0_5'] + df['age_5_17'] + df['age_18_greater']
    else:
        total_col = mapping['total']
        df['norm_total'] = df[total_col] if total_col in df.columns else (df[mapping['5-17']] + df[mapping['18+']])
        
    df['norm_5_17'] = df[mapping['5-17']]
    df['norm_18_plus'] = df[mapping['18+']]
    df['norm_0_5'] = df[mapping['0-5']] if mapping['0-5'] else 0
    
    df['state'] = df['state'].replace(STATE_NAME_MAPPING)
    
    # Filter for Border
    df['district_norm'] = df['district'].astype(str).str.title()
    border_df = df[df['district_norm'].isin(border_list_norm)].copy()
    
    # Calculate Metrics per State
    for m_col, label, _ in METRICS_CONFIG:
        # Aggregation
        node_agg = border_df.groupby('state')[m_col].sum().reset_index()
        # Align to ALL_BORDER_STATES
        node_agg = node_agg.set_index('state').reindex(ALL_BORDER_STATES, fill_value=0).reset_index()
        
        # Hotspots
        hotspot_agg = border_df.groupby(['state', 'district', 'pincode'])[m_col].sum().reset_index()
        # Get top hotspot per state
        if not hotspot_agg.empty:
            idx = hotspot_agg.groupby('state')[m_col].idxmax()
            top_hotspots = hotspot_agg.loc[idx].set_index('state')
        else:
            top_hotspots = pd.DataFrame()
            
        # Build Arrays for Plotly
        lat_arr = []
        lon_arr = []
        size_arr = []
        color_arr = []
        customdata_arr = []
        text_arr = []
        
        max_val = node_agg[m_col].max()
        if max_val == 0: max_val = 1
        
        for state in ALL_BORDER_STATES:
            # Coords
            lat, lon = state_coords.get(state, (None, None))
            if lat is None: continue
            
            val = node_agg.loc[node_agg['state'] == state, m_col].values[0]
            
            # Hotspot Data
            if state in top_hotspots.index:
                hs = top_hotspots.loc[state]
                hs_pincode = hs['pincode']
                hs_dist = hs['district']
                hs_val = hs[m_col]
            else:
                hs_pincode, hs_dist, hs_val = "N/A", "N/A", 0
                
            lat_arr.append(lat)
            lon_arr.append(lon)
            color_arr.append(val)
            size_arr.append((val / max_val * 50) + 15)
            text_arr.append(state)
            customdata_arr.append([hs_pincode, hs_dist, hs_val, val])
            
        DATA_CACHE[dtype][m_col] = {
            'lat': lat_arr, 'lon': lon_arr, 
            'size': size_arr, 'color': color_arr,
            'customdata': customdata_arr, 'text': text_arr
        }

# --- 4. VISUALIZATION ---
fig = go.Figure()

# A. Background Map
geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
fig.add_trace(go.Choropleth(
    geojson=geojson_url,
    featureidkey='properties.ST_NM',
    locations=list(state_coords.keys()),
    z=[0] * len(state_coords),
    colorscale=[[0, 'rgba(0,0,0,0)'], [1, 'rgba(0,0,0,0)']],
    marker_line_color='#9ca3af',
    marker_line_width=1,
    showscale=False,
    hoverinfo='skip',
    name='Border Context'
))

# B. Initial Traces (First Dataset)
init_dataset = list(DATA_CACHE.keys())[0] # usually Enrolment

for i, (m_col, label, color_scale) in enumerate(METRICS_CONFIG):
    data = DATA_CACHE[init_dataset][m_col]
    
    fig.add_trace(go.Scattergeo(
        locationmode='country names',
        lon=data['lon'],
        lat=data['lat'],
        text=data['text'],
        mode='markers+text',
        marker=dict(
            size=data['size'],
            color=data['color'],
            colorscale=color_scale,
            reversescale=False,
            opacity=0.9,
            line=dict(width=1, color='white'),
            symbol='circle',
            colorbar=dict(
                title=dict(text=label, font=dict(size=12)),
                x=0.95, len=0.5, thickness=15,
                bgcolor='rgba(255,255,255,0.8)'
            )
        ),
        textposition="bottom center",
        textfont=dict(color="#1a1a2e", size=11, family="Arial Black"),
        customdata=data['customdata'],
        hovertemplate=(
            '<b>%{text}</b><br>' +
            f'{label}: ' + '%{customdata[3]:,.0f}<br>' +
            '<br><b>📍 Hotspot Zone:</b><br>' +
            'District: %{customdata[1]}<br>' +
            'Pincode: %{customdata[0]}<br>' +
            'Volume: %{customdata[2]:,.0f}' +
            '<extra></extra>'
        ),
        visible=(i == 0),
        name=label
    ))

# --- 5. DROPDOWNS ---
# Dataset Dropdown
dataset_buttons = []
for ds_name in DATA_CACHE.keys():
    # Construct update args: update 'marker.size', 'marker.color', 'customdata' for ALL 4 metrics
    # We update all 4 traces even though only 1 is visible, so that when metric is toggled, it's correct
    
    args_lat = []
    args_lon = []
    args_size = []
    args_color = []
    args_custom = []
    
    # Loop through metrics to build the update arrays
    for m_col, _, _ in METRICS_CONFIG:
        d = DATA_CACHE[ds_name][m_col]
        args_lat.append(d['lat'])
        args_lon.append(d['lon'])
        args_size.append(d['size'])
        args_color.append(d['color'])
        args_custom.append(d['customdata'])
        
    dataset_buttons.append(dict(
        label=ds_name,
        method="update",
        args=[
            {
                'lat': [None] + args_lat, # None for background trace
                'lon': [None] + args_lon,
                'marker.size': [None] + args_size,
                'marker.color': [None] + args_color,
                'customdata': [None] + args_custom
            },
            {"title": f"<b>Border Radar: {ds_name}</b>"}
        ]
    ))

# Metric Dropdown
metric_buttons = []
for i, (_, label, _) in enumerate(METRICS_CONFIG):
    # Visibility: Background (True) + this metric (True) + others (False)
    vis = [False] * len(fig.data)
    vis[0] = True # Background
    vis[i+1] = True # The Metric Trace
    
    metric_buttons.append(dict(
        label=label,
        method="update",
        args=[{"visible": vis}]
    ))

# --- 6. LAYOUT ---
fig.update_layout(
    autosize=True,
    title=dict(
        text=f"<b>Border Radar: {init_dataset}</b>",
        x=0.02, y=0.98,
        xanchor='left', yanchor='top',
        font=dict(size=20, color='#1a1a2e', family='Arial Black')
    ),
    paper_bgcolor='#ffffff',
    plot_bgcolor='#ffffff',
    geo=dict(
        fitbounds="locations",
        visible=True,
        showframe=False,
        bgcolor='rgba(0,0,0,0)',
        showland=True,
        landcolor='#f4f6f8',
        showocean=False,
        showcountries=False,
        domain=dict(x=[0.0, 1.0], y=[0.0, 1.0])
    ),
    margin=dict(l=0, r=0, t=60, b=0),
    updatemenus=[
        # Dataset Dropdown
        dict(
            active=0,
            buttons=dataset_buttons,
            x=0.65, y=0.98,
            xanchor='left', yanchor='top',
            bgcolor='#ffffff', bordercolor='#e0e0e0', borderwidth=1,
            pad=dict(r=10, t=10)
        ),
        # Metric Dropdown
        dict(
            active=0,
            buttons=metric_buttons,
            x=0.85, y=0.98,
            xanchor='left', yanchor='top',
            bgcolor='#ffffff', bordercolor='#e0e0e0', borderwidth=1,
            pad=dict(r=10, t=10)
        )
    ]
)

# Footer
fig.add_annotation(
    text="<i>Border Intensity Clusters</i>",
    x=0.98, y=0.02,
    showarrow=False,
    xref="paper", yref="paper", 
    xanchor="right",
    font=dict(size=10, color="#9ca3af", family="Arial")
)

# Exports
output_path = os.path.join(SCRIPT_DIR, "border_radar_visualization.png")
html_path = os.path.join(SCRIPT_DIR, "border_radar_visualization.html")

try:
    fig.write_image(output_path, scale=2, width=1600, height=1200)
    print(f"[OK] Image saved: {output_path}")
except Exception:
    pass

fig.write_html(html_path, config={'responsive': True, 'displayModeBar': False})
print(f"[OK] Interactive HTML saved: {html_path}")

fig.show(config={'responsive': True})