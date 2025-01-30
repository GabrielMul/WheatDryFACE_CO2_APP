import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import gdown
import os

# Function to download files from Google Drive
def download_file_from_drive(drive_url, output_path):
    gdown.download(drive_url, output_path, quiet=False)

# Function to load and preprocess data for each ring
def load_and_process_ring_data(ring_name, historical_paths, recent_path):
    all_dfs = []

    # Load historical datasets
    for path in historical_paths:
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            print(f"⚠️ Error: File {path} is empty or missing!")
            continue

        df = pd.read_csv(path, low_memory=False, skiprows=1)
        # Identify the timestamp column
        timestamp_col = next((col for col in df.columns if "timestamp" in col.lower()), None)
        if timestamp_col is None:
            print(f"⚠️ Error: No timestamp column found in {path}")
            continue

        df = df[df[timestamp_col].str.match(r'\d{4}-\d{2}-\d{2}.*', na=False)]
        df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')
        df.rename(columns={timestamp_col: "TIMESTAMP"}, inplace=True)

        df = df[['TIMESTAMP', 'CO2_Avg']].dropna(subset=['CO2_Avg'])
        df['Rings'] = ring_name
        df['CO2'] = "eCO2" if "eCO2" in ring_name else "aCO2"
        df['CO2_Avg'] = pd.to_numeric(df['CO2_Avg'], errors='coerce')

        all_dfs.append(df)

    # Load the most recent dataset
    if not os.path.exists(recent_path) or os.path.getsize(recent_path) == 0:
        print(f"⚠️ Error: Recent file {recent_path} is empty or missing!")
        return None

    df_recent = pd.read_csv(recent_path, low_memory=False, skiprows=1)
    timestamp_col = next((col for col in df_recent.columns if "timestamp" in col.lower()), None)
    if timestamp_col is None:
        print(f"⚠️ Error: No timestamp column found in {recent_path}")
        return None

    df_recent = df_recent[df_recent[timestamp_col].str.match(r'\d{4}-\d{2}-\d{2}.*', na=False)]
    df_recent[timestamp_col] = pd.to_datetime(df_recent[timestamp_col], errors='coerce')
    df_recent.rename(columns={timestamp_col: "TIMESTAMP"}, inplace=True)

    df_recent = df_recent[['TIMESTAMP', 'CO2_Avg']].dropna(subset=['CO2_Avg'])
    df_recent['Rings'] = ring_name
    df_recent['CO2'] = "eCO2" if "eCO2" in ring_name else "aCO2"
    df_recent['CO2_Avg'] = pd.to_numeric(df_recent['CO2_Avg'], errors='coerce')

    all_dfs.append(df_recent)
    # Drop duplicates by TIMESTAMP
    return pd.concat(all_dfs, ignore_index=True).drop_duplicates(subset=['TIMESTAMP'], keep='first')

# 1. Get Google Drive links from your Streamlit secrets
drive_links = {}
for ring_num in range(1, 7):
    ring_key = f"Ring_{ring_num}"
    drive_links[ring_key] = {
        "historical": st.secrets["drive_links"][ring_key]["historical"],
        "recent": st.secrets["drive_links"][ring_key]["recent"]
    }

# 2. Download and merge all ring data
all_rings_data = []
for ring, files in drive_links.items():
    historical_files = []
    for i, url in enumerate(files['historical']):
        file_name = f"{ring}_historical_{i}.csv"
        download_file_from_drive(url, file_name)
        historical_files.append(file_name)
    
    recent_file = f"{ring}_recent.csv"
    download_file_from_drive(files['recent'], recent_file)
    
    ring_data = load_and_process_ring_data(ring, historical_files, recent_file)
    if ring_data is not None:
        all_rings_data.append(ring_data)

df = pd.concat(all_rings_data, ignore_index=True)

# ====================
# Streamlit UI
# ====================
st.set_page_config(page_title="CO₂ Monitoring Dashboard", layout="wide")
st.title("🌍 CO₂ Monitoring Dashboard")

# 3. Sidebar Filters
st.sidebar.header("Filter Data")
selected_rings = st.sidebar.multiselect("Select Rings:", df['Rings'].unique(), default=df['Rings'].unique())
co2_type = st.sidebar.selectbox("Select CO₂ Type:", df['CO2'].unique(), index=0)
date_range = st.sidebar.date_input(
    "Select Date Range:",
    [df['TIMESTAMP'].min().date(), df['TIMESTAMP'].max().date()]
)

# Filter by ring, CO2 type, and date
df_filtered = df[
    (df['Rings'].isin(selected_rings)) &
    (df['CO2'] == co2_type)
]
df_filtered = df_filtered[
    (df_filtered['TIMESTAMP'].dt.date >= date_range[0]) &
    (df_filtered['TIMESTAMP'].dt.date <= date_range[-1])
]

# 4. Plot - Raw Data
fig_raw = px.line(
    df_filtered,
    x="TIMESTAMP",
    y="CO2_Avg",
    color="Rings",
    title="CO₂ Concentration Over Time",
    labels={"CO2_Avg": "CO₂ Average", "TIMESTAMP": "Time"}
)
st.plotly_chart(fig_raw, use_container_width=True)

# 5. Moving Average Calculation & Plot
rolling_window = st.sidebar.slider("Select Rolling Window (5-min intervals):", min_value=1, max_value=60, value=12)
# Ensure data is sorted by time before rolling:
df_filtered = df_filtered.sort_values("TIMESTAMP")

df_filtered["CO2_Avg_MA"] = df_filtered.groupby("Rings")["CO2_Avg"]\
    .transform(lambda x: x.rolling(rolling_window).mean())

fig_ma = px.line(
    df_filtered,
    x="TIMESTAMP",
    y="CO2_Avg_MA",
    color="Rings",
    title="Smoothed CO₂ Average with Moving Average",
    labels={"CO2_Avg_MA": "CO₂ Moving Avg", "TIMESTAMP": "Time"}
)
st.plotly_chart(fig_ma, use_container_width=True)

# 6. Compute Statistics
st.subheader("📊 CO₂ Statistics")
st.write("**5-Minute Interval Stats**")
df_stats = df_filtered.groupby("Rings")["CO2_Avg"].agg(["mean", "std"]).reset_index()
st.dataframe(df_stats)

# 7. (Optional) Auto-refresh
st.experimental_rerun()
