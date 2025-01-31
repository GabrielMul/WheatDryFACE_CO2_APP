import streamlit as st
import pandas as pd
import plotly.express as px
import datetime
import gdown
import os
import sys

# =====================================================
# 1. PAGE CONFIG MUST BE FIRST STREAMLIT COMMAND
# =====================================================
st.set_page_config(page_title="CO₂ Monitoring Dashboard", layout="wide")

# Optional: Display Python & Streamlit version for debugging
st.write("Python version:", sys.version)
st.write("Streamlit version:", st.__version__)

# =========================
# 2. Initialize Session State
# =========================
# We'll toggle this to force a rerun instead of using st.experimental_rerun()
if "force_rerun" not in st.session_state:
    st.session_state["force_rerun"] = False

# =========================
# 3. Helper Functions
# =========================
def get_co2_type(ring_name):
    """Assign 'aCO2' or 'eCO2' based on ring name."""
    aCO2_rings = ["Ring_1", "Ring_3", "Ring_6"]
    eCO2_rings = ["Ring_2", "Ring_4", "Ring_5"]
    return "aCO2" if ring_name in aCO2_rings else "eCO2"

def load_and_process_ring_data(ring_name, historical_paths, recent_path):
    """
    Reads multiple CSVs for a given ring (Ring_1,...),
    processes them, and returns a combined DataFrame.
    """
    all_dfs = []

    # Load historical files
    for path in historical_paths:
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            print(f"⚠️ Error: File {path} is empty or missing!")
            continue

        df = pd.read_csv(path, low_memory=False, skiprows=1)
        # Identify timestamp column
        timestamp_col = next((col for col in df.columns if "timestamp" in col.lower()), None)
        if not timestamp_col:
            print(f"⚠️ Error: No timestamp column found in {path}")
            continue

        # Clean timestamps
        df = df[df[timestamp_col].str.match(r'\d{4}-\d{2}-\d{2}.*', na=False)]
        df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')
        df.rename(columns={timestamp_col: "TIMESTAMP"}, inplace=True)

        # Keep only needed columns
        df = df[['TIMESTAMP', 'CO2_Avg']].dropna(subset=['CO2_Avg'])
        df['Rings'] = ring_name
        df['CO2'] = get_co2_type(ring_name)
        df['CO2_Avg'] = pd.to_numeric(df['CO2_Avg'], errors='coerce')

        all_dfs.append(df)

    # Load most recent file
    if not os.path.exists(recent_path) or os.path.getsize(recent_path) == 0:
        print(f"⚠️ Error: Recent file {recent_path} is empty or missing!")
        return None

    df_recent = pd.read_csv(recent_path, low_memory=False, skiprows=1)
    timestamp_col = next((col for col in df_recent.columns if "timestamp" in col.lower()), None)
    if not timestamp_col:
        print(f"⚠️ Error: No timestamp column found in {recent_path}")
        return None

    df_recent = df_recent[df_recent[timestamp_col].str.match(r'\d{4}-\d{2}-\d{2}.*', na=False)]
    df_recent[timestamp_col] = pd.to_datetime(df_recent[timestamp_col], errors='coerce')
    df_recent.rename(columns={timestamp_col: "TIMESTAMP"}, inplace=True)

    df_recent = df_recent[['TIMESTAMP', 'CO2_Avg']].dropna(subset=['CO2_Avg'])
    df_recent['Rings'] = ring_name
    df_recent['CO2'] = get_co2_type(ring_name)
    df_recent['CO2_Avg'] = pd.to_numeric(df_recent['CO2_Avg'], errors='coerce')

    all_dfs.append(df_recent)

    # Combine & drop duplicates
    combined_df = pd.concat(all_dfs, ignore_index=True).drop_duplicates(subset='TIMESTAMP', keep='first')
    return combined_df

# =========================
# 4. Cache the Data Download
# =========================
@st.cache_data
def download_and_load_all_data(drive_links):
    """
    Downloads all historical & recent CSVs for each ring from Google Drive,
    loads them, merges into a single DataFrame.
    """
    all_rings_data = []
    for ring, files in drive_links.items():
        historical_files = []
        for i, url in enumerate(files['historical']):
            file_name = f"{ring}_historical_{i}.csv"
            gdown.download(url, file_name, quiet=False)
            historical_files.append(file_name)

        recent_file = f"{ring}_recent.csv"
        gdown.download(files['recent'], recent_file, quiet=False)

        ring_data = load_and_process_ring_data(ring, historical_files, recent_file)
        if ring_data is not None:
            all_rings_data.append(ring_data)

    # Combine all ring data
    df_combined = pd.concat(all_rings_data, ignore_index=True)
    return df_combined

# =========================
# 5. Main Application
# =========================
def main():
    st.title("🌍 CO₂ Monitoring Dashboard")

    # =============== Sidebar: REFRESH DATA ===============
    st.sidebar.subheader("Data Refresh")
    if st.sidebar.button("Refresh Data"):
        # Clear cache and toggle a session-state var to force a rerun
        st.cache_data.clear()
        st.session_state["force_rerun"] = not st.session_state["force_rerun"]

    # Show the toggled state (for debugging)
    st.write("Force Rerun State:", st.session_state["force_rerun"])

    # =============== Load Data from Cache ===============
    drive_links = {}
    for ring_num in range(1, 7):
        ring_key = f"Ring_{ring_num}"
        drive_links[ring_key] = {
            "historical": st.secrets["drive_links"][ring_key]["historical"],
            "recent": st.secrets["drive_links"][ring_key]["recent"]
        }

    df = download_and_load_all_data(drive_links)
    
    # ============== SIDEBAR FILTERS (for PLOTS) ==============
    st.sidebar.header("Plot Filters")

    # Ring selection
    selected_rings = st.sidebar.multiselect(
        "Select Rings:",
        sorted(df['Rings'].unique()),
        default=sorted(df['Rings'].unique())
    )

    # CO₂ type with 3 options
    co2_type_selection = st.sidebar.selectbox(
        "Select CO₂ Type:",
        ["All", "aCO2", "eCO2"],
        index=0
    )

    # Plot Date Range
    plot_date_range = st.sidebar.date_input(
        "Select Plot Date Range:",
        [df['TIMESTAMP'].min().date(), df['TIMESTAMP'].max().date()]
    )

    # Filter data for plots
    df_plot_filtered = df.copy()
    df_plot_filtered = df_plot_filtered[df_plot_filtered['Rings'].isin(selected_rings)]
    if co2_type_selection != "All":
        df_plot_filtered = df_plot_filtered[df_plot_filtered["CO2"] == co2_type_selection]
    df_plot_filtered = df_plot_filtered[
        (df_plot_filtered['TIMESTAMP'].dt.date >= plot_date_range[0]) &
        (df_plot_filtered['TIMESTAMP'].dt.date <= plot_date_range[-1])
    ]

    # ============== Plot: Raw Data ==============
    fig_raw = px.line(
        df_plot_filtered.sort_values("TIMESTAMP"),
        x="TIMESTAMP",
        y="CO2_Avg",
        color="Rings",
        title="CO₂ Concentration Over Time (Raw)",
        labels={"CO2_Avg": "CO₂ Average", "TIMESTAMP": "Time"}
    )
    st.plotly_chart(fig_raw, use_container_width=True)

    # ============== Plot: Rolling Average ==============
    rolling_window = st.sidebar.slider(
        "Select Rolling Window (5-min intervals):",
        min_value=1, max_value=60, value=12
    )

    df_plot_filtered = df_plot_filtered.sort_values("TIMESTAMP").copy()
    df_plot_filtered["CO2_Avg_MA"] = (
        df_plot_filtered.groupby("Rings")["CO2_Avg"]
        .transform(lambda x: x.rolling(rolling_window).mean())
    )

    fig_ma = px.line(
        df_plot_filtered,
        x="TIMESTAMP",
        y="CO2_Avg_MA",
        color="Rings",
        title="Smoothed CO₂ Average (Moving Average)",
        labels={"CO2_Avg_MA": "CO₂ Moving Avg", "TIMESTAMP": "Time"}
    )
    st.plotly_chart(fig_ma, use_container_width=True)

    # ============== Stats Section (Separate Range) ==============
    # Choose data source for stats: raw or rolling
    st.sidebar.subheader("Choose Data for Statistics")
    stat_source = st.sidebar.radio(
        "Compute Stats From:",
        ["Raw Data", "Rolling Average"],
        index=0
    )

    st.header("📊 CO₂ Statistics")
    st.write("Below, you can choose a **separate** date range + time-of-day window for computing summary statistics:")

    stats_date_range = st.date_input(
        "Select Stats Date Range:",
        [df['TIMESTAMP'].min().date(), df['TIMESTAMP'].max().date()]
    )

    st.subheader("Stats Time-of-Day Range")
    start_time = st.time_input("Start Time (hh:mm)", datetime.time(0, 0))
    end_time = st.time_input("End Time (hh:mm)", datetime.time(23, 59))

    # Filter data for stats
    df_stats_filtered = df.copy()
    df_stats_filtered = df_stats_filtered[df_stats_filtered['Rings'].isin(selected_rings)]
    if co2_type_selection != "All":
        df_stats_filtered = df_stats_filtered[df_stats_filtered["CO2"] == co2_type_selection]
    df_stats_filtered = df_stats_filtered[
        (df_stats_filtered['TIMESTAMP'].dt.date >= stats_date_range[0]) &
        (df_stats_filtered['TIMESTAMP'].dt.date <= stats_date_range[-1])
    ]
    df_stats_filtered = df_stats_filtered[
        (df_stats_filtered['TIMESTAMP'].dt.time >= start_time) &
        (df_stats_filtered['TIMESTAMP'].dt.time <= end_time)
    ]

    # EXCLUDE eCO2 < 350 HERE:
    df_stats_filtered = df_stats_filtered[
        ~((df_stats_filtered["CO2"] == "eCO2") & (df_stats_filtered["CO2_Avg"] < 350))
    ]

    # Apply rolling if "Rolling Average" selected for stats
    if stat_source == "Rolling Average":
        df_stats_filtered = df_stats_filtered.sort_values("TIMESTAMP").copy()
        df_stats_filtered["CO2_Avg_MA"] = (
            df_stats_filtered.groupby("Rings")["CO2_Avg"]
            .transform(lambda x: x.rolling(rolling_window).mean())
        )
        stat_column = "CO2_Avg_MA"
    else:
        stat_column = "CO2_Avg"

    # Compute stats
    if df_stats_filtered.empty or df_stats_filtered[stat_column].isna().all():
        st.warning("No data available in the specified Stats date/time range (or after excluding eCO2 < 350).")
    else:
        df_stats = df_stats_filtered.groupby("Rings")[stat_column].agg(["mean", "std"]).reset_index()
        df_stats.rename(columns={"mean": "Mean CO₂", "std": "Std Dev"}, inplace=True)
        st.write(
            f"**Stats computed for data between** "
            f"`{stats_date_range[0]} - {stats_date_range[-1]}` "
            f"**and** `{start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}`."
        )
        st.write(f"**Data source:** {'Rolling Average' if stat_source == 'Rolling Average' else 'Raw Data'}")
        st.write("**Note:** Values < 350 for eCO₂ have been excluded from stats.")
        st.dataframe(df_stats, use_container_width=True)

def run_app():
    # Single entry point:
    main()

if __name__ == "__main__":
    run_app()
