import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import datetime
import gdown
import os
import sys

# =====================================================
# 1. PAGE CONFIG MUST BE FIRST STREAMLIT COMMAND
# =====================================================
st.set_page_config(page_title="WheatDryFACE Monitoring Dashboard", layout="wide")

# Optional: Display Python & Streamlit version for debugging
st.write("Python version:", sys.version)
st.write("Streamlit version:", st.__version__)

# =========================
# 2. Initialize Session State
# =========================
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
    processes them, and returns a combined DataFrame (CO₂ only).
    """
    all_dfs = []

    # Load historical files
    for path in historical_paths:
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            print(f"⚠️ Error: File {path} is empty or missing!")
            continue

        df = pd.read_csv(path, low_memory=False, skiprows=1)
        timestamp_col = next((col for col in df.columns if "timestamp" in col.lower()), None)
        if not timestamp_col:
            print(f"⚠️ Error: No timestamp column found in {path}")
            continue

        df = df[df[timestamp_col].str.match(r'\d{4}-\d{2}-\d{2}.*', na=False)]
        df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')
        df.rename(columns={timestamp_col: "TIMESTAMP"}, inplace=True)

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


def load_and_process_ring4_temp_rh_data(historical_paths, recent_path):
    """
    Specifically loads T_Air_Avg (°C) and RH_Avg (%) from Ring_4.
    Returns a DataFrame with columns: [TIMESTAMP, T_C, RH].
    """
    all_dfs = []

    for path in historical_paths:
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            print(f"⚠️ Ring4 T/RH: File {path} is empty or missing!")
            continue

        df_trh = pd.read_csv(path, low_memory=False, skiprows=1)
        timestamp_col = next((col for col in df_trh.columns if "timestamp" in col.lower()), None)
        if not timestamp_col:
            print(f"⚠️ Ring4 T/RH: No timestamp column found in {path}")
            continue

        df_trh = df_trh[df_trh[timestamp_col].str.match(r'\d{4}-\d{2}-\d{2}.*', na=False)]
        df_trh[timestamp_col] = pd.to_datetime(df_trh[timestamp_col], errors='coerce')
        df_trh.rename(columns={timestamp_col: "TIMESTAMP"}, inplace=True)

        if "T_Air_Avg" not in df_trh.columns or "RH_Avg" not in df_trh.columns:
            print(f"⚠️ Ring4 T/RH: Missing T_Air_Avg or RH_Avg in {path}")
            continue

        df_trh = df_trh[['TIMESTAMP', 'T_Air_Avg', 'RH_Avg']].dropna(subset=['T_Air_Avg', 'RH_Avg'])
        df_trh['T_C'] = pd.to_numeric(df_trh['T_Air_Avg'], errors='coerce')
        df_trh['RH']  = pd.to_numeric(df_trh['RH_Avg'], errors='coerce')

        df_trh = df_trh[['TIMESTAMP', 'T_C', 'RH']]
        all_dfs.append(df_trh)

    # recent file
    if not os.path.exists(recent_path) or os.path.getsize(path) == 0:
        print(f"⚠️ Ring4 T/RH: Recent file {recent_path} is empty or missing!")
        return None

    df_trh_recent = pd.read_csv(recent_path, low_memory=False, skiprows=1)
    timestamp_col = next((col for col in df_trh_recent.columns if "timestamp" in col.lower()), None)
    if not timestamp_col:
        print(f"⚠️ Ring4 T/RH: No timestamp column found in {recent_path}")
        return None

    df_trh_recent = df_trh_recent[df_trh_recent[timestamp_col].str.match(r'\d{4}-\d{2}-\d{2}.*', na=False)]
    df_trh_recent[timestamp_col] = pd.to_datetime(df_trh_recent[timestamp_col], errors='coerce')
    df_trh_recent.rename(columns={timestamp_col: "TIMESTAMP"}, inplace=True)

    if "T_Air_Avg" in df_trh_recent.columns and "RH_Avg" in df_trh_recent.columns:
        df_trh_recent['T_C'] = pd.to_numeric(df_trh_recent['T_Air_Avg'], errors='coerce')
        df_trh_recent['RH']  = pd.to_numeric(df_trh_recent['RH_Avg'], errors='coerce')
        df_trh_recent = df_trh_recent[['TIMESTAMP', 'T_C', 'RH']].dropna(subset=['T_C', 'RH'])
        all_dfs.append(df_trh_recent)
    else:
        print(f"⚠️ Ring4 T/RH: Missing T_Air_Avg or RH_Avg in {recent_path}")
        return None

    if len(all_dfs) == 0:
        return None

    combined = pd.concat(all_dfs, ignore_index=True).drop_duplicates(subset='TIMESTAMP', keep='first')
    return combined


def load_and_process_ring5_rain_data(historical_paths, recent_path):
    """
    Specifically loads rain data for Ring_5 from the same CSVs.
    Extracts 'Rain_p_Tot' (rainfall in mm).
    Returns a DataFrame with columns: [TIMESTAMP, Rain_mm].
    """
    all_rain_dfs = []

    for path in historical_paths:
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            print(f"⚠️ Ring5 Rain: File {path} is empty or missing!")
            continue

        df_rain = pd.read_csv(path, low_memory=False, skiprows=1)
        timestamp_col = next((col for col in df_rain.columns if "timestamp" in col.lower()), None)
        if not timestamp_col:
            print(f"⚠️ Ring5 Rain: No timestamp column found in {path}")
            continue

        df_rain = df_rain[df_rain[timestamp_col].str.match(r'\d{4}-\d{2}-\d{2}.*', na=False)]
        df_rain[timestamp_col] = pd.to_datetime(df_rain[timestamp_col], errors='coerce')
        df_rain.rename(columns={timestamp_col: "TIMESTAMP"}, inplace=True)

        if 'Rain_p_Tot' not in df_rain.columns:
            print(f"⚠️ Ring5 Rain: Missing 'Rain_p_Tot' in {path}")
            continue

        df_rain = df_rain[['TIMESTAMP', 'Rain_p_Tot']].dropna(subset=['Rain_p_Tot'])
        df_rain['Rain_mm'] = pd.to_numeric(df_rain['Rain_p_Tot'], errors='coerce')
        df_rain = df_rain[['TIMESTAMP', 'Rain_mm']]

        all_rain_dfs.append(df_rain)

    # recent file
    if not os.path.exists(recent_path) or os.path.getsize(path) == 0:
        print(f"⚠️ Ring5 Rain: Recent file {recent_path} is empty or missing!")
        return None

    df_rain_recent = pd.read_csv(recent_path, low_memory=False, skiprows=1)
    timestamp_col = next((col for col in df_rain_recent.columns if "timestamp" in col.lower()), None)
    if not timestamp_col:
        print(f"⚠️ Ring5 Rain: No timestamp column found in {recent_path}")
        return None

    df_rain_recent = df_rain_recent[df_rain_recent[timestamp_col].str.match(r'\d{4}-\d{2}-\d{2}.*', na=False)]
    df_rain_recent[timestamp_col] = pd.to_datetime(df_rain_recent[timestamp_col], errors='coerce')
    df_rain_recent.rename(columns={timestamp_col: "TIMESTAMP"}, inplace=True)

    if 'Rain_p_Tot' in df_rain_recent.columns:
        df_rain_recent['Rain_mm'] = pd.to_numeric(df_rain_recent['Rain_p_Tot'], errors='coerce')
        df_rain_recent = df_rain_recent[['TIMESTAMP', 'Rain_mm']].dropna(subset=['Rain_mm'])
        all_rain_dfs.append(df_rain_recent)
    else:
        print(f"⚠️ Ring5 Rain: Missing 'Rain_p_Tot' in {recent_path}")
        return None

    if len(all_rain_dfs) == 0:
        return None

    rain_combined = pd.concat(all_rain_dfs, ignore_index=True).drop_duplicates(subset='TIMESTAMP', keep='first')
    return rain_combined


def load_and_process_ring2_wind_data(historical_paths, recent_path):
    """
    Specifically loads wind data for Ring_2 from the same CSVs.
    Extracts Speed_WVc(1) (wind speed) and Speed_WVc(2) (wind direction).
    Returns a DataFrame with columns: [TIMESTAMP, Wind_Speed, Wind_Dir].
    """
    all_wind_dfs = []

    for path in historical_paths:
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            print(f"⚠️ Ring2 Wind: File {path} is empty or missing!")
            continue

        df_wind = pd.read_csv(path, low_memory=False, skiprows=1)
        timestamp_col = next((col for col in df_wind.columns if "timestamp" in col.lower()), None)
        if not timestamp_col:
            print(f"⚠️ Ring2 Wind: No timestamp column found in {path}")
            continue

        df_wind = df_wind[df_wind[timestamp_col].str.match(r'\d{4}-\d{2}-\d{2}.*', na=False)]
        df_wind[timestamp_col] = pd.to_datetime(df_wind[timestamp_col], errors='coerce')
        df_wind.rename(columns={timestamp_col: "TIMESTAMP"}, inplace=True)

        if 'Speed_WVc(1)' not in df_wind.columns or 'Speed_WVc(2)' not in df_wind.columns:
            print(f"⚠️ Ring2 Wind: Missing Speed_WVc(1) or Speed_WVc(2) in {path}")
            continue

        df_wind = df_wind[['TIMESTAMP', 'Speed_WVc(1)', 'Speed_WVc(2)']].dropna(subset=['Speed_WVc(1)', 'Speed_WVc(2)'])
        df_wind['Wind_Speed'] = pd.to_numeric(df_wind['Speed_WVc(1)'], errors='coerce')
        df_wind['Wind_Dir']   = pd.to_numeric(df_wind['Speed_WVc(2)'], errors='coerce')

        df_wind = df_wind[['TIMESTAMP', 'Wind_Speed', 'Wind_Dir']]
        all_wind_dfs.append(df_wind)

    # Recent file
    if not os.path.exists(recent_path) or os.path.getsize(recent_path) == 0:
        print(f"⚠️ Ring2 Wind: Recent file {recent_path} is empty or missing!")
        return None

    df_wind_recent = pd.read_csv(recent_path, low_memory=False, skiprows=1)
    timestamp_col = next((col for col in df_wind_recent.columns if "timestamp" in col.lower()), None)
    if not timestamp_col:
        print(f"⚠️ Ring2 Wind: No timestamp column found in {recent_path}")
        return None

    df_wind_recent = df_wind_recent[df_wind_recent[timestamp_col].str.match(r'\d{4}-\d{2}-\d{2}.*', na=False)]
    df_wind_recent[timestamp_col] = pd.to_datetime(df_wind_recent[timestamp_col], errors='coerce')
    df_wind_recent.rename(columns={timestamp_col: "TIMESTAMP"}, inplace=True)

    if 'Speed_WVc(1)' in df_wind_recent.columns and 'Speed_WVc(2)' in df_wind_recent.columns:
        df_wind_recent['Wind_Speed'] = pd.to_numeric(df_wind_recent['Speed_WVc(1)'], errors='coerce')
        df_wind_recent['Wind_Dir']   = pd.to_numeric(df_wind_recent['Speed_WVc(2)'], errors='coerce')
        df_wind_recent = df_wind_recent[['TIMESTAMP', 'Wind_Speed', 'Wind_Dir']].dropna(subset=['Wind_Speed', 'Wind_Dir'])
        all_wind_dfs.append(df_wind_recent)
    else:
        print(f"⚠️ Ring2 Wind: Missing Speed_WVc(1) or Speed_WVc(2) in {recent_path}")
        return None

    if len(all_wind_dfs) == 0:
        return None

    wind_combined = pd.concat(all_wind_dfs, ignore_index=True).drop_duplicates(subset='TIMESTAMP', keep='first')
    return wind_combined


# =========================
# 4. Cache the Data Download
# =========================
@st.cache_data
def download_and_load_all_data(drive_links):
    """
    Downloads all historical & recent CSVs for each ring from Google Drive,
    loads them, merges into a single DataFrame for CO₂,
    plus specific data from:
      - Ring_4: T & RH
      - Ring_5: Rain
      - Ring_2: Wind
    """
    all_rings_data = []
    ring4_temp_rh_data = None
    ring5_rain_data = None
    ring2_wind_data = None

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

        if ring == "Ring_4":
            ring4_temp_rh_data = load_and_process_ring4_temp_rh_data(historical_files, recent_file)

        if ring == "Ring_2":
            ring2_wind_data = load_and_process_ring2_wind_data(historical_files, recent_file)

        if ring == "Ring_5":
            ring5_rain_data = load_and_process_ring5_rain_data(historical_files, recent_file)

    df_combined = pd.concat(all_rings_data, ignore_index=True)
    return df_combined, ring4_temp_rh_data, ring5_rain_data, ring2_wind_data

# =========================
# 5. Main Application
# =========================
def main():
    st.title("WheatDryFACE Monitoring Dashboard")

    # =============== Sidebar: REFRESH DATA ===============
    st.sidebar.subheader("Data Refresh")
    if st.sidebar.button("Refresh Data"):
        st.cache_data.clear()
        st.session_state["force_rerun"] = not st.session_state["force_rerun"]

    st.write("Force Rerun State:", st.session_state["force_rerun"])  # Debug info

    # =============== Load Data ===============
    drive_links = {}
    for ring_num in range(1, 7):
        ring_key = f"Ring_{ring_num}"
        drive_links[ring_key] = {
            "historical": st.secrets["drive_links"][ring_key]["historical"],
            "recent": st.secrets["drive_links"][ring_key]["recent"]
        }

    df_co2, ring4_trh_df, ring5_rain_df, ring2_wind_df = download_and_load_all_data(drive_links)

    # ------------- Sidebar Filters -------------
    st.sidebar.header("Plot Filters")

    selected_rings = st.sidebar.multiselect(
        "Select Rings for CO₂:",
        sorted(df_co2['Rings'].unique()),
        default=sorted(df_co2['Rings'].unique())
    )

    co2_type_selection = st.sidebar.selectbox(
        "Select CO₂ Type:",
        ["All", "aCO2", "eCO2"],
        index=0
    )

    plot_date_range = st.sidebar.date_input(
        "Select Plot Date Range:",
        [df_co2['TIMESTAMP'].min().date(), df_co2['TIMESTAMP'].max().date()]
    )

    rolling_window = st.sidebar.slider(
        "Select Rolling Window (5-min intervals):",
        min_value=1, max_value=60, value=12
    )

    # -------------------------
    # Filter CO₂
    # -------------------------
    df_plot_filtered = df_co2.copy()
    if co2_type_selection != "All":
        df_plot_filtered = df_plot_filtered[df_plot_filtered["CO2"] == co2_type_selection]
    df_plot_filtered = df_plot_filtered[df_plot_filtered["Rings"].isin(selected_rings)]
    df_plot_filtered = df_plot_filtered[
        (df_plot_filtered['TIMESTAMP'].dt.date >= plot_date_range[0]) &
        (df_plot_filtered['TIMESTAMP'].dt.date <= plot_date_range[-1])
    ]

    # -------------------------
    # Constants for dashed lines
    # -------------------------
    TARGET = 600
    TOLERANCE_PERCENT = 0.1  # 10%
    lower_bound = TARGET * (1 - TOLERANCE_PERCENT)  # 540
    upper_bound = TARGET * (1 + TOLERANCE_PERCENT)  # 660

    # ============= 1) RAW DATA (4 Rows) =============
    st.subheader("5-min Raw Data")

    trh_raw = pd.DataFrame()
    rain_raw = pd.DataFrame()
    wind_raw = pd.DataFrame()
    df_co2_raw = pd.DataFrame()

    # Create figure with custom row heights & figure height
    fig_raw = make_subplots(
        rows=4, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.02,
        row_heights=[0.2, 0.15, 0.25, 0.4],  # (20%, 15%, 25%, 40%)
        specs=[
            [{"secondary_y": True}],  # Row 1 => T & RH
            [{}],                     # Row 2 => Rain
            [{"secondary_y": True}],  # Row 3 => Wind
            [{}]                      # Row 4 => CO₂
        ]
    )
    fig_raw.update_layout(height=900)

    # ----- ROW 1: T & RH (Ring_4) -----
    if ring4_trh_df is not None and not ring4_trh_df.empty:
        trh_raw = ring4_trh_df[
            (ring4_trh_df['TIMESTAMP'].dt.date >= plot_date_range[0]) &
            (ring4_trh_df['TIMESTAMP'].dt.date <= plot_date_range[-1])
        ].copy()
        trh_raw.sort_values("TIMESTAMP", inplace=True)

        if not trh_raw.empty:
            fig_raw.add_trace(
                go.Scatter(
                    x=trh_raw["TIMESTAMP"],
                    y=trh_raw["T_C"],
                    mode='lines',
                    name='T (°C)',
                    connectgaps=False
                ),
                row=1, col=1, secondary_y=False
            )
            fig_raw.add_trace(
                go.Scatter(
                    x=trh_raw["TIMESTAMP"],
                    y=trh_raw["RH"],
                    mode='lines',
                    name='RH (%)',
                    connectgaps=False
                ),
                row=1, col=1, secondary_y=True
            )

    # ----- ROW 2: Rain (Ring_5) as bars with reversed y-axis -----
    if ring5_rain_df is not None and not ring5_rain_df.empty:
        rain_raw = ring5_rain_df[
            (ring5_rain_df['TIMESTAMP'].dt.date >= plot_date_range[0]) &
            (ring5_rain_df['TIMESTAMP'].dt.date <= plot_date_range[-1])
        ].copy()
        rain_raw.sort_values("TIMESTAMP", inplace=True)

        if not rain_raw.empty:
            fig_raw.add_trace(
                go.Bar(
                    x=rain_raw["TIMESTAMP"],
                    y=rain_raw["Rain_mm"],
                    name="Rain (mm)",
                    marker=dict(color='blue')
                ),
                row=2, col=1
            )
            fig_raw.update_yaxes(autorange="reversed", row=2, col=1)

    # ----- ROW 3: Wind (Ring_2) -----
    if ring2_wind_df is not None and not ring2_wind_df.empty:
        wind_raw = ring2_wind_df[
            (ring2_wind_df['TIMESTAMP'].dt.date >= plot_date_range[0]) &
            (ring2_wind_df['TIMESTAMP'].dt.date <= plot_date_range[-1])
        ].copy()
        wind_raw.sort_values("TIMESTAMP", inplace=True)

        if not wind_raw.empty:
            fig_raw.add_trace(
                go.Scatter(
                    x=wind_raw["TIMESTAMP"],
                    y=wind_raw["Wind_Speed"],
                    mode='lines',
                    name='Wind Speed (m/s)',
                    connectgaps=False
                ),
                row=3, col=1, secondary_y=False
            )
            fig_raw.add_trace(
                go.Scatter(
                    x=wind_raw["TIMESTAMP"],
                    y=wind_raw["Wind_Dir"],
                    mode='lines',
                    name='Wind Direction (°)',
                    connectgaps=False
                ),
                row=3, col=1, secondary_y=True
            )

    # ----- ROW 4: CO₂ (all selected rings) -----
    df_co2_raw = df_plot_filtered.sort_values("TIMESTAMP").copy()
    for ring_name, ring_df in df_co2_raw.groupby("Rings"):
        fig_raw.add_trace(
            go.Scatter(
                x=ring_df["TIMESTAMP"],
                y=ring_df["CO2_Avg"],
                mode='lines',
                name=f"{ring_name} CO₂ Raw",
                connectgaps=True
            ),
            row=4, col=1
        )

    # Dashed lines on row=4
    fig_raw.add_hline(
        y=lower_bound,
        line_dash="dash",
        line_color="black",
        row=4, col=1,
        annotation_text=f"Lower 10% limit ({lower_bound:.0f} ppm)",
        annotation_position="bottom right"
    )
    fig_raw.add_hline(
        y=upper_bound,
        line_dash="dash",
        line_color="green",
        row=4, col=1,
        annotation_text=f"Upper 10% limit ({upper_bound:.0f} ppm)",
        annotation_position="top right"
    )

    # Axis Labels
    fig_raw.update_yaxes(title_text="T (°C)", row=1, col=1, secondary_y=False)
    fig_raw.update_yaxes(title_text="RH (%)", row=1, col=1, secondary_y=True)
    fig_raw.update_yaxes(title_text="Rain (mm)", row=2, col=1)
    fig_raw.update_yaxes(title_text="Wind Speed (m/s)", row=3, col=1, secondary_y=False)
    fig_raw.update_yaxes(title_text="Wind Direction (°)", row=3, col=1, secondary_y=True)
    fig_raw.update_yaxes(title_text="CO₂ (ppm)", row=4, col=1)
    fig_raw.update_xaxes(title_text="Time", row=4, col=1)

    # Unique key to avoid duplicate ID error
    st.plotly_chart(fig_raw, use_container_width=True, key="raw_chart_key")

    # ============== 2) MOVING AVERAGE DATA (4 Rows) ==============
    st.subheader("Moving Average Data")

    trh_ma = pd.DataFrame()
    rain_ma = pd.DataFrame()
    wind_ma = pd.DataFrame()
    df_co2_ma = pd.DataFrame()

    fig_ma = make_subplots(
        rows=4, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.02,
        row_heights=[0.2, 0.15, 0.25, 0.4],
        specs=[
            [{"secondary_y": True}],
            [{}],
            [{"secondary_y": True}],
            [{}]
        ]
    )
    fig_ma.update_layout(height=900)

    # ----- ROW 1: T & RH (MA) -----
    if ring4_trh_df is not None and not ring4_trh_df.empty:
        trh_ma = ring4_trh_df[
            (ring4_trh_df['TIMESTAMP'].dt.date >= plot_date_range[0]) &
            (ring4_trh_df['TIMESTAMP'].dt.date <= plot_date_range[-1])
        ].copy()
        trh_ma.sort_values("TIMESTAMP", inplace=True)

        if not trh_ma.empty:
            trh_ma["T_C_MA"] = trh_ma["T_C"].rolling(rolling_window).mean()
            trh_ma["RH_MA"]  = trh_ma["RH"].rolling(rolling_window).mean()

            fig_ma.add_trace(
                go.Scatter(
                    x=trh_ma["TIMESTAMP"],
                    y=trh_ma["T_C_MA"],
                    mode='lines',
                    name='T (°C) - MA',
                    connectgaps=False
                ),
                row=1, col=1, secondary_y=False
            )
            fig_ma.add_trace(
                go.Scatter(
                    x=trh_ma["TIMESTAMP"],
                    y=trh_ma["RH_MA"],
                    mode='lines',
                    name='RH (%) - MA',
                    connectgaps=False
                ),
                row=1, col=1, secondary_y=True
            )

    # ----- ROW 2: Rain (MA) -----
    if ring5_rain_df is not None and not ring5_rain_df.empty:
        rain_ma = ring5_rain_df[
            (ring5_rain_df['TIMESTAMP'].dt.date >= plot_date_range[0]) &
            (ring5_rain_df['TIMESTAMP'].dt.date <= plot_date_range[-1])
        ].copy()
        rain_ma.sort_values("TIMESTAMP", inplace=True)

        if not rain_ma.empty:
            rain_ma["Rain_mm_MA"] = rain_ma["Rain_mm"].rolling(rolling_window).sum()

            fig_ma.add_trace(
                go.Scatter(
                    x=rain_ma["TIMESTAMP"],
                    y=rain_ma["Rain_mm_MA"],
                    mode='lines',
                    name="Rain (mm) - MSum",
                    connectgaps=False,
                    line=dict(color='blue')
                ),
                row=2, col=1
            )
            fig_ma.update_yaxes(autorange="reversed", row=2, col=1)

    # ----- ROW 3: Wind (MA) -----
    if ring2_wind_df is not None and not ring2_wind_df.empty:
        wind_ma = ring2_wind_df[
            (ring2_wind_df['TIMESTAMP'].dt.date >= plot_date_range[0]) &
            (ring2_wind_df['TIMESTAMP'].dt.date <= plot_date_range[-1])
        ].copy()
        wind_ma.sort_values("TIMESTAMP", inplace=True)

        if not wind_ma.empty:
            wind_ma["Wind_Speed_MA"] = wind_ma["Wind_Speed"].rolling(rolling_window).mean()
            wind_ma["Wind_Dir_MA"]   = wind_ma["Wind_Dir"].rolling(rolling_window).mean()

            fig_ma.add_trace(
                go.Scatter(
                    x=wind_ma["TIMESTAMP"],
                    y=wind_ma["Wind_Speed_MA"],
                    mode='lines',
                    name='Wind Speed (m/s) - MA',
                    connectgaps=False
                ),
                row=3, col=1, secondary_y=False
            )
            fig_ma.add_trace(
                go.Scatter(
                    x=wind_ma["TIMESTAMP"],
                    y=wind_ma["Wind_Dir_MA"],
                    mode='lines',
                    name='Wind Direction (°) - MA',
                    connectgaps=False
                ),
                row=3, col=1, secondary_y=True
            )

    # ----- ROW 4: CO₂ (MA) -----
    df_co2_ma = df_plot_filtered.sort_values("TIMESTAMP").copy()
    df_co2_ma["CO2_Avg_MA"] = df_co2_ma.groupby("Rings")["CO2_Avg"].transform(
        lambda x: x.rolling(rolling_window).mean()
    )

    for ring_name, ring_df in df_co2_ma.groupby("Rings"):
        fig_ma.add_trace(
            go.Scatter(
                x=ring_df["TIMESTAMP"],
                y=ring_df["CO2_Avg_MA"],
                mode='lines',
                name=f"{ring_name} CO₂ MA",
                connectgaps=True
            ),
            row=4, col=1
        )

    fig_ma.add_hline(
        y=lower_bound,
        line_dash="dash",
        line_color="black",
        row=4, col=1,
        annotation_text=f"Lower 10% limit ({lower_bound:.0f} ppm)",
        annotation_position="bottom right"
    )
    fig_ma.add_hline(
        y=upper_bound,
        line_dash="dash",
        line_color="green",
        row=4, col=1,
        annotation_text=f"Upper 10% limit ({upper_bound:.0f} ppm)",
        annotation_position="top right"
    )

    fig_ma.update_yaxes(title_text="T (°C)", row=1, col=1, secondary_y=False)
    fig_ma.update_yaxes(title_text="RH (%)", row=1, col=1, secondary_y=True)
    fig_ma.update_yaxes(title_text="Rain (mm)", row=2, col=1)
    fig_ma.update_yaxes(title_text="Wind Speed (m/s)", row=3, col=1, secondary_y=False)
    fig_ma.update_yaxes(title_text="Wind Direction (°)", row=3, col=1, secondary_y=True)
    fig_ma.update_yaxes(title_text="CO₂ (ppm)", row=4, col=1)
    fig_ma.update_xaxes(title_text="Time", row=4, col=1)

    # Unique key for the MA chart
    st.plotly_chart(fig_ma, use_container_width=True, key="ma_chart_key")

    # ============== Stats Section (Separate Range) ==============
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
        [df_co2['TIMESTAMP'].min().date(), df_co2['TIMESTAMP'].max().date()]
    )

    st.subheader("Stats Time-of-Day Range")
    start_time = st.time_input("Start Time (hh:mm)", datetime.time(0, 0))
    end_time = st.time_input("End Time (hh:mm)", datetime.time(23, 59))

    df_stats_filtered = df_co2.copy()
    df_stats_filtered = df_stats_filtered[df_stats_filtered["Rings"].isin(selected_rings)]
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

    # Exclude eCO2 < 380
    df_stats_filtered = df_stats_filtered[
        ~((df_stats_filtered["CO2"] == "eCO2") & (df_stats_filtered["CO2_Avg"] < 380))
    ]

    if stat_source == "Rolling Average":
        df_stats_filtered = df_stats_filtered.sort_values("TIMESTAMP").copy()
        df_stats_filtered["CO2_Avg_MA"] = df_stats_filtered.groupby("Rings")["CO2_Avg"].transform(
            lambda x: x.rolling(rolling_window).mean()
        )
        stat_column = "CO2_Avg_MA"
    else:
        stat_column = "CO2_Avg"

    if df_stats_filtered.empty or df_stats_filtered[stat_column].isna().all():
        st.warning("No data available in the specified Stats date/time range (or after excluding eCO2 < 380).")
    else:
        df_stats = df_stats_filtered.groupby("Rings")[stat_column].agg(["mean", "std"]).reset_index()
        df_stats.rename(columns={"mean": "Mean CO₂ (ppm)", "std": "Std Dev"}, inplace=True)
        st.write(
            f"**Stats computed for data between** "
            f"`{stats_date_range[0]} - {stats_date_range[-1]}` "
            f"**and** `{start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}`."
        )
        st.write(f"**Data source:** {'Rolling Average' if stat_source == 'Rolling Average' else 'Raw Data'}")
        st.write("**Note:** Values < 380 for eCO₂ have been excluded from stats.")
        st.dataframe(df_stats, use_container_width=True)

    # === NEW: Data Downloads (Password Protected) ===
    st.subheader("Data Downloads (Password Protected)")
    password = st.text_input("Enter password to enable data downloads:", type="password")
    correct_pw = st.secrets["password"]

    if password == correct_pw:
        st.success("Correct password! You can now download the CSV files.")
        
        # -- Download Raw Data --
        if not df_co2_raw.empty:
            st.download_button(
                "Download Raw CO₂ CSV",
                data=df_co2_raw.to_csv(index=False),
                file_name="raw_co2.csv",
                mime="text/csv"
            )
        if not wind_raw.empty:
            st.download_button(
                "Download Raw Wind CSV",
                data=wind_raw.to_csv(index=False),
                file_name="raw_wind.csv",
                mime="text/csv"
            )
        if not rain_raw.empty:
            st.download_button(
                "Download Raw Rain CSV",
                data=rain_raw.to_csv(index=False),
                file_name="raw_rain.csv",
                mime="text/csv"
            )
        if not trh_raw.empty:
            st.download_button(
                "Download Raw Temp_RH CSV",
                data=trh_raw.to_csv(index=False),
                file_name="raw_temp_rh.csv",
                mime="text/csv"
            )

        st.write("---")

        # -- Download Moving Average Data --
        if not df_co2_ma.empty:
            st.download_button(
                "Download MA CO₂ CSV",
                data=df_co2_ma[["TIMESTAMP","Rings","CO2_Avg_MA"]].dropna().to_csv(index=False),
                file_name="ma_co2.csv",
                mime="text/csv"
            )
        if not wind_ma.empty and "Wind_Speed_MA" in wind_ma.columns:
            ma_wind = wind_ma[["TIMESTAMP","Wind_Speed_MA","Wind_Dir_MA"]].dropna()
            st.download_button(
                "Download MA Wind CSV",
                data=ma_wind.to_csv(index=False),
                file_name="ma_wind.csv",
                mime="text/csv"
            )
        if not rain_ma.empty and "Rain_mm_MA" in rain_ma.columns:
            st.download_button(
                "Download MA Rain CSV",
                data=rain_ma[["TIMESTAMP","Rain_mm_MA"]].dropna().to_csv(index=False),
                file_name="ma_rain.csv",
                mime="text/csv"
            )
        if not trh_ma.empty and "T_C_MA" in trh_ma.columns:
            ma_trh = trh_ma[["TIMESTAMP","T_C_MA","RH_MA"]].dropna()
            st.download_button(
                "Download MA Temp_RH CSV",
                data=ma_trh.to_csv(index=False),
                file_name="ma_temp_rh.csv",
                mime="text/csv"
            )
    else:
        st.warning("Enter the correct password to enable data downloads.")

def run_app():
    main()

if __name__ == "__main__":
    run_app()
