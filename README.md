# CO₂ Monitoring Dashboard

## Overview
This Streamlit app provides a real-time dashboard for monitoring CO₂ concentrations across multiple experimental rings. The app fetches historical and recent CO₂ data from Google Drive, processes it, and visualizes trends using interactive plots.

## Features
- Downloads CO₂ data from Google Drive.
- Processes and merges historical and recent datasets.
- Filters data based on user-selected date range, ring, and CO₂ type.
- Visualizes CO₂ concentration trends with line plots.
- Computes and displays summary statistics.

## Deployment on Streamlit Cloud
### 1. Add Your Secrets
Since Streamlit Cloud does not support `.env` files, store Google Drive links in `secrets.toml` under Streamlit's secrets management.

#### **Example `secrets.toml` Format:**
```toml
[drive_links.Ring_1]
historical = [
    "https://drive.google.com/uc?id=1tcf0abdfgdgfix3vxy44Dli",
    "https://drive.google.com/uc?id=1t3XAXW234234d2PHiCKrZBHsGT62x1I"
]
recent = "https://drive.google.com/uc?id=1-0D333423472x3mWzCbfTs"
```
Repeat for other rings as needed.

To add secrets on Streamlit Cloud:
1. Go to **Streamlit Cloud**.
2. Open your app settings.
3. Paste the `secrets.toml` content in the **Secrets** section.
4. Click **Save**.

### 2. Install Dependencies
Streamlit Cloud automatically installs dependencies from `requirements.txt`. Ensure the file includes:
```plaintext
streamlit
pandas
plotly
gdown
```

### 3. Deploy the App
1. Push `app.py`, `requirements.txt`, and `secrets.toml` (without uploading it to GitHub) to your repository.
2. Connect your GitHub repository to **Streamlit Cloud**.
3. Click **Deploy** and enjoy the dashboard!

## Running Locally
If you want to run the app locally:
1. Clone the repository:
   ```bash
   git clone <repo_url>
   cd <repo_directory>
   ```
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Create a local `.env` file (only for local testing):
   ```plaintext
   RING_1_HISTORICAL_1=<Google Drive Link>
   RING_1_HISTORICAL_2=<Google Drive Link>
   RING_1_RECENT=<Google Drive Link>
   ```
5. Run the app:
   ```bash
   streamlit run app.py
   ```

## License
This project is open-source and available under the MIT License.

