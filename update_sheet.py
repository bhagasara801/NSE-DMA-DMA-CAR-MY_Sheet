#!/usr/bin/env python3
import os
import json
import argparse
import logging
from datetime import datetime, timedelta
import io
import zipfile

import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ------- Configuration / Defaults -------
DEFAULT_TOP = 250
DEFAULT_VOLUME_WS = "Top 250 Stocks"
DEFAULT_TURNOVER_WS = "Top 250 Turnover"
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# ------- Helpers -------
def setup_logger(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=level)

def requests_session_with_retries(total_retries=3, backoff_factor=0.3):
    s = requests.Session()
    retries = Retry(total=total_retries, backoff_factor=backoff_factor,
                    status_forcelist=(500, 502, 503, 504))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

# ------- NSE Bhavcopy fetcher -------
def fetch_bhavcopy_for_date(date_obj, session=None):
    date_str = date_obj.strftime("%Y%m%d")
    url = f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000.csv.zip"
    headers = {'User-Agent': 'Mozilla/5.0'}
    logging.info("Checking date %s ...", date_obj.strftime("%d-%b-%Y"))
    session = session or requests_session_with_retries()
    try:
        resp = session.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            logging.debug("NSE server returned status: %s for %s", resp.status_code, date_str)
            return None
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            csv_name = z.namelist()[0]
            with z.open(csv_name) as f:
                df = pd.read_csv(f)
                return df
    except Exception as e:
        logging.debug("Error fetching bhavcopy for %s: %s", date_str, e)
        return None

# ------- Data processing -------
def normalize_columns(df):
    # Determine symbol/close/series columns and volume/turnover columns
    sym_col = 'TckrSymb' if 'TckrSymb' in df.columns else ('SYMBOL' if 'SYMBOL' in df.columns else None)
    close_col = 'ClsPric' if 'ClsPric' in df.columns else ('CLOSE' if 'CLOSE' in df.columns else None)
    series_col = 'SctySrs' if 'SctySrs' in df.columns else ('SERIES' if 'SERIES' in df.columns else None)

    vol_col_candidates = ['TtlTradgVol', 'TOTTRDQTY', 'TtlTrdQty', 'TotTrdQty', 'VOLUME']
    vol_col = next((c for c in vol_col_candidates if c in df.columns), None)

    turnover_candidates = ['TtlTrfVal', 'TOTTRDVAL', 'TtlTrdVal', 'TotTrdVal', 'TURNOVER']
    turnover_col = next((c for c in turnover_candidates if c in df.columns), None)

    return sym_col, close_col, series_col, vol_col, turnover_col

def filter_and_prepare(df):
    sym_col, close_col, series_col, vol_col, turnover_col = normalize_columns(df)
    if not sym_col:
        raise ValueError("Symbol column not found in bhavcopy.")
    # Filter EQ series
    if series_col and series_col in df.columns:
        df = df[df[series_col].astype(str).str.strip() == 'EQ']
    # Remove ETF/GOLD/etc by symbol patterns
    filter_keywords = 'BEES|ETF|GOLD|LIQUID|CASE|SILVER|LIQ'
    df = df[~df[sym_col].astype(str).str.contains(filter_keywords, case=False, na=False)]
    # Convert numeric columns
    if vol_col:
        df[vol_col] = pd.to_numeric(df[vol_col], errors='coerce')
    if turnover_col:
        df[turnover_col] = pd.to_numeric(df[turnover_col], errors='coerce')
    return df, sym_col, close_col, vol_col, turnover_col

def top_n_by_column(df, sym_col, close_col, col, n=DEFAULT_TOP):
    if not col:
        return []
    df2 = df.dropna(subset=[col]).sort_values(by=col, ascending=False).head(n)
    # Return list of [symbol, value, close] to match previous shape
    if close_col and close_col in df2.columns:
        return df2[[sym_col, col, close_col]].values.tolist()
    else:
        return df2[[sym_col, col]].values.tolist()

# ------- Google Sheets update -------
def authorize_gspread(gcp_credentials_json):
    creds_dict = json.loads(gcp_credentials_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
    client = gspread.authorize(creds)
    return client

def update_worksheet(ws, start_cell, data, top_n):
    # clear then update
    if not data:
        logging.warning("No data to write for worksheet %s", ws.title)
        return
    try:
        # clear A2..C{top_n+1}
        end_row = top_n + 1
        ws.batch_clear([f"A2:C{end_row}"])
        ws.update('A2', data)
        logging.info("Updated worksheet %s with %d rows", ws.title, len(data))
    except Exception as e:
        logging.error("Failed to update worksheet %s: %s", ws.title, e)
        raise

# ------- Main -------
def main():
    parser = argparse.ArgumentParser(description="Fetch NSE bhavcopy and update Google Sheets.")
    parser.add_argument("--top", type=int, default=DEFAULT_TOP, help="Top N rows to write (default: 250)")
    parser.add_argument("--mode", choices=["both", "volume", "turnover"], default="both",
                        help="Which metric to write: both (default), volume, or turnover")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to sheet; print sample output")
    parser.add_argument("--days-back", type=int, default=7, help="How many past business days to try when finding bhavcopy (default 7)")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    setup_logger(args.verbose)
    session = requests_session_with_retries()

    # Read environment-config
    gcp_credentials = os.environ.get("GCP_CREDENTIALS")
    spreadsheet_id = os.environ.get("SPREADSHEET_ID")
    ws_volume_name = os.environ.get("WORKSHEET_VOLUME", DEFAULT_VOLUME_WS)
    ws_turnover_name = os.environ.get("WORKSHEET_TURNOVER", DEFAULT_TURNOVER_WS)

    if not gcp_credentials:
        logging.critical("Missing GCP_CREDENTIALS environment variable (service account JSON).")
        raise SystemExit(1)
    if not spreadsheet_id:
        logging.critical("Missing SPREADSHEET_ID environment variable.")
        raise SystemExit(1)

    # Find bhavcopy dataframe from recent days (skip weekends)
    today = datetime.now()
    df_found = None
    fetched_date = None
    for i in range(args.days_back):
        candidate = today - timedelta(days=i)
        if candidate.weekday() >= 5:  # Saturday=5, Sunday=6
            continue
        df = fetch_bhavcopy_for_date(candidate, session=session)
        if df is not None:
            df_found = df
            fetched_date = candidate
            break

    if df_found is None:
        logging.critical("No bhavcopy found in the last %d days", args.days_back)
        raise SystemExit(1)

    logging.info("Bhavcopy fetched for %s", fetched_date.strftime("%d-%b-%Y"))
    df_prepared, sym_col, close_col, vol_col, turnover_col = filter_and_prepare(df_found)

    # Build top lists
    data_vol = top_n_by_column(df_prepared, sym_col, close_col, vol_col, args.top) if (args.mode in ["both", "volume"]) else None
    data_turnover = top_n_by_column(df_prepared, sym_col, close_col, turnover_col, args.top) if (args.mode in ["both", "turnover"]) else None

    # Dry-run: print samples and exit
    if args.dry_run:
        if data_vol is not None:
            logging.info("Sample Top Volume rows (first 5):")
            for row in data_vol[:5]:
                print(row)
        if data_turnover is not None:
            logging.info("Sample Top Turnover rows (first 5):")
            for row in data_turnover[:5]:
                print(row)
        logging.info("Dry run complete. No writes performed.")
        return

    # Authorize and update sheets
    client = authorize_gspread(gcp_credentials)
    try:
        sh = client.open_by_key(spreadsheet_id)
    except Exception as e:
        logging.critical("Unable to open spreadsheet: %s", e)
        raise SystemExit(1)

    try:
        if data_vol is not None:
            ws_volume = sh.worksheet(ws_volume_name)
            update_worksheet(ws_volume, 'A2', data_vol, args.top)
        if data_turnover is not None:
            ws_turnover = sh.worksheet(ws_turnover_name)
            update_worksheet(ws_turnover, 'A2', data_turnover, args.top)

        # Update status cell K2 in both sheets (if present)
        ist_now = (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime('%d-%b %H:%M')
        status_msg = f"Data Date: {fetched_date.strftime('%d-%b-%Y')} | Last Update: {ist_now} (IST)"
        try:
            if data_vol is not None:
                ws_volume.update('K2', [[status_msg]])
            if data_turnover is not None:
                ws_turnover.update('K2', [[status_msg]])
        except Exception:
            logging.debug("Status cell update failed or K column not present.")
        logging.info("SUCCESS: Sheets updated.")
    except Exception as e:
        logging.critical("Error updating sheets: %s", e)
        raise SystemExit(1)

if __name__ == "__main__":
    main()
