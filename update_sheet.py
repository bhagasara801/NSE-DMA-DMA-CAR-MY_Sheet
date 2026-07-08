#!/usr/bin/env python3
import os
import json
import argparse
import logging
from datetime import datetime, timedelta
import io
import zipfile
import string

import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ------- Configuration / Defaults -------
DEFAULT_TOP = 250
DEFAULT_VOLUME_WS = "Top 250 Stocks"        # unused in single layout but kept for compatibility
DEFAULT_TURNOVER_WS = "Top 250 Turnover"
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Single-sheet layout defaults (can be overridden via env)
DEFAULT_SHEET_NAME = os.environ.get("SHEET_NAME", "Bhav Copy Data")
DEFAULT_START_ROW = int(os.environ.get("START_ROW", "8"))
DEFAULT_VOLUME_START_COL = os.environ.get("VOLUME_START_COL", "A")   # A..D will be used
DEFAULT_TURNOVER_START_COL = os.environ.get("TURNOVER_START_COL", "F")  # F..I will be used

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

def col_offset(col_letter, offset):
    # Simple increment within A-Z; works for our expected small offsets
    base = ord(col_letter.upper()) - ord('A')
    new = base + offset
    return chr(ord('A') + new)

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
    # Determine symbol/close/series/prev close/volume/turnover columns
    sym_col = next((c for c in ['TckrSymb','SYMBOL','SYMBOL1','SYMBOL2'] if c in df.columns), None)
    close_col = next((c for c in ['ClsPric','CLOSE','Close','Close Price'] if c in df.columns), None)
    series_col = next((c for c in ['SctySrs','SERIES','Series'] if c in df.columns), None)
    prev_close_candidates = ['PrvCls','PREVCLOSE','Prev Close','PREV_CLOSE','PREVCLOSE']
    prev_close_col = next((c for c in prev_close_candidates if c in df.columns), None)

    vol_col_candidates = ['TtlTradgVol','TOTTRDQTY','TtlTrdQty','TotTrdQty','VOLUME','TOTTRDQTY']
    vol_col = next((c for c in vol_col_candidates if c in df.columns), None)

    turnover_candidates = ['TtlTrfVal','TOTTRDVAL','TtlTrdVal','TotTrdVal','TURNOVER','TOTTRDVAL']
    turnover_col = next((c for c in turnover_candidates if c in df.columns), None)

    return sym_col, close_col, series_col, prev_close_col, vol_col, turnover_col

def filter_and_prepare(df):
    sym_col, close_col, series_col, prev_close_col, vol_col, turnover_col = normalize_columns(df)
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
    if close_col:
        df[close_col] = pd.to_numeric(df[close_col], errors='coerce')
    if prev_close_col:
        df[prev_close_col] = pd.to_numeric(df[prev_close_col], errors='coerce')
    return df, sym_col, close_col, prev_close_col, vol_col, turnover_col

def top_n_with_prev(df, sym_col, close_col, prev_close_col, col, n=DEFAULT_TOP):
    if not col:
        return []
    cols = [sym_col, col]
    if close_col and close_col in df.columns:
        cols.insert(1, close_col)  # symbol, close, value (for now)
    df2 = df.dropna(subset=[col]).sort_values(by=col, ascending=False).head(n)
    # Build rows as [symbol, close, prev_close, value] if prev_close exists, else [symbol, close, value]
    rows = []
    for _, r in df2.iterrows():
        sym = r.get(sym_col)
        close = r.get(close_col) if close_col in df2.columns else ''
        prevc = r.get(prev_close_col) if (prev_close_col and prev_close_col in df2.columns) else ''
        val = r.get(col)
        # Normalize numbers to plain Python types
        def norm(x):
            if pd.isna(x):
                return ''
            if isinstance(x, (float, int, complex)):
                if float(x).is_integer():
                    return int(x)
                return float(x)
            return x
        rows.append([sym, norm(close), norm(prevc), norm(val)])
    return rows

# ------- Google Sheets update (single-sheet layout) -------
def authorize_gspread(gcp_credentials_json):
    creds_dict = json.loads(gcp_credentials_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
    client = gspread.authorize(creds)
    return client

def clear_range(ws, start_col, start_row, n_rows, n_cols):
    end_col = col_offset(start_col, n_cols-1)
    end_row = start_row + n_rows - 1
    range_str = f"{start_col}{start_row}:{end_col}{end_row}"
    try:
        ws.batch_clear([range_str])
    except Exception as e:
        logging.debug("Clear range failed for %s: %s", range_str, e)

def update_block(ws, start_col, start_row, data, top_n):
    # data is list of rows (each a list of length 4 ideally)
    n_cols = 4
    n_rows = max(len(data), top_n)
    end_col = col_offset(start_col, n_cols-1)
    end_row = start_row + n_rows - 1
    range_start = f"{start_col}{start_row}"
    range_end = f"{end_col}{end_row}"
    # Prepare values — pad to n_cols
    values = []
    for row in data:
        row = list(row)
        # ensure length 4
        while len(row) < n_cols:
            row.append('')
        values.append(row)
    # If fewer rows than top_n, pad with blanks so update size is stable
    while len(values) < top_n:
        values.append([''] * n_cols)
    try:
        ws.batch_clear([f"{start_col}{start_row}:{end_col}{end_row}"])
        ws.update(range_start, values)
        logging.info("Updated block %s:%s with %d rows", range_start, range_end, len(data))
    except Exception as e:
        logging.error("Failed to update block %s:%s - %s", range_start, range_end, e)
        raise

# ------- Main -------
def main():
    parser = argparse.ArgumentParser(description="Fetch NSE bhavcopy and update Google Sheets (single-sheet side-by-side layout).")
    parser.add_argument("--top", type=int, default=DEFAULT_TOP, help="Top N rows to write (default: 250)")
    parser.add_argument("--mode", choices=["both", "volume", "turnover"], default="both",
                        help="Which metric to write: both (default), volume, or turnover")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to sheet; print sample output")
    parser.add_argument("--days-back", type=int, default=7, help="How many past business days to try when finding bhavcopy (default 7)")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    setup_logger(args.verbose)
    session = requests_session_with_retries()

    # Read environment-config / layout
    gcp_credentials = os.environ.get("GCP_CREDENTIALS")
    spreadsheet_id = os.environ.get("SPREADSHEET_ID")
    sheet_name = os.environ.get("SHEET_NAME", DEFAULT_SHEET_NAME)
    start_row = int(os.environ.get("START_ROW", DEFAULT_START_ROW))
    vol_start_col = os.environ.get("VOLUME_START_COL", DEFAULT_VOLUME_START_COL)
    trn_start_col = os.environ.get("TURNOVER_START_COL", DEFAULT_TURNOVER_START_COL)

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
    df_prepared, sym_col, close_col, prev_close_col, vol_col, turnover_col = filter_and_prepare(df_found)

    data_vol = None
    data_turn = None
    if args.mode in ["both", "volume"]:
        data_vol = top_n_with_prev(df_prepared, sym_col, close_col, prev_close_col, vol_col, args.top)
    if args.mode in ["both", "turnover"]:
        data_turn = top_n_with_prev(df_prepared, sym_col, close_col, prev_close_col, turnover_col, args.top)

    if args.dry_run:
        if data_vol is not None:
            logging.info("Sample Top Volume rows (first 5):")
            for row in data_vol[:5]:
                print(row)
        if data_turn is not None:
            logging.info("Sample Top Turnover rows (first 5):")
            for row in data_turn[:5]:
                print(row)
        logging.info("Dry run complete. No writes performed.")
        return

    client = authorize_gspread(gcp_credentials)
    try:
        sh = client.open_by_key(spreadsheet_id)
    except Exception as e:
        logging.critical("Unable to open spreadsheet: %s", e)
        raise SystemExit(1)

    try:
        ws = sh.worksheet(sheet_name)
    except Exception as e:
        logging.critical("Worksheet '%s' not found: %s", sheet_name, e)
        raise SystemExit(1)

    try:
        # Update left block (Volume) at vol_start_col, start_row
        if data_vol is not None:
            update_block(ws, vol_start_col, start_row, data_vol, args.top)
        # Update right block (Turnover) at trn_start_col, start_row
        if data_turn is not None:
            update_block(ws, trn_start_col, start_row, data_turn, args.top)

        # Update status cell (K2 by default) - try both blocks, user can change if they want
        ist_dt = (datetime.utcnow() + timedelta(hours=5, minutes=30))
        ist_now_short = ist_dt.strftime('%d-%b %H:%M')
        status_msg = f"Data Date: {fetched_date.strftime('%d-%b-%Y')} | Last Update: {ist_now_short} (IST)"
        ist_full = ist_dt.strftime('%A, %d %B %Y at %H:%M:%S')
        try:
            ws.update('K2', [[status_msg]])
            ws.update('D4', [[ist_full]])
        except Exception:
            logging.debug("Status cell update failed or K/D cell not present.")
        logging.info("SUCCESS: Sheets updated.")
    except Exception as e:
        logging.critical("Error updating sheets: %s", e)
        raise SystemExit(1)

if __name__ == "__main__":
    main()
