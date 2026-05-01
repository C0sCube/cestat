import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


def get_dt_prev():
    tz = ZoneInfo("Asia/Kolkata")
    return (datetime.now(tz).date() - timedelta(days=1)).strftime("%d-%b-%Y")


def get_runtime_ts():
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y%m%d_%H%M%S")


def build_url(dt: str) -> str:
    return f"https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={dt}"


def fetch_data():
    dt = get_dt_prev()
    url = build_url(dt)

    print(f"[INFO] Fetching for {dt}")

    res = requests.get(url, timeout=30)
    res.raise_for_status()

    os.makedirs("data", exist_ok=True)

    ts = get_runtime_ts()
    file_path = f"data/amfi_{ts}.csv"

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(res.text)

    print(f"[INFO] Saved: {file_path}")
    return file_path


def parse_file(file_path):
    rows = []

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            if not line or not line[0].isdigit():
                continue

            parts = line.split(";")

            if len(parts) >= 8:
                rows.append(parts[:8])

    cols = [
        "scheme_code",
        "scheme_name",
        "isin_growth",
        "isin_reinvestment",
        "nav",
        "repurchase_price",
        "sale_price",
        "date",
    ]

    df = pd.DataFrame(rows, columns=cols)

    df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], format="%d-%b-%Y", errors="coerce")

    return df


def main():
    
    
    
    file_path = fetch_data()
    df = parse_file(file_path)

    print(df.head())


if __name__ == "__main__":
    main()