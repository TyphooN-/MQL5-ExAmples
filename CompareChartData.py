#!/usr/bin/env python3
"""
CompareChartData.py — Compare Darwinex monthly OHLC data against Yahoo Finance
to detect unadjusted corporate actions (splits, mergers, restructurings).

Usage:
    1. Run ExportMonthlyOHLC.mq5 in MT5 to generate the CSV
    2. Copy the CSV from MT5's Files directory to this script's directory
    3. Run: python3 CompareChartData.py MonthlyOHLC-ServerName-Date.csv

Output: CSV report of symbols with data anomalies, sorted by severity.
"""

import sys
import os

import pandas as pd
import yfinance as yf

# Threshold for flagging a month as anomalous (ratio divergence from recent baseline)
RATIO_THRESHOLD = 0.20  # 20% deviation flags an anomaly
MIN_BARS_REQUIRED = 12  # Need at least 12 months of Darwinex data to compare


def load_darwinex_data(csv_path):
    """Load the MQL5-exported monthly OHLC CSV."""
    df = pd.read_csv(csv_path, sep=';', engine='python', on_bad_lines='skip')
    df['Date'] = pd.to_datetime(df['Date'])
    df['Close'] = pd.to_numeric(df['Close'], errors='coerce')
    return df


def get_yahoo_monthly(symbol, start_date):
    """Fetch Yahoo Finance adjusted monthly close data for a symbol."""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(start=start_date, interval="1mo")
        if hist.empty:
            return None
        hist = hist[['Close']].dropna()
        hist.index = hist.index.tz_localize(None)
        # Normalize to first of month for matching
        hist.index = hist.index.to_period('M').to_timestamp()
        hist = hist.rename(columns={'Close': 'Yahoo_Close'})
        return hist
    except Exception as e:
        print(f"  Yahoo fetch failed for {symbol}: {e}")
        return None


def compare_symbol(symbol, darwinex_df):
    """Compare a single symbol's Darwinex data against Yahoo Finance."""
    dwx = darwinex_df[darwinex_df['Symbol'] == symbol].copy()
    dwx = dwx.sort_values('Date')

    if len(dwx) < MIN_BARS_REQUIRED:
        return None

    start_date = dwx['Date'].min().strftime('%Y-%m-%d')
    yahoo = get_yahoo_monthly(symbol, start_date)

    if yahoo is None or len(yahoo) < MIN_BARS_REQUIRED:
        return None

    # Normalize Darwinex dates to first of month for matching
    dwx['Month'] = dwx['Date'].dt.to_period('M').dt.to_timestamp()
    dwx = dwx.set_index('Month')

    # Merge on month
    merged = dwx[['Close']].join(yahoo, how='inner', lsuffix='_DWX')
    merged = merged.rename(columns={'Close': 'DWX_Close'})
    merged = merged.dropna()

    if len(merged) < 6:
        return None

    # Calculate ratio (Darwinex / Yahoo)
    merged['Ratio'] = merged['DWX_Close'] / merged['Yahoo_Close']

    # Use the most recent 6 months as baseline
    recent_ratio = merged['Ratio'].tail(6).median()
    if recent_ratio == 0:
        return None

    # Normalized ratio (should be ~1.0 if data matches throughout)
    merged['Norm_Ratio'] = merged['Ratio'] / recent_ratio

    # Flag months where normalized ratio deviates significantly
    merged['Anomaly'] = abs(merged['Norm_Ratio'] - 1.0) > RATIO_THRESHOLD
    anomaly_count = merged['Anomaly'].sum()

    if anomaly_count == 0:
        return None

    # Find the first anomalous month (likely the corporate action date)
    first_anomaly = merged[merged['Anomaly']].index[0]
    worst_ratio = merged.loc[merged['Anomaly'], 'Norm_Ratio'].iloc[0]
    anomaly_pct = abs(worst_ratio - 1.0) * 100

    return {
        'Symbol': symbol,
        'Total_Months': len(merged),
        'Anomalous_Months': int(anomaly_count),
        'First_Anomaly_Date': first_anomaly.strftime('%Y-%m'),
        'DWX_Close_At_Anomaly': round(merged.loc[first_anomaly, 'DWX_Close'], 4),
        'Yahoo_Close_At_Anomaly': round(merged.loc[first_anomaly, 'Yahoo_Close'], 4),
        'Deviation_Pct': round(anomaly_pct, 1),
        'Recent_Ratio': round(recent_ratio, 4),
        'Anomaly_Ratio': round(worst_ratio, 4),
        'Sector': dwx['Sector'].iloc[0] if 'Sector' in dwx.columns else '',
        'Industry': dwx['Industry'].iloc[0] if 'Industry' in dwx.columns else '',
    }


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 CompareChartData.py <MonthlyOHLC-export.csv>")
        sys.exit(1)

    csv_path = sys.argv[1]
    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}")
        sys.exit(1)

    print(f"Loading Darwinex data from {csv_path}...")
    try:
        darwinex_df = load_darwinex_data(csv_path)
    except Exception as e:
        print(f"Error loading CSV: {e}")
        sys.exit(1)

    if darwinex_df.empty:
        print("No data found in CSV")
        sys.exit(1)

    if 'Symbol' not in darwinex_df.columns:
        print("Error: 'Symbol' column not found in CSV")
        sys.exit(1)

    symbols = darwinex_df['Symbol'].unique()
    print(f"Found {len(symbols)} symbols to compare against Yahoo Finance.\n")

    results = []
    for idx, symbol in enumerate(symbols):
        pct = (idx + 1) / len(symbols) * 100
        print(f"[{idx+1}/{len(symbols)}] ({pct:.0f}%) Checking {symbol}...", end='')

        result = compare_symbol(symbol, darwinex_df)
        if result:
            print(f" ANOMALY: {result['Deviation_Pct']}% deviation from {result['First_Anomaly_Date']}")
            results.append(result)
        else:
            print(" OK")

    # Generate report
    if results:
        report_df = pd.DataFrame(results)
        report_df = report_df.sort_values('Deviation_Pct', ascending=False)

        base = os.path.splitext(csv_path)[0]
        report_path = f"{base}-Anomalies.csv"
        try:
            report_df.to_csv(report_path, index=False)
        except OSError as e:
            print(f"Error writing report: {e}")
            sys.exit(1)

        print(f"\n{'='*70}")
        print(f"ANOMALY REPORT: {len(results)} symbols with data discrepancies")
        print(f"{'='*70}")
        print(report_df[['Symbol', 'First_Anomaly_Date', 'Deviation_Pct', 'Anomalous_Months', 'Sector']].to_string(index=False))
        print(f"\nFull report saved to: {report_path}")
    else:
        print(f"\nNo anomalies found across {len(symbols)} symbols.")


if __name__ == '__main__':
    main()
