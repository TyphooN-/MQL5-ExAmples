#!/usr/bin/env python3
"""
History Integrity Checker — compares broker MN1 ATH against Yahoo Finance
for ALL symbols on the Darwinex stocks server.

Usage:
    pip install yfinance pandas
    python check_history_integrity.py HistoryCheck-ServerName-2026.03.16.csv

Reads the CSV exported by HistoryIntegrityCheck.mq5 and flags symbols where:
  1. Broker ATH is significantly below Yahoo Finance ATH (bad/truncated history)
  2. Very few MN1 bars for a stock that should have years of data
  3. Zero MN1 bars (no history at all)
"""
import sys
import csv
import time
import re
from datetime import datetime
from pathlib import Path

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    print("Install dependencies: pip install yfinance pandas")
    sys.exit(1)


# Darwinex symbol -> Yahoo ticker mapping overrides
# Add known mismatches here as you discover them
TICKER_MAP = {
    "BRK.B": "BRK-B",
    "BF.B": "BF-B",
    "BF.A": "BF-A",
    # European stocks on Darwinex may need exchange suffixes:
    # "VOD": "VOD.L",  # Vodafone London
    # "SAP": "SAP.DE", # SAP Germany
}

# Symbols to skip (indices, ETFs that won't match, known issues)
SKIP_SYMBOLS = set()

# Minimum expected MN1 bars for a stock to be considered "has history"
MIN_EXPECTED_BARS = 12  # 1 year — anything less is suspect

# ATH tolerance — flag if broker ATH is below this fraction of Yahoo ATH
ATH_TOLERANCE = 0.70  # broker ATH < 70% of Yahoo ATH = definitely suspect

# Yahoo Finance rate limiting
REQUESTS_PER_BATCH = 10
BATCH_SLEEP_SECONDS = 2


def map_ticker(darwinex_symbol: str) -> str:
    """Map Darwinex symbol name to Yahoo Finance ticker."""
    sym = darwinex_symbol.strip()
    if sym in TICKER_MAP:
        return TICKER_MAP[sym]
    # Common Darwinex patterns:
    # 1. US stocks are usually raw ticker: AAPL, MSFT, DD
    # 2. Some have .US suffix on other brokers but not Darwinex
    # 3. Dots in tickers become dashes on Yahoo: BRK.B -> BRK-B
    ticker = sym.replace(".", "-")
    return ticker


def get_yahoo_ath(ticker: str) -> tuple:
    """Fetch all-time high and earliest date from Yahoo Finance.
    Returns (ath, earliest_date_str, total_months, error_msg)."""
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="max", interval="1mo")
        if hist.empty:
            return None, None, 0, f"No Yahoo data"
        ath = float(hist["High"].max())
        earliest = hist.index[0].strftime("%Y-%m-%d")
        months = len(hist)
        return ath, earliest, months, None
    except Exception as e:
        return None, None, 0, str(e)[:60]


def main():
    if len(sys.argv) < 2:
        print("Usage: python check_history_integrity.py <HistoryCheck-*.csv>")
        print("       CSV exported by HistoryIntegrityCheck.mq5 (semicolon-delimited)")
        sys.exit(1)

    csv_path = Path(sys.argv[1])
    if not csv_path.exists():
        print(f"File not found: {csv_path}")
        sys.exit(1)

    # Parse broker CSV (semicolon-delimited)
    symbols = []
    try:
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                symbols.append(row)
    except (OSError, csv.Error) as e:
        print(f"Error reading CSV: {e}")
        sys.exit(1)

    if not symbols:
        print(f"No data rows found in {csv_path.name}")
        sys.exit(1)

    print(f"Loaded {len(symbols)} symbols from {csv_path.name}")
    print(f"Checking against Yahoo Finance (this may take a while)...\n")
    print(f"{'#':>5} {'Status':>6} {'Symbol':<12} {'BrkATH':>10} {'YahATH':>10} "
          f"{'Ratio':>7} {'BrkBars':>8} {'YahMo':>6}")
    print("-" * 80)

    # Results
    bad_history = []
    no_data = []
    good = []
    errors = []
    request_count = 0

    for i, sym in enumerate(symbols):
        darwinex_sym = sym.get("Symbol", "").strip()
        if not darwinex_sym or darwinex_sym in SKIP_SYMBOLS:
            continue

        try:
            broker_ath = float(sym.get("BrokerATH", "0"))
        except ValueError:
            broker_ath = 0.0
        try:
            broker_bars = int(sym.get("MN1Bars", "0"))
        except ValueError:
            broker_bars = 0

        broker_earliest = sym.get("EarliestDate", "").strip()
        sector = sym.get("Sector", "").strip()
        industry = sym.get("Industry", "").strip()
        desc = sym.get("Description", "").strip()

        # Skip symbols with 0 bars — record separately
        if broker_bars == 0:
            no_data.append({
                "symbol": darwinex_sym, "sector": sector,
                "industry": industry, "desc": desc
            })
            print(f"[{i+1:>4}/{len(symbols)}]  NONE {'':>1}{darwinex_sym:<12} "
                  f"{'N/A':>10} {'':>10} {'':>7} {broker_bars:>8}")
            continue

        ticker = map_ticker(darwinex_sym)

        # Rate limiting
        request_count += 1
        if request_count > 1 and request_count % REQUESTS_PER_BATCH == 0:
            time.sleep(BATCH_SLEEP_SECONDS)

        yahoo_ath, yahoo_earliest, yahoo_months, err = get_yahoo_ath(ticker)

        if err:
            errors.append({
                "symbol": darwinex_sym, "ticker": ticker, "error": err,
                "broker_ath": broker_ath, "broker_bars": broker_bars
            })
            print(f"[{i+1:>4}/{len(symbols)}]   ERR {darwinex_sym:<12} "
                  f"{broker_ath:>10.2f} {'ERR':>10} {'':>7} {broker_bars:>8}  {err}")
            continue

        # Check 1: ATH mismatch
        ath_ratio = broker_ath / yahoo_ath if yahoo_ath and yahoo_ath > 0 else 0
        ath_bad = ath_ratio < ATH_TOLERANCE and yahoo_ath > 0

        # Check 2: Low bar count vs Yahoo
        bars_ratio = broker_bars / yahoo_months if yahoo_months > 0 else 0
        bars_bad = broker_bars < MIN_EXPECTED_BARS and yahoo_months >= MIN_EXPECTED_BARS * 2

        is_bad = ath_bad or bars_bad

        if is_bad:
            entry = {
                "symbol": darwinex_sym,
                "ticker": ticker,
                "broker_ath": broker_ath,
                "yahoo_ath": yahoo_ath,
                "ath_ratio": ath_ratio,
                "broker_bars": broker_bars,
                "yahoo_months": yahoo_months,
                "bars_ratio": bars_ratio,
                "broker_earliest": broker_earliest,
                "yahoo_earliest": yahoo_earliest,
                "sector": sector,
                "industry": industry,
                "desc": desc,
                "issues": [],
            }
            if ath_bad:
                entry["issues"].append(
                    f"ATH {broker_ath:.2f} vs {yahoo_ath:.2f} ({ath_ratio:.0%})"
                )
            if bars_bad:
                entry["issues"].append(
                    f"{broker_bars} bars vs {yahoo_months} Yahoo months ({bars_ratio:.0%})"
                )
            bad_history.append(entry)
        else:
            good.append(darwinex_sym)

        status = "  BAD" if is_bad else "   OK"
        print(
            f"[{i+1:>4}/{len(symbols)}]{status} {darwinex_sym:<12} "
            f"{broker_ath:>10.2f} {yahoo_ath:>10.2f} "
            f"{ath_ratio:>6.0%} {broker_bars:>8} {yahoo_months:>6}"
        )

    # ==================== SUMMARY REPORT ====================
    print("\n" + "=" * 80)
    print("DARWINEX HISTORY INTEGRITY REPORT")
    print("=" * 80)

    if bad_history:
        print(f"\n{'!'*60}")
        print(f"  {len(bad_history)} SYMBOLS WITH BAD/TRUNCATED HISTORY")
        print(f"{'!'*60}\n")

        bad_history.sort(key=lambda x: x["ath_ratio"])

        print(f"{'Symbol':<12} {'BrkATH':>10} {'YahATH':>10} {'ATH%':>6} "
              f"{'BrkBr':>6} {'YahMo':>6} {'Issues'}")
        print("-" * 90)
        for b in bad_history:
            issues_str = " | ".join(b["issues"])
            print(
                f"{b['symbol']:<12} {b['broker_ath']:>10.2f} {b['yahoo_ath']:>10.2f} "
                f"{b['ath_ratio']:>5.0%} {b['broker_bars']:>6} {b['yahoo_months']:>6}  "
                f"{issues_str}"
            )

    if no_data:
        print(f"\n{len(no_data)} symbols with ZERO MN1 bars (no history at all):")
        for nd in no_data[:20]:
            print(f"  {nd['symbol']:<12} {nd['desc']}")
        if len(no_data) > 20:
            print(f"  ... and {len(no_data) - 20} more")

    if errors:
        print(f"\n{len(errors)} symbols failed Yahoo lookup:")
        for e in errors[:20]:
            print(f"  {e['symbol']:<12} ({e['ticker']:<12}): {e['error']}")
        if len(errors) > 20:
            print(f"  ... and {len(errors) - 20} more")

    print(f"\n{'='*40}")
    print(f"  OK:        {len(good):>5}")
    print(f"  BAD:       {len(bad_history):>5}")
    print(f"  NO DATA:   {len(no_data):>5}")
    print(f"  ERRORS:    {len(errors):>5}")
    print(f"  TOTAL:     {len(good) + len(bad_history) + len(no_data) + len(errors):>5}")
    print(f"{'='*40}")

    # Write results to CSVs
    stem = csv_path.stem

    if bad_history:
        out_path = f"{stem}-BAD.csv"
        try:
            f = open(out_path, "w", newline="")
        except OSError as e:
            print(f"Error writing {out_path}: {e}")
            sys.exit(1)
        with f:
            writer = csv.writer(f)
            writer.writerow(["Symbol", "Ticker", "BrokerATH", "YahooATH", "ATHRatio",
                             "BrokerBars", "YahooMonths", "BarsRatio",
                             "BrokerEarliest", "YahooEarliest",
                             "Sector", "Industry", "Description", "Issues"])
            for b in bad_history:
                writer.writerow([
                    b["symbol"], b["ticker"],
                    f"{b['broker_ath']:.2f}", f"{b['yahoo_ath']:.2f}",
                    f"{b['ath_ratio']:.2%}",
                    b["broker_bars"], b["yahoo_months"],
                    f"{b['bars_ratio']:.2%}",
                    b["broker_earliest"], b["yahoo_earliest"],
                    b["sector"], b["industry"], b["desc"],
                    " | ".join(b["issues"])
                ])
        print(f"\nBad history: {out_path}")

    if no_data:
        out_path = f"{stem}-NODATA.csv"
        try:
            f = open(out_path, "w", newline="")
        except OSError as e:
            print(f"Error writing {out_path}: {e}")
            sys.exit(1)
        with f:
            writer = csv.writer(f)
            writer.writerow(["Symbol", "Sector", "Industry", "Description"])
            for nd in no_data:
                writer.writerow([nd["symbol"], nd["sector"], nd["industry"], nd["desc"]])
        print(f"No data:     {out_path}")

    if errors:
        out_path = f"{stem}-ERRORS.csv"
        try:
            f = open(out_path, "w", newline="")
        except OSError as e:
            print(f"Error writing {out_path}: {e}")
            sys.exit(1)
        with f:
            writer = csv.writer(f)
            writer.writerow(["Symbol", "Ticker", "BrokerATH", "BrokerBars", "Error"])
            for e in errors:
                writer.writerow([e["symbol"], e["ticker"],
                                 f"{e['broker_ath']:.2f}", e["broker_bars"], e["error"]])
        print(f"Errors:      {out_path}")


if __name__ == "__main__":
    main()
