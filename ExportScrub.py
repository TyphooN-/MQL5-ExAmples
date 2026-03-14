#!/usr/bin/env python3
"""
ExportScrub.py — Enrich ExportSymbols CSV with Yahoo Finance monthly volume data.

Usage:
    python3 ExportScrub.py [input.csv]

If no argument is given, prompts for the filename interactively.
Output: input-YAPI_Volume.csv with a Volume column added.
"""

import sys
import os

import pandas as pd
import yfinance as yf


def fetch_monthly_volumes(symbols):
    """
    Fetch the latest monthly trading volume for multiple symbols.
    Args:
        symbols (list): List of ticker symbols.
    Returns:
        dict: Dictionary mapping symbol to its latest monthly volume, or None if unavailable.
    """
    try:
        # Fetch data for all symbols at once
        data = yf.download(symbols, period="1mo")
        # Initialize a dictionary to store results
        volumes = {}
        # Extract the latest volume for each symbol
        for symbol in symbols:
            if symbol in data['Volume']:
                vol_df = data['Volume'][symbol]
                if not vol_df.empty:
                    volumes[symbol] = vol_df.iloc[-1]
                else:
                    volumes[symbol] = None
            else:
                volumes[symbol] = None
        return volumes
    except Exception as e:
        print(f"Error fetching data for symbols: {e}")
        return None


def main(input_csv_path, output_csv_path):
    """
    Read the CSV file, fetch monthly volumes for all symbols, and save the updated CSV.
    Args:
        input_csv_path (str): Path to your input CSV file.
        output_csv_path (str): Path to your output CSV file.
    """
    try:
        # Specify the correct delimiter handling
        df = pd.read_csv(input_csv_path, sep=';', on_bad_lines='skip')
        print("Successfully read the input CSV file.")
    except Exception as e:
        print(f"Error reading the CSV file: {e}")
        return
    # Ensure 'Symbol' column exists
    if 'Symbol' not in df.columns:
        print("Error: 'Symbol' column not found in the CSV.")
        return
    # Extract all symbols from the dataframe
    symbols = df['Symbol'].unique().tolist()
    # Initialize a new column for Volume if it doesn't exist
    if 'Volume' not in df.columns:
        df['Volume'] = None
    print(f"Fetching volumes for {len(symbols)} symbols...")
    try:
        volumes = fetch_monthly_volumes(symbols)
        if volumes is not None:
            df['Volume'] = df['Symbol'].map(volumes)
    except Exception as e:
        print(f"Error processing symbols: {str(e)}")
    # Save the updated dataframe to a new CSV file
    try:
        df.to_csv(output_csv_path, index=False)
        print(f"Updated volumes saved successfully to: {output_csv_path}")
    except Exception as e:
        print(f"Error saving the updated data: {e}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        input_filename = sys.argv[1]
    else:
        input_filename = input("Please enter your CSV filename (including .csv extension): ")

    if not os.path.exists(input_filename):
        print(f"File not found: {input_filename}")
        sys.exit(1)

    base, extension = os.path.splitext(input_filename)
    output_filename = f"{base}-YAPI_Volume{extension}"
    main(input_filename, output_filename)
