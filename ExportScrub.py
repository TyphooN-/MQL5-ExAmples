import pandas as pd
import yfinance as yf
def fetch_monthly_volume(symbol):
    """
    Fetch the latest monthly trading volume for a given symbol.
    Returns:
        float: The latest monthly volume or None if unavailable.
    """
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.history(period="1mo")  # Fetch 1 month of data
        if not data.empty:
            return data['Volume'].iloc[-1]  # Get the latest volume
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
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
        df = pd.read_csv(input_csv_path, engine='python', sep='[;]', on_bad_lines='skip')
        print("Successfully read the input CSV file.")
    except Exception as e:
        print(f"Error reading the CSV file: {e}")
        return
    # Ensure 'Symbol' column exists
    if 'Symbol' not in df.columns:
        print("Error: 'Symbol' column not found in the CSV.")
        return
    # Initialize a new column for Volume if it doesn't exist
    if 'Volume' not in df.columns:
        df['Volume'] = None
    # Fetch volume for each symbol
    for index, row in df.iterrows():
        symbol = row['Symbol']
        print(f"Fetching volume for {symbol}...")
        try:
            volume = fetch_monthly_volume(symbol)
            if volume is not None:
                df.loc[index, 'Volume'] = volume
            else:
                print(f"No volume data available for {symbol}.")
        except Exception as e:
            print(f"Error processing symbol {symbol}: {str(e)}")
    # Save the updated dataframe to a new CSV file
    try:
        df.to_csv(output_csv_path, index=False)
        print(f"Updated volumes saved successfully to: {output_csv_path}")
    except Exception as e:
        print(f"Error saving the updated data: {e}")
if __name__ == "__main__":
    # Get input filename from user
    input_filename = input("Please enter your CSV filename (including .csv extension): ")
    # Generate output filename
    base, extension = input_filename.rsplit('.', 1)
    output_filename = f"{base}-YAPI_Volume.{extension}"
    main(input_filename, output_filename)
