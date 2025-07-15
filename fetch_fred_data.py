import requests
import pandas as pd
from datetime import datetime, date
import os
from dotenv import load_dotenv

def get_fred_series_and_export_csv(series_ids_dict, api_key, start_date='1900-01-01'):
    """
    Retrieves economic data from the FRED API for multiple series and exports each to a CSV file.

    Args:
        series_ids_dict (dict): A dictionary where keys are descriptive names (e.g., "Unemployment Rate")
                                and values are FRED series IDs (e.g., "UNRATE").
        api_key (str): Your FRED API key. Obtain one from https://fred.stlouisfed.org/docs/api/api_key.html.
        start_date (str): The start date for data retrieval in 'YYYY-MM-DD' format.
                          Defaults to '1900-01-01'.
    """
    base_url = "https://api.stlouisfed.org/fred/series/observations"
    end_date = date.today().strftime('%Y-%m-%d')

    data_folder = "data"

    total_nas_found = 0

    for name, series_id in series_ids_dict.items():
        print(f"Attempting to retrieve data for series: '{name}' (ID: {series_id})")

        # Construct the full API URL with parameters
        api_url = (
            f"{base_url}"
            f"?series_id={series_id}"
            f"&api_key={api_key}"
            f"&file_type=json"
            f"&observation_start={start_date}"
            f"&observation_end={end_date}"
        )

        try:
            response = requests.get(api_url)
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
            data = response.json()

            if 'observations' not in data or not data['observations']:
                print(f"No observations found for series ID: {series_id} or response structure unexpected.")
                continue

            df = pd.DataFrame(data['observations'])

            # Drop 'realtime_start' and 'realtime_end' columns
            columns_to_drop = ['realtime_start', 'realtime_end']

            # Check if columns exist before dropping to prevent errors
            df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

            # Clean and convert data types
            df['date'] = pd.to_datetime(df['date'])
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            nas_in_current_series = df['value'].isna().sum()
            total_nas_found += nas_in_current_series
            if nas_in_current_series > 0:
                print(f"  Found {nas_in_current_series} NA values for series: {series_id}")
            df = df.rename(columns={'value': series_id})

            if df.empty:
                print(f"No valid numeric observations found for series ID: {series_id} after cleaning.")
                continue
            filename = f"{data_folder}/{name.lower().replace(' ', '_')}.csv"
            df.to_csv(filename, index=False)
            print(f"Successfully retrieved and saved {len(df)} observations to {filename}")

        except requests.exceptions.RequestException as e:
            print(f"Error making API request for '{series_id}': {e}")
        except ValueError as e:
            print(f"Error parsing JSON or processing data for '{series_id}': {e}")
        except Exception as e:
            print(f"An unexpected error occurred for '{series_id}': {e}")

    print(f"\n--- Data Fetching Process Completed ---")
    print(f"Total number of NA values found across all fetched series: {total_nas_found}")


# --- Example Usage ---
# Load environment variables from .env file
load_dotenv()

# Retrieve FRED API key from environment variable
fred_api_key = os.getenv("FRED_API_KEY")

if fred_api_key is None:
    print("Error: FRED_API_KEY not found in environment variables.")
    print("Please create a .env file in the same directory as this script with the content:")
    print("FRED_API_KEY=YOUR_ACTUAL_FRED_API_KEY")
    print("You can get one for free at: https://fred.stlouisfed.org/docs/api/api_key.html")
else:
    # Define the dictionary of series IDs you want to retrieve
    fred_series_to_fetch = {
        # I. Inflation Measures
        "PCE Price Index": "PCEPI",
        "Core PCE Price Index": "PCEPILFE",
        "CPI All Urban Consumers": "CPIAUCSL",
        "Core CPI All Urban Consumers": "CPILFESL",
        "PPI Final Demand Goods": "PPIFGS",
        "Employment Cost Index Total Compensation Private Industry": "CIU2010000000000I",
        "Average Hourly Earnings Total Private": "CEU0500000003",
        "Unit Labor Costs Nonfarm Business Sector": "ULCNFB",
        "Michigan Consumer Sentiment Index": "MICH",
        "5-Year Breakeven Inflation Rate": "T5YIE",
        "10-Year Breakeven Inflation Rate": "T10YIE",

        # II. Labor Market Indicators
        "Unemployment Rate U3": "UNRATE",
        "Unemployment Rate U6": "U6RATE",
        "Nonfarm Payrolls": "PAYEMS",
        "Labor Force Participation Rate": "CIVPART",
        "JOLTS Job Openings": "JTSJOL",
        "JOLTS Quits Rate": "JTSQUR",
        "Initial Jobless Claims Seasonally Adjusted": "ICNSA",

        # III. Real Activity and Growth
        "Real Gross Domestic Product": "GDPC1",
        "Industrial Production Index": "INDPRO",
        "Retail Sales Total Excluding Food Services": "RSXFS",
        "Housing Starts Total New Privately Owned": "HOUST",
        "Existing Home Sales Total": "EXHOSLUSM495S",
        "Case-Shiller US National Home Price Index": "CSUSHPINSA",
        "Real Private Nonresidential Fixed Investment": "PNFI",
        "New Orders for Durable Goods": "NEWORDER",
        "Capacity Utilization Manufacturing": "CUMFNS",

        # IV. Financial Market Indicators
        "Effective Federal Funds Rate": "DFF",
        "3-Month Treasury Constant Maturity Rate": "DGS3MO",
        "10-Year Treasury Constant Maturity Rate": "DGS10",
        "ICE BofA US High Yield Master II Option Adjusted Spread": "BAMLH0A0HYM2",
        "30-Year Fixed Rate Mortgage Average": "MORTGAGE30US",
        "S&P 500 Index": "SP500",
        "Trade Weighted US Dollar Index Broad Goods and Services": "DTWEXBGS",
        "M2 Money Stock": "M2SL",

        # V. Expectations and Sentiment
        "Consumer Confidence Index": "CSCICP03USM665S",

        # VI. Global Economic Conditions
        "Crude Oil Prices West Texas Intermediate": "DCOILWTICO",
        "All Commodities Price Index IMF": "PALLFNFINDEXM",

        # VII. Other Contextual Data
        "Federal Surplus or Deficit": "FYFSD",
        "Federal Debt Total Public Debt": "GFDEBTN",
        "Federal Funds Effective Rate": "FEDFUNDS",
    }

    # Set the desired start date
    desired_start_date = "1990-01-01"

    # Call the function to retrieve and export data
    get_fred_series_and_export_csv(
        series_ids_dict=fred_series_to_fetch,
        api_key=fred_api_key,
        start_date=desired_start_date
    )

    print("\nData retrieval and export process completed.")
