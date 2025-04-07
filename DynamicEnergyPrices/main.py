import requests
import json
from datetime import datetime, timedelta
import urllib.parse
import os
import csv

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the full path to config.json
config_path = os.path.join(script_dir, "config.json")

# Load configuration from config.json
with open(config_path, "r") as config_file:
    config = json.load(config_file)

# Configuration variables
HOME_ASSISTANT_API_URL = config["HOME_ASSISTANT_API_URL"]
HOME_ASSISTANT_API_TOKEN = config["HOME_ASSISTANT_API_TOKEN"]
DYNAMIC_PRICES_API_URL = config["DYNAMIC_PRICES_API_URL"]
DYNAMIC_PRICES_API_KEY = config["DYNAMIC_PRICES_API_KEY"]
START_DATE = config["START_DATE"]
END_DATE = config["END_DATE"]
CONSUMPTION_SENSORS = config["CONSUMPTION_SENSORS"]
PRODUCTION_SENSORS = config["PRODUCTION_SENSORS"]
VICTORIAMETRICS_URL = config["VICTORIAMETRICS_URL"]

# Load taxes
TAXES = config["TAXES"]
ENERGY_TAX = TAXES["ENERGY_TAX"]  # Energy tax per kWh (in euro)
STORAGE_COSTS = TAXES["STORAGE_COSTS"]  # Storage costs per kWh (in euro)
STORAGE_COSTS_PRODUCTION = TAXES["STORAGE_COSTS_PRODUCTION"]  # Storage costs for production (in euro)
VAT = TAXES["VAT"]  # VAT percentage
FIXED_SUPPLY_COSTS = TAXES["FIXED_SUPPLY_COSTS"]  # Fixed supply costs per month (in euro)
TRANSPORT_COSTS = TAXES["TRANSPORT_COSTS"]  # Transport costs per month (in euro)
ENERGY_TAX_COMPENSATION = TAXES["ENERGY_TAX_COMPENSATION"]  # Energy tax compensation per month (in euro)

# Load debug setting
DEBUG = config.get("DEBUG", False)

# Load production stop setting
STOP_PRODUCTION_NEGATIVE_PRICES = config.get("STOP_PRODUCTION_NEGATIVE_PRICES", False)

def debug_print(message):
    """Print debug messages if DEBUG is enabled."""
    if DEBUG:
        print(message)

def fetch_sensor_data_victoriametrics(sensor_ids, start_date, end_date, output_file):
    """Fetch historical sensor data from VictoriaMetrics and save combined raw data to a JSON file."""
    # Convert start_date and end_date to Unix timestamps (VictoriaMetrics requires this format)
    start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ").timestamp())
    end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ").timestamp())

    combined_data = []  # List to store combined raw data for all sensors
    hourly_totals = {}  # Dictionary to store merged hourly totals

    for sensor_id in sensor_ids:
        # Construct the PromQL query to include the last data point before the start date
        query = f'{sensor_id}[1h]'

        # Construct the request parameters
        params = {
            "query": query,
            "start": start_timestamp - 3600 * 24 * 30,  # Include one month before the start date
            "end": end_timestamp,
            "step": "3600s"  # Step size of 1 hour
        }

        # Print the query and parameters for debugging
        debug_print(f"Testing query for {sensor_id}:")
        debug_print(f"Query: {query}")
        debug_print(f"Full URL: {VICTORIAMETRICS_URL}?{urllib.parse.urlencode(params)}")

        # Make the request to the VictoriaMetrics API
        response = requests.get(VICTORIAMETRICS_URL, params=params)

        # Check the response
        if response.status_code == 200:
            # Parse the response and merge the data
            parsed_data, original_totals = parse_victoriametrics_response_with_originals(
                response.json(), start_date, end_date
            )
            debug_print(f"Number of items fetched for {sensor_id}: {len(parsed_data)}")  # Print the count of items

            # Combine parsed data into a single list
            for timestamp, value in parsed_data.items():
                combined_data.append({
                    "timestamp": timestamp,
                    "kWh": value,
                    "total_kWh": original_totals.get(timestamp, 0),  # Add the original total kWh value
                    "sensor": sensor_id
                })

            # Merge parsed data into hourly_totals
            for timestamp, value in parsed_data.items():
                if timestamp not in hourly_totals:
                    hourly_totals[timestamp] = 0
                hourly_totals[timestamp] += value
        else:
            debug_print(f"Failed to fetch data for {sensor_id}: {response.status_code}, {response.text}")

    # Sort combined data by timestamp in ascending order
    combined_data.sort(key=lambda x: x["timestamp"])

    # Write combined data to a JSON file
    try:
        with open(output_file, "w") as file:
            json.dump(combined_data, file, indent=4)
        debug_print(f"Combined raw data written to {output_file}")
    except IOError as e:
        debug_print(f"Failed to write combined raw data to {output_file}: {e}")

    # Sort hourly_totals by timestamp (key) in ascending order
    sorted_hourly_totals = dict(sorted(hourly_totals.items()))
    return sorted_hourly_totals

def parse_victoriametrics_response_with_originals(response_json, start_date, end_date):
    """Parse the VictoriaMetrics response and calculate hourly differences, including one record before the start date."""
    hourly_totals = {}
    original_totals = {}

    # Convert start_date and end_date to datetime objects
    start_datetime = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
    end_datetime = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")

    try:
        results = response_json.get("data", {}).get("result", [])
        for result in results:
            metric = result.get("metric", {})
            values = result.get("values", [])  # List of [timestamp, value] pairs

            # Track the previous value for this sensor
            previous_value = 0
            last_record_before_start = None

            for timestamp, value in values:
                try:
                    current_value = float(value)  # Convert value to float
                    record_datetime = datetime.utcfromtimestamp(int(timestamp))

                    # Convert the timestamp to ISO format (YYYY-MM-DDTHH)
                    hour_timestamp = record_datetime.strftime("%Y-%m-%dT%H")

                    # Store the original cumulative kWh value
                    original_totals[hour_timestamp] = current_value

                    # Check if this record is before the start date
                    if record_datetime < start_datetime:
                        last_record_before_start = (hour_timestamp, current_value)
                        continue

                    # If we have a record before the start date, use it as the previous value
                    if last_record_before_start:
                        previous_value = last_record_before_start[1]
                        last_record_before_start = None  # Reset after using it

                    # Calculate the hourly difference
                    hourly_difference = max(0, current_value - previous_value)  # Ensure no negative values
                    previous_value = current_value  # Update the previous value

                    # Accumulate the hourly difference for this timestamp
                    hourly_totals[hour_timestamp] = hourly_difference
                except ValueError:
                    continue  # Skip invalid values
    except KeyError:
        print("Unexpected response format from VictoriaMetrics")

    # Filter out records outside the start and end date range
    filtered_hourly_totals = {
        timestamp: value
        for timestamp, value in hourly_totals.items()
        if start_datetime <= datetime.strptime(timestamp, "%Y-%m-%dT%H") < end_datetime
    }
    filtered_original_totals = {
        timestamp: value
        for timestamp, value in original_totals.items()
        if start_datetime <= datetime.strptime(timestamp, "%Y-%m-%dT%H") < end_datetime
    }

    return filtered_hourly_totals, filtered_original_totals

def fetch_dynamic_prices(start_date, end_date):
    """Fetch dynamic energy prices for the given date range, handling multiple years and caching."""
    # Parse the start and end years
    start_year = datetime.strptime(start_date, "%Y-%m-%d").year
    end_year = datetime.strptime(end_date, "%Y-%m-%d").year
    current_date = datetime.now().date()

    # Initialize an empty list to store price data
    combined_price_data = []

    # Loop through each year in the range
    for year in range(start_year, end_year + 1):
        cache_file = f"./dynamic_energy_prices_{year}.json"  # Cache file for the year

        # Determine if the year is in the past or the current year
        if year < current_date.year:
            # For past years, always use the cached data if available
            if os.path.exists(cache_file):
                try:
                    with open(cache_file, "r") as file:
                        cached_data = json.load(file)
                        debug_print(f"Using cached dynamic prices for year {year}")
                        combined_price_data.extend(normalize_price_data(cached_data))
                        continue
                except (json.JSONDecodeError, IOError):
                    debug_print(f"Failed to read cache file for year {year}, fetching from API...")

        elif year == current_date.year:
            # For the current year, check if the cache is up-to-date (download once per day)
            if os.path.exists(cache_file):
                last_modified = datetime.fromtimestamp(os.path.getmtime(cache_file)).date()
                if last_modified == current_date:
                    try:
                        with open(cache_file, "r") as file:
                            cached_data = json.load(file)
                            debug_print(f"Using cached dynamic prices for year {year} (up-to-date)")
                            combined_price_data.extend(normalize_price_data(cached_data))
                            continue
                    except (json.JSONDecodeError, IOError):
                        debug_print(f"Failed to read cache file for year {year}, fetching from API...")

        # Fetch data from the API if cache is not available or outdated
        url = f"{DYNAMIC_PRICES_API_URL}?period=jaar&year={year}&type=json&key={DYNAMIC_PRICES_API_KEY}"
        debug_print(f"Fetching dynamic prices from: {url}")
        response = requests.get(url)
        if response.status_code == 200:
            price_data = json.loads(response.text)

            # Save the fetched data to the cache file
            try:
                with open(cache_file, "w") as file:
                    json.dump(price_data, file)
                    print(f"Cached dynamic prices for year {year}")
            except IOError:
                print(f"Failed to write cache file for year {year}")

            combined_price_data.extend(normalize_price_data(price_data))
        else:
            debug_print(f"Failed to fetch dynamic prices from API for year {year}: {response.status_code}")

    return combined_price_data

def normalize_price_data(price_data):
    """Normalize the timestamps in price_data to the format YYYY-MM-DDTHH."""
    normalized_data = []
    for entry in price_data:
        try:
            # Parse the timestamp and reformat it to YYYY-MM-DDTHH
            original_timestamp = entry["datum"]
            # Handle both formats: with 'T' or with a space
            if "T" in original_timestamp:
                normalized_timestamp = datetime.strptime(original_timestamp, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%dT%H")
            else:
                normalized_timestamp = datetime.strptime(original_timestamp, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%dT%H")
            entry["datum"] = normalized_timestamp
            normalized_data.append(entry)
        except (KeyError, ValueError) as e:
            debug_print(f"Invalid price entry: {entry}, Error: {e}")
    return normalized_data

def process_sensor_data(sensor_data):
    """Process sensor data to calculate hourly totals."""
    hourly_totals = {}

    # The sensor_data is a list containing another list of records
    for sensor_records in sensor_data:
        for record in sensor_records:
            # Skip records with invalid or unavailable states
            try:
                state = float(record.get("state", 0))  # Use .get() to avoid KeyError
            except ValueError:
                continue

            # Extract the hour (YYYY-MM-DDTHH) from the last_changed timestamp
            timestamp = record.get("last_changed", "")[:13]  # Ensure format matches price_data
            if not timestamp:
                continue

            # Add the state to the hourly total
            if timestamp not in hourly_totals:
                hourly_totals[timestamp] = 0
            hourly_totals[timestamp] += state

    return hourly_totals

def calculate_costs(consumption_data, production_data, price_data):
    """Calculate energy costs, income, and total consumption/production, with monthly breakdowns."""
    costs = 0
    income = 0
    total_consumption = 0
    total_production = 0

    # Monthly breakdowns
    monthly_breakdown = {}

    # Convert START_DATE and END_DATE to datetime objects
    start_datetime = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_datetime = datetime.strptime(END_DATE, "%Y-%m-%d") + timedelta(days=1)

    debug_print("Debugging calculate_costs:")
    for price_entry in price_data:
        # Extract the timestamp and base price
        timestamp_str = price_entry["datum"]
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H")

        # Skip entries outside the start and end date range
        if not (start_datetime <= timestamp < end_datetime):
            continue

        # Extract the base price (purchase price excluding VAT)
        purchase_price_excl_vat = float(price_entry["prijs_excl_belastingen"].replace(",", "."))

        # Calculate the total price for consumption (including storage costs, energy tax, and VAT)
        total_price_excl_vat_consumption = purchase_price_excl_vat + STORAGE_COSTS + ENERGY_TAX
        total_price_incl_vat_consumption = total_price_excl_vat_consumption * (1 + VAT / 100)

        # Calculate the total price for production (including storage costs for production)
        total_price_excl_vat_production = purchase_price_excl_vat + ENERGY_TAX + STORAGE_COSTS_PRODUCTION
        total_price_incl_vat_production = total_price_excl_vat_production * (1 + VAT / 100)

        # Get hourly consumption and production values for the timestamp
        hourly_consumption = consumption_data.get(timestamp_str, 0)
        hourly_production = production_data.get(timestamp_str, 0)

        # Check if production should be stopped for negative prices
        if STOP_PRODUCTION_NEGATIVE_PRICES and total_price_incl_vat_production < 0:
            debug_print(f"Negative price detected at {timestamp_str}: {total_price_incl_vat_production:.2f}. Stopping production.")
            hourly_production = 0  # Stop production for this hour

        # Accumulate total consumption and production
        total_consumption += hourly_consumption
        total_production += hourly_production

        # Accumulate costs and income
        costs += hourly_consumption * total_price_incl_vat_consumption
        income += hourly_production * total_price_incl_vat_production

        # Calculate the month key (e.g., "2024-12")
        month_key = timestamp.strftime("%Y-%m")

        # Initialize monthly breakdown if not already present
        if month_key not in monthly_breakdown:
            monthly_breakdown[month_key] = {
                "costs": 0,
                "income": 0,
                "consumption": 0,
                "production": 0,
                "fixed_supply_costs": FIXED_SUPPLY_COSTS,
                "transport_costs": TRANSPORT_COSTS,
                "energy_tax_compensation": ENERGY_TAX_COMPENSATION
            }

        # Update monthly breakdown
        monthly_breakdown[month_key]["costs"] += hourly_consumption * total_price_incl_vat_consumption
        monthly_breakdown[month_key]["income"] += hourly_production * total_price_incl_vat_production
        monthly_breakdown[month_key]["consumption"] += hourly_consumption
        monthly_breakdown[month_key]["production"] += hourly_production

    # Add fixed monthly costs to the total costs
    for month, data in monthly_breakdown.items():
        data["costs"] += data["fixed_supply_costs"] + data["transport_costs"] + data["energy_tax_compensation"]
        costs += data["fixed_supply_costs"] + data["transport_costs"] + data["energy_tax_compensation"]

    # Debugging: Print the final totals
    debug_print(f"Total Costs: {costs}, Total Income: {income}")
    debug_print(f"Total Consumption: {total_consumption}, Total Production: {total_production}")

    return costs, income, total_consumption, total_production, monthly_breakdown

def write_results_to_csv(total_costs, total_income, total_consumption, total_production, monthly_breakdown):
    """Write the results to a CSV file."""
    # Ensure the results folder exists
    results_folder = "results"
    os.makedirs(results_folder, exist_ok=True)

    # Create a timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = os.path.join(results_folder, f"results_{timestamp}.csv")

    # Write the results to the CSV file
    with open(csv_filename, mode="w", newline="") as csv_file:
        writer = csv.writer(csv_file)

        # Write the header
        writer.writerow(["Metric", "Value"])
        writer.writerow(["Total Costs (€)", f"{total_costs:.2f}"])
        writer.writerow(["Total Income (€)", f"{total_income:.2f}"])
        writer.writerow(["Total Consumption (kWh)", f"{total_consumption:.2f}"])
        writer.writerow(["Total Production (kWh)", f"{total_production:.2f}"])

        # Add a blank line
        writer.writerow([])

        # Write the monthly breakdown
        writer.writerow([
            "Month", 
            "Costs (€)", 
            "Income (€)", 
            "Consumption (kWh)", 
            "Production (kWh)", 
            "Fixed Supply Costs (€)", 
            "Transport Costs (€)", 
            "Energy Tax Compensation (€)", 
            "Net Monthly Costs (€)"  # New column for net monthly costs
        ])
        for month, data in monthly_breakdown.items():
            # Calculate net monthly costs (costs - income)
            net_monthly_costs = data["costs"] - data["income"]
            writer.writerow([
                month,
                f"{data['costs']:.2f}",
                f"{data['income']:.2f}",
                f"{data['consumption']:.2f}",
                f"{data['production']:.2f}",
                f"{data['fixed_supply_costs']:.2f}",
                f"{data['transport_costs']:.2f}",
                f"{data['energy_tax_compensation']:.2f}",
                f"{net_monthly_costs:.2f}"  # Write the net monthly costs
            ])

    print(f"Results written to {csv_filename}")

def main():
    # Fetch sensor data from VictoriaMetrics
    sensor_start_date = f"{START_DATE}T00:00:00Z"
    sensor_end_date = f"{END_DATE}T23:59:59Z"
    print(f"Fetching consumption data from VictoriaMetrics from {sensor_start_date} to {sensor_end_date}")
    
    # Fetch consumption data and save raw data to a JSON file
    consumption_data = fetch_sensor_data_victoriametrics(
        CONSUMPTION_SENSORS, sensor_start_date, sensor_end_date, "raw_consumption_data.json"
    )
    print("Consumption Data fetched and saved to raw_consumption_data.json.")

    # Fetch production data and save raw data to a JSON file
    production_data = fetch_sensor_data_victoriametrics(
        PRODUCTION_SENSORS, sensor_start_date, sensor_end_date, "raw_production_data.json"
    )
    print("Production Data fetched and saved to raw_production_data.json.")

    # Fetch dynamic prices
    price_data = fetch_dynamic_prices(START_DATE, END_DATE)

    # Calculate costs, income, and totals
    total_costs, total_income, total_consumption, total_production, monthly_breakdown = calculate_costs(
        consumption_data, production_data, price_data
    )

    # Write results to a CSV file
    write_results_to_csv(total_costs, total_income, total_consumption, total_production, monthly_breakdown)

if __name__ == "__main__":
    main()