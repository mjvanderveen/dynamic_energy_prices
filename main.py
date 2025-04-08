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

def simulate_battery(hourly_consumption, hourly_production, battery_state, config, total_price_incl_vat_production, timestamp, strategy):
    """
    Simulate the behavior of a battery for a single hour based on the chosen strategy.

    Args:
        hourly_consumption (float): The energy consumption for the hour (kWh).
        hourly_production (float): The energy production for the hour (kWh).
        battery_state (dict): The current state of the battery.
        config (dict): The battery configuration.
        total_price_incl_vat_production (float): The energy price for production (including taxes).
        timestamp (str): The timestamp for the current hour (format: YYYY-MM-DDTHH).
        strategy (str): The battery charge strategy ("self-sufficiency" or "dynamic_cost_optimization").

    Returns:
        tuple: Adjusted consumption, adjusted production, updated battery state.
    """
    # Extract battery parameters from the config
    battery_size = config["BATTERY_SIMULATION"]["BATTERY_SIZE_KWH"]
    max_charging_rate = config["BATTERY_SIMULATION"]["MAX_CHARGING_RATE_KWH"]
    max_discharging_rate = config["BATTERY_SIMULATION"]["MAX_DISCHARGING_RATE_KWH"]
    round_trip_efficiency = config["BATTERY_SIMULATION"].get("ROUND_TRIP_EFFICIENCY", 0.96)
    discharge_limit = (config["BATTERY_SIMULATION"].get("DISCHARGE_LIMIT_PERCENTAGE", 10) / 100) * battery_size

    # Get the current battery level
    battery_level = battery_state["level"]

    # Initialize charge/discharge tracking
    charge_amount = 0
    discharge_amount = 0

    if strategy == "self-sufficiency":
        # Self-Sufficiency Strategy
        # Adjust production by charging the battery
        if hourly_production > 0:
            charge_amount = min(hourly_production, max_charging_rate, battery_size - battery_level)
            if charge_amount > 0:
                battery_level += charge_amount * round_trip_efficiency
                hourly_production -= charge_amount

        # Adjust consumption by discharging the battery
        if hourly_consumption > 0:
            discharge_amount = min(hourly_consumption, max_discharging_rate, battery_level - discharge_limit)
            if discharge_amount > 0:
                battery_level -= discharge_amount
                hourly_consumption -= discharge_amount

    elif strategy == "dynamic_cost_optimization":
        # Dynamic Cost Optimization Strategy
        # Charge the battery when prices are low
        if total_price_incl_vat_production < config["BATTERY_SIMULATION"].get("DYNAMIC_PRICE_THRESHOLD_LOW", 0.10):
            charge_amount = min(max_charging_rate, battery_size - battery_level)
            if charge_amount > 0:
                battery_level += charge_amount * round_trip_efficiency

        # Discharge the battery when prices are high
        elif total_price_incl_vat_production > config["BATTERY_SIMULATION"].get("DYNAMIC_PRICE_THRESHOLD_HIGH", 0.25):
            discharge_amount = min(max_discharging_rate, battery_level - discharge_limit)
            if discharge_amount > 0:
                battery_level -= discharge_amount
                hourly_consumption -= discharge_amount

    # Update the battery state
    battery_state["level"] = battery_level
    battery_state["total_charged"] += charge_amount
    battery_state["total_discharged"] += discharge_amount

    return hourly_consumption, hourly_production, battery_state

def calculate_costs(consumption_data, production_data, price_data):
    """Calculate energy costs, income, and total consumption/production, with battery simulation."""
    costs = 0
    income = 0
    total_consumption = 0
    total_production = 0

    # Battery simulation variables
    battery_enabled = config["BATTERY_SIMULATION"]["ENABLE"]
    battery_state = {
        "level": config["BATTERY_SIMULATION"].get("DISCHARGE_LIMIT", 0.1) * config["BATTERY_SIMULATION"]["BATTERY_SIZE_KWH"],
        "total_charged": 0,
        "total_discharged": 0
    }

    # Get the battery charge strategy
    strategy = config["BATTERY_SIMULATION"].get("BATTERY_CHARGE_STRATEGY", "self-sufficiency")


    # Daily charge/discharge tracking
    daily_discharge = {}
    daily_charge = 0  # Track the total charge for the current day
    daily_discharge_total = 0  # Track the total discharge for the current day
    current_day = None

    # Monthly breakdowns
    monthly_breakdown = {}
    battery_adjusted_costs = 0
    battery_adjusted_income = 0
    total_battery_consumption = 0
    total_battery_production = 0

    # Convert START_DATE and END_DATE to datetime objects
    start_datetime = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_datetime = datetime.strptime(END_DATE, "%Y-%m-%d") + timedelta(days=1)

    debug_print("Debugging calculate_costs:")
    for price_entry in price_data:
        # Extract the timestamp and base price
        timestamp_str = price_entry["datum"]
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H")
        day = timestamp.date()

        # Skip entries outside the start and end date range
        if not (start_datetime <= timestamp < end_datetime):
            continue

        # Finalize the previous day's discharge totals
        if current_day != day:
            if current_day is not None and daily_discharge_total > 1:  # Only track days with more than 1 kWh discharged
                daily_discharge[current_day] = daily_discharge_total
            current_day = day
            daily_charge = 0
            daily_discharge_total = 0

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

        # Original values (without battery adjustments)
        original_hourly_consumption = hourly_consumption
        original_hourly_production = hourly_production

        # Check if production should be stopped for negative prices
        if STOP_PRODUCTION_NEGATIVE_PRICES and total_price_incl_vat_production < 0:
            debug_print(f"Negative price detected at {timestamp_str}: {total_price_incl_vat_production:.2f}. Stopping production.")
            hourly_production = 0  # Stop production for this hour

        # Simulate battery behavior if enabled
        if battery_enabled:
            battery_consumption, battery_production, battery_state = simulate_battery(
                hourly_consumption, hourly_production, battery_state, config, total_price_incl_vat_production, timestamp_str, strategy
            )
            # Track daily charge and discharge
            daily_charge += battery_state["total_charged"]
            daily_discharge_total += battery_state["total_discharged"]
        else:
            battery_consumption = hourly_consumption
            battery_production = hourly_production

        # Accumulate total consumption and production (original values)
        total_consumption += original_hourly_consumption
        total_production += original_hourly_production

        # Accumulate costs and income (original values)
        costs += original_hourly_consumption * total_price_incl_vat_consumption
        income += original_hourly_production * total_price_incl_vat_production

        # Accumulate battery-adjusted consumption and production
        total_battery_consumption += battery_consumption
        total_battery_production += battery_production

        # Accumulate battery-adjusted costs and income
        battery_adjusted_costs += battery_consumption * total_price_incl_vat_consumption
        battery_adjusted_income += battery_production * total_price_incl_vat_production

        # Calculate the month key (e.g., "2024-12")
        month_key = timestamp.strftime("%Y-%m")

        # Initialize monthly breakdown if not already present
        if month_key not in monthly_breakdown:
            monthly_breakdown[month_key] = {
                "costs": 0,
                "income": 0,
                "consumption": 0,
                "production": 0,
                "battery_adjusted_costs": 0,
                "battery_adjusted_income": 0,
                "fixed_supply_costs": FIXED_SUPPLY_COSTS,
                "transport_costs": TRANSPORT_COSTS,
                "energy_tax_compensation": ENERGY_TAX_COMPENSATION
            }

        # Update monthly breakdown
        monthly_breakdown[month_key]["costs"] += original_hourly_consumption * total_price_incl_vat_consumption
        monthly_breakdown[month_key]["income"] += original_hourly_production * total_price_incl_vat_production
        monthly_breakdown[month_key]["consumption"] += original_hourly_consumption
        monthly_breakdown[month_key]["production"] += original_hourly_production
        monthly_breakdown[month_key]["battery_adjusted_costs"] += battery_consumption * total_price_incl_vat_consumption
        monthly_breakdown[month_key]["battery_adjusted_income"] += battery_production * total_price_incl_vat_production

    # Finalize daily discharge tracking for the last day
    if current_day is not None and daily_discharge_total > 1:
        daily_discharge[current_day] = daily_discharge_total

    # Add fixed monthly costs to the total costs
    for month, data in monthly_breakdown.items():
        data["costs"] += data["fixed_supply_costs"] + data["transport_costs"] + data["energy_tax_compensation"]
        data["battery_adjusted_costs"] += data["fixed_supply_costs"] + data["transport_costs"] + data["energy_tax_compensation"]
        costs += data["fixed_supply_costs"] + data["transport_costs"] + data["energy_tax_compensation"]
        battery_adjusted_costs += data["fixed_supply_costs"] + data["transport_costs"] + data["energy_tax_compensation"]

    # Debugging: Print the final totals
    debug_print(f"Total Costs: {costs}, Total Income: {income}")
    debug_print(f"Battery-Adjusted Costs: {battery_adjusted_costs}, Battery-Adjusted Income: {battery_adjusted_income}")
    debug_print(f"Total Consumption: {total_consumption}, Total Production: {total_production}")
    debug_print(f"Battery-Adjusted Consumption: {total_battery_consumption}, Battery-Adjusted Production: {total_battery_production}")
    debug_print(f"Total Charged: {battery_state['total_charged']:.2f} kWh")
    debug_print(f"Total Discharged: {battery_state['total_discharged']:.2f} kWh")

    return costs, income, total_consumption, total_production, monthly_breakdown, battery_adjusted_costs, battery_adjusted_income

def write_results_to_csv(total_costs, total_income, total_consumption, total_production, monthly_breakdown, battery_adjusted_costs, battery_adjusted_income):
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
        writer.writerow(["Battery-Adjusted Costs (€)", f"{battery_adjusted_costs:.2f}"])
        writer.writerow(["Battery-Adjusted Income (€)", f"{battery_adjusted_income:.2f}"])
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
            "Battery-Adjusted Costs (€)", 
            "Battery-Adjusted Income (€)", 
            "Fixed Supply Costs (€)", 
            "Transport Costs (€)", 
            "Energy Tax Compensation (€)", 
            "Net Monthly Costs (€)"
        ])
        for month, data in monthly_breakdown.items():
            # Calculate net monthly costs (costs - income)
            net_monthly_costs = data["costs"] - data["income"]
            net_battery_monthly_costs = data["battery_adjusted_costs"] - data["battery_adjusted_income"]
            writer.writerow([
                month,
                f"{data['costs']:.2f}",
                f"{data['income']:.2f}",
                f"{data['consumption']:.2f}",
                f"{data['production']:.2f}",
                f"{data['battery_adjusted_costs']:.2f}",
                f"{data['battery_adjusted_income']:.2f}",
                f"{data['fixed_supply_costs']:.2f}",
                f"{data['transport_costs']:.2f}",
                f"{data['energy_tax_compensation']:.2f}",
                f"{net_monthly_costs:.2f}",
                f"{net_battery_monthly_costs:.2f}"  # Include net costs with battery simulation
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

    # Calculate costs, income, and totals (with and without battery simulation)
    (
        total_costs,
        total_income,
        total_consumption,
        total_production,
        monthly_breakdown,
        battery_adjusted_costs,
        battery_adjusted_income
    ) = calculate_costs(consumption_data, production_data, price_data)

    # Write results to a CSV file
    write_results_to_csv(
        total_costs,
        total_income,
        total_consumption,
        total_production,
        monthly_breakdown,
        battery_adjusted_costs,
        battery_adjusted_income
    )


if __name__ == "__main__":
    main()