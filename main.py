import requests
import json
from datetime import datetime, timedelta
import urllib.parse
import os
import csv
import openpyxl
from openpyxl.styles import Font
from openpyxl.chart import BarChart, Reference
import random
import pandas as pd

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the full path to config.json
config_path = os.path.join(script_dir, "config.json")

# Load configuration from config.json
with open(config_path, "r") as config_file:
    config = json.load(config_file)

# Configuration variables
DYNAMIC_PRICES_API_URL = config["DATA"]["DYNAMIC_PRICES_API_URL"]
DYNAMIC_PRICES_API_KEY = config["DATA"]["DYNAMIC_PRICES_API_KEY"]
START_DATE = config["PARAMETERS"]["START_DATE"]
END_DATE = config["PARAMETERS"]["END_DATE"]
CONSUMPTION_SENSORS = config["CONSUMPTION_SENSORS"]
PRODUCTION_SENSORS = config["PRODUCTION_SENSORS"]
VICTORIAMETRICS_URL = config["DATA"]["VICTORIAMETRICS_URL"]

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
DEBUG = config["PARAMETERS"].get("DEBUG", False)

# Load production stop setting
STOP_PRODUCTION_NEGATIVE_PRICES = config["PARAMETERS"].get("STOP_PRODUCTION_NEGATIVE_PRICES", False)
def debug_print(message):
    """Print debug messages if DEBUG is enabled."""
    if DEBUG:
        print(message)

def fetch_sensor_data_from_json(file_path, start_date, end_date, sensor_ids, output_file=None):
    """
    Fetch and parse sensor data from a JSON file (export.json), filtering by sensor IDs and date range.

    Args:
        file_path (str): Path to the JSON file.
        start_date (str): The start date for filtering data (format: YYYY-MM-DD).
        end_date (str): The end date for filtering data (format: YYYY-MM-DD).
        sensor_ids (list): List of sensor IDs to filter the data.
        output_file (str): Path to save the raw filtered data as a CSV file (optional).

    Returns:
        dict: A dictionary with timestamps as keys and hourly sensor increments as values.
    """
    try:
        # Load the JSON file
        with open(file_path, "r") as file:
            data = json.load(file)

        # Convert start_date and end_date to datetime objects
        start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
        end_datetime = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)

        # Initialize the filtered data list
        filtered_data = []

        # Process each sensor ID separately
        for sensor_id in sensor_ids:
            # Filter the data for the current sensor ID
            sensor_data = [
                record for record in data
                if record["statistic_id"] == sensor_id
                and start_datetime <= datetime.strptime(record["d"], "%Y-%m-%d %H:%M:%S") < end_datetime
            ]

            # Add the filtered data for the current sensor to the overall filtered data
            filtered_data.extend(sensor_data)

        # Debug: Print the number of records fetched
        debug_print(f"Filtered {len(filtered_data)} records from {file_path} for sensors: {sensor_ids} within date range {start_date} to {end_date}")

        # Save the raw filtered data to a CSV file if output_file is provided
        if output_file:
            try:
                # Ensure the data folder exists
                data_folder = os.path.join(script_dir, "data")
                os.makedirs(data_folder, exist_ok=True)

                # Construct the full path for the output file
                output_file_path = os.path.join(data_folder, output_file)

                # Write the filtered data to the CSV file
                with open(output_file_path, mode="w", newline="") as csv_file:
                    writer = csv.writer(csv_file)
                    # Write the header
                    writer.writerow(["statistic_id", "timestamp", "increment"])
                    # Write the data
                    for record in filtered_data:
                        writer.writerow([record["statistic_id"], record["d"], record["increment"]])
                debug_print(f"Raw filtered data written to {output_file_path}")
            except IOError as e:
                debug_print(f"Failed to write raw filtered data to {output_file}: {e}")

        # Process the cumulative data
        return process_cumulative_data(filtered_data, start_date, end_date, "d", "increment", "%Y-%m-%d %H:%M:%S")
    except (IOError, json.JSONDecodeError) as e:
        print(f"Error reading or parsing JSON file {file_path}: {e}")
        return {}
    
def fetch_sensor_data_victoriametrics(sensor_objs, start_date, end_date, output_file):
    """
    Fetches sensor data from VictoriaMetrics for the given sensor objects and date range.
    Uses per-sensor 'type', 'interval', and 'data_gap_fill' from config.json.
    If multiple sensors are combined in one query, only store the combined result once.
    """
    combined_data = []  # List to store combined raw data for all sensors
    hourly_totals = {}  # Dictionary to store hourly increments

    # Convert dates to timestamps if needed
    start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ").timestamp())
    end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ").timestamp())

    # Group sensors for combined query if needed
    counter_group = [s for s in sensor_objs if s.get("type") == "counter" and s.get("resets", "no") != "yes"]
    processed_counters = set()

    for sensor_obj in sensor_objs:
        sensor_id = sensor_obj["sensor"]
        sensor_type = sensor_obj.get("type", "gauge")
        data_gap_fill = sensor_obj.get("data_gap_fill")
        interval = sensor_obj.get("interval")
        resets = sensor_obj.get("resets", "no")

        # Determine query logic
        if sensor_type == "counter":
            if resets == "yes":
                # Counter resets, use the current formula
                query = f'clamp_min(delta(last_over_time({sensor_id}_value[1d])[1h]),0) offset 1h'
                result_sensor_id = sensor_id
            else:
                # Only process the group once
                if tuple(sorted([s["sensor"] for s in counter_group])) in processed_counters:
                    continue
                group_sensors = [s["sensor"] for s in counter_group]
                sensor_regex = "|".join([f"{s}_value" for s in group_sensors])
                query = f'sum(increase(last_over_time({{__name__=~"{sensor_regex}"}}[1d])))'
                result_sensor_id = "combined_counter"
                processed_counters.add(tuple(sorted(group_sensors)))
        elif sensor_type == "gauge":
            # Gauge: just use the value
            if data_gap_fill:
                query = f'last_over_time({sensor_id}_value[{data_gap_fill}])'
            else:
                query = f'{sensor_id}_value'
            result_sensor_id = sensor_id
        else:
            raise ValueError(f"Unknown sensor type: {sensor_type}")

        print(f"Fetching data for sensor {sensor_id} from VictoriaMetrics, query: {query}")
        params = {
            "query": query,
            "start": start_timestamp,
            "end": end_timestamp,
            "step": "3600s"
        }

        response = requests.get(VICTORIAMETRICS_URL, params=params)

        if response.status_code == 200:
            raw_data = response.json().get("data", {}).get("result", [])
            for result in raw_data:
                for ts, val in result.get("values", []):
                    utc_timestamp = datetime.utcfromtimestamp(int(ts))
                    formatted_timestamp = utc_timestamp.strftime("%Y-%m-%dT%H")
                    increment = float(val)
                    if result_sensor_id not in hourly_totals:
                        hourly_totals[result_sensor_id] = {}
                    hourly_totals[result_sensor_id][formatted_timestamp] = increment
                    combined_data.append({
                        "statistic_id": result_sensor_id,
                        "d": formatted_timestamp,
                        "value": increment
                    })
        else:
            print(f"Failed to fetch data for {sensor_id}: {response.status_code} {response.text}")

    # Save combined raw data to a JSON file
    try:
        data_folder = os.path.join(script_dir, "data")
        os.makedirs(data_folder, exist_ok=True)
        output_file_path = os.path.join(data_folder, output_file)
        with open(output_file_path, "w") as file:
            json.dump(combined_data, file, indent=4)
        debug_print(f"Combined raw data written to {output_file_path}")
    except IOError as e:
        debug_print(f"Failed to write combined raw data to {output_file_path}: {e}")

    return hourly_totals

def process_cumulative_data(data, start_date, end_date, timestamp_key, value_key, timestamp_format):
    """
    Process cumulative data to aggregate hourly values.

    Args:
        data (list): List of records containing hourly increments.
        start_date (str): The start date for filtering data (format: YYYY-MM-DD).
        end_date (str): The end date for filtering data (format: YYYY-MM-DD).
        timestamp_key (str): The key in the record that contains the timestamp.
        value_key (str): The key in the record that contains the increment value.
        timestamp_format (str): The format of the timestamp in the data.

    Returns:
        dict: A dictionary with timestamps as keys and aggregated hourly values as values.
    """
    # Convert start_date and end_date to datetime objects
    start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    end_datetime = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)

    # Initialize the result dictionary
    hourly_totals = {}

    # Process each record in the data
    for record in data:
        try:
            # Parse the timestamp and value
            timestamp = datetime.strptime(record[timestamp_key], timestamp_format)
            value = float(record[value_key])

            # Filter records within the specified date range
            if start_datetime <= timestamp < end_datetime:
                # Format the timestamp as YYYY-MM-DDTHH
                formatted_timestamp = timestamp.strftime("%Y-%m-%dT%H")

                # Aggregate the value
                if formatted_timestamp not in hourly_totals:
                    hourly_totals[formatted_timestamp] = 0
                hourly_totals[formatted_timestamp] += value
        except (KeyError, ValueError) as e:
            debug_print(f"Skipping invalid record: {record}, Error: {e}")

    return hourly_totals

def write_hourly_comparison_to_csv(victoriametrics_data, export_json_data, output_file):
    """
    Write a CSV file comparing hourly kWh data from VictoriaMetrics and export.json.

    Args:
        victoriametrics_data (dict): Hourly kWh data from VictoriaMetrics.
        export_json_data (dict): Hourly kWh data from export.json.
        output_file (str): Path to save the comparison CSV file.
    """
    # Ensure the results folder exists
    results_folder = "results"
    os.makedirs(results_folder, exist_ok=True)

    # Construct the full path for the output file
    output_file_path = os.path.join(results_folder, output_file)

    # Get all unique timestamps from both data sources
    all_timestamps = set(victoriametrics_data.keys()).union(set(export_json_data.keys()))

    # Write the comparison data to the CSV file
    with open(output_file_path, mode="w", newline="") as csv_file:
        writer = csv.writer(csv_file)

        # Write the header
        writer.writerow(["Timestamp", "VictoriaMetrics (kWh)", "Export.json (kWh)"])

        # Write the data for each timestamp
        for timestamp in sorted(all_timestamps):
            victoriametrics_value = victoriametrics_data.get(timestamp, 0)
            export_json_value = export_json_data.get(timestamp, 0)
            writer.writerow([timestamp, f"{victoriametrics_value:.3f}", f"{export_json_value:.3f}"])

    #print(f"Hourly comparison written to {output_file_path}")

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
        cache_file = f"./data/dynamic_energy_prices_{year}.json"  # Cache file for the year

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

def simulate_battery(hourly_consumption, hourly_production, battery_state, config, total_price_incl_vat_production, total_price_incl_vat_consumption, timestamp, strategy):
    """
    Simulate the behavior of a battery for a single hour based on the chosen strategy.
    """
    # Extract battery parameters from the config
    battery_size = config["BATTERY_SIMULATION"]["BATTERY_SIZE_KWH"]
    max_charging_rate = config["BATTERY_SIMULATION"]["MAX_CHARGING_RATE_KWH"]
    max_discharging_rate = config["BATTERY_SIMULATION"]["MAX_DISCHARGING_RATE_KWH"]
    round_trip_efficiency = config["BATTERY_SIMULATION"].get("ROUND_TRIP_EFFICIENCY", 0.8)
    discharge_minimum = (config["BATTERY_SIMULATION"]["DISCHARGE_MINIMUM_PERCENTAGE"] / 100) * battery_size
    charge_maximum = (config["BATTERY_SIMULATION"]["CHARGE_MAXIMUM_PERCENTAGE"] / 100) * battery_size

    # Get the current battery level
    battery_level = battery_state["level"]

    # Reset charge/discharge tracking for this hour
    charge_amount = 0
    discharge_amount = 0
    battery_state["charge_amount"] = 0  # Reset charge amount
    battery_state["discharge_amount"] = 0  # Reset discharge amount
    energy_loss = 0  # Track energy loss due to round-trip efficiency
    simulated_consumption = hourly_consumption
    simulated_production = hourly_production

    if strategy == "self-sufficiency":
    # Self-Sufficiency Strategy

        net_consumption = max(0, hourly_consumption - hourly_production)
        net_production = max(0, hourly_production - hourly_consumption)
        
        # Step 1: Use production to reduce consumption
        if net_consumption > 0:
            simulated_production = 0  # All production is used to offset consumption
            simulated_consumption = net_consumption  # Remaining consumption after using production
        else:
            # No consumption, all production is available
            simulated_consumption = 0
            simulated_production = net_production  # All production is available for the grid

        # Step 2: Charge the battery with remaining production
        if net_production > 0:
            charge_amount = min(net_production, max_charging_rate, charge_maximum - battery_level)
            if charge_amount >= 0.1:  # Only charge if the amount is at least 0.1 kWh
                battery_level += charge_amount * round_trip_efficiency
                energy_loss += charge_amount * (1 - round_trip_efficiency)
                battery_state["total_charged"] += charge_amount
                battery_state["charge_amount"] = charge_amount  # Update charge amount for this hour
                net_production -= charge_amount  # Reduce net production by the amount charged
                simulated_production = net_production  # Remaining production goes to the grid

        # Step 3: If there is no production, discharge the battery to meet consumption
        elif net_consumption > 0:
            discharge_amount = min(net_consumption, max_discharging_rate, max(0, battery_level - discharge_minimum))
            if discharge_amount >= 0.1:  # Only discharge if the amount is at least 0.1 kWh
                battery_level -= discharge_amount
                battery_state["total_discharged"] += discharge_amount
                battery_state["discharge_amount"] = discharge_amount  # Update discharge amount for this hour
                net_consumption -= discharge_amount  # Reduce net consumption by the amount discharged
                simulated_consumption = net_consumption
                   
    elif strategy == "dynamic_cost_optimization":
        # Dynamic Cost Optimization Strategy

        # Case 1: Charge the battery if the consumption price is below the low threshold
        if total_price_incl_vat_consumption < config["BATTERY_SIMULATION"].get("DYNAMIC_PRICE_THRESHOLD_LOW", 0.10):
            charge_amount = min(max_charging_rate, charge_maximum - battery_level)
            if charge_amount >= 0.1:  # Only charge if the amount is at least 0.1 kWh
                battery_level += charge_amount * round_trip_efficiency
                energy_loss += charge_amount * (1 - round_trip_efficiency)
                battery_state["total_charged"] += charge_amount
                battery_state["charge_amount"] = charge_amount  # Update charge amount for this hour
                simulated_consumption += charge_amount  # Increase simulated consumption for charging

        # Case 2: Discharge the battery for consumption if the consumption price is very high
        elif hourly_consumption > 0 and total_price_incl_vat_consumption > config["BATTERY_SIMULATION"]["DYNAMIC_PRICE_THRESHOLD_HIGH"]:
            discharge_amount = min(hourly_consumption, max_discharging_rate, max(0, battery_level - discharge_minimum))
            if discharge_amount >= 0.1:  # Only discharge if the amount is at least 0.1 kWh
                battery_level -= discharge_amount
                simulated_consumption -= discharge_amount
                battery_state["total_discharged"] += discharge_amount
                battery_state["discharge_amount"] = discharge_amount  # Update discharge amount for this hour

        # Case 3: Discharge the battery for production if the production price is very high
        elif total_price_incl_vat_production > config["BATTERY_SIMULATION"]["DYNAMIC_PRICE_THRESHOLD_HIGH"]:
            discharge_amount = min(max_discharging_rate, max(0, battery_level - discharge_minimum))
            if discharge_amount >= 0.1:  # Only discharge if the amount is at least 0.1 kWh
                battery_level -= discharge_amount
                simulated_production += discharge_amount
                battery_state["total_discharged"] += discharge_amount
                battery_state["discharge_amount"] = discharge_amount  # Update discharge amount for this hour

        # Case 4: Charge the battery with solar production if the production price is not above the high threshold
        if hourly_production > 0 and total_price_incl_vat_production <= config["BATTERY_SIMULATION"]["DYNAMIC_PRICE_THRESHOLD_HIGH"]:
            charge_amount = min(hourly_production, max_charging_rate, charge_maximum - battery_level)
            if charge_amount >= 0.1:  # Only charge if the amount is at least 0.1 kWh
                battery_level += charge_amount * round_trip_efficiency
                energy_loss += charge_amount * (1 - round_trip_efficiency)
                battery_state["total_charged"] += charge_amount
                battery_state["charge_amount"] = charge_amount  # Update charge amount for this hour
                simulated_production = max(0, simulated_production - charge_amount)  # Ensure it doesn't go below 0

            # Handle the remainder of the production if the battery is full
            remainder_production = hourly_production - charge_amount
            if remainder_production > 0:
                simulated_production += remainder_production  # Return the remainder to the grid
            
    # Update the battery state
    battery_state["level"] = battery_level

    # Calculate charge cycles
    usable_capacity = charge_maximum - discharge_minimum
    battery_state["charge_cycles"] = int(battery_state["total_discharged"] // usable_capacity)

    return simulated_consumption, simulated_production, battery_state, energy_loss

def create_corrected_heatpump_consumption_sensor(heatpump_consumption_sensor, power_output_sensor):
    """
    Create a more detailed heatpump consumption sensor by distributing the daily total
    consumption over the hours, proportional to the power output sensor for each hour.
    Ensures no hour has negative consumption; if so, redistributes the deficit to adjacent hours.
    Args:
        heatpump_consumption_sensor (dict): {timestamp: kWh} with 1kWh increments.
        power_output_sensor (dict): {timestamp: kWh produced}.
    Returns:
        dict: {timestamp: corrected consumption (float)}
    """
    from collections import defaultdict

    # Group timestamps by day
    daily_consumption = defaultdict(float)
    daily_power_output = defaultdict(float)
    hourly_power_output = defaultdict(dict)

    for ts, val in power_output_sensor.items():
        day = ts[:10]
        daily_power_output[day] += val
        hourly_power_output[day][ts] = val

    for ts, val in heatpump_consumption_sensor.items():
        day = ts[:10]
        daily_consumption[day] += val

    corrected = {}
    for day in hourly_power_output:
        total_output = daily_power_output[day]
        total_consumption = daily_consumption.get(day, 0)
        timestamps = sorted(hourly_power_output[day].keys())
        # Initial proportional distribution
        if total_output == 0 or total_consumption == 0:
            for ts in timestamps:
                corrected[ts] = max(0, heatpump_consumption_sensor.get(ts, 0))
        else:
            # Proportional distribution
            temp = {}
            for ts in timestamps:
                output = hourly_power_output[day][ts]
                temp[ts] = (output / total_output) * total_consumption if total_output > 0 else 0

            # Iteratively fix negatives by redistributing to adjacent hours
            while True:
                negatives = [ts for ts in timestamps if temp[ts] < 0]
                if not negatives:
                    break
                for ts in negatives:
                    deficit = -temp[ts]
                    temp[ts] = 0
                    # Find adjacent hours to redistribute
                    idx = timestamps.index(ts)
                    # Try previous and next hours
                    adjacents = []
                    if idx > 0:
                        adjacents.append(timestamps[idx - 1])
                    if idx < len(timestamps) - 1:
                        adjacents.append(timestamps[idx + 1])
                    # If no adjacents (shouldn't happen), skip
                    if not adjacents:
                        continue
                    # Split deficit over adjacents that are >0
                    positive_adjacents = [a for a in adjacents if temp[a] > 0]
                    if not positive_adjacents:
                        continue
                    share = deficit / len(positive_adjacents)
                    for a in positive_adjacents:
                        temp[a] -= share
            # After redistribution, set negatives to zero (should be none)
            for ts in timestamps:
                corrected[ts] = max(0, temp[ts])

            # Final normalization: scale to match daily total (in case of rounding)
            sum_corrected = sum(corrected[ts] for ts in timestamps)
            if sum_corrected > 0 and abs(sum_corrected - total_consumption) > 1e-6:
                scale = total_consumption / sum_corrected
                for ts in timestamps:
                    corrected[ts] *= scale

    return corrected

def calculate_smart_heating_savings(hourly_data, config):
    """
    Adjust heatpump consumption based on smart heating logic and calculate savings.
    Returns a dict with monthly savings and updates hourly_data in-place.
    """
    if not config["HEATPUMP"].get("ENABLE_SMART_HEATING", False):
        return {}, hourly_data  # No adjustment

    stop_hours = int(config["HEATPUMP"]["STOP_HEATPUMP_ON_NUMBER_OF_MOST_EXPENSIVE_HOURS"])
    temp_threshold = config["HEATPUMP"]["FULL_TIME_RUNNING_MINIMUM_THRESHOLD_BASED_ON_OUTSIDE_TEMPERATURE"]
    heatpump_sensor_objs = config["HEATPUMP"]["HEATPUMP_SENSORS"]
    outside_temp_sensor = next((s["sensor"] for s in heatpump_sensor_objs if s.get("name") == "OUTSIDE_TEMPERATURE_SENSOR"), None)
    daily_consumption_sensor = next((s["sensor"] for s in heatpump_sensor_objs if s.get("name") == "HEATPUMP_CONSUMPTION_SENSOR"), None)
    power_output_sensor = next((s["sensor"] for s in heatpump_sensor_objs if s.get("name") == "HEATPUMP_POWER_OUTPUT_SENSOR"), None)

    # --- Build dicts for corrected heatpump consumption ---
    # Collect all hourly values for the two sensors
    heatpump_consumption_dict = {}
    power_output_dict = {}
    for record in hourly_data:
        ts = record["timestamp"]
        if daily_consumption_sensor:
            heatpump_consumption_dict[ts] = record.get("heatpump_consumption_sensor", 0) or 0
        if power_output_sensor:
            power_output_dict[ts] = record.get("power_output_sensor", 0) or 0

    # Use the correction function
    corrected_heatpump_consumption = create_corrected_heatpump_consumption_sensor(
        heatpump_consumption_dict, power_output_dict
    )

    from collections import defaultdict
    daily_data = defaultdict(list)
    for record in hourly_data:
        day = record["timestamp"][:10]
        daily_data[day].append(record)

    monthly_savings = defaultdict(float)

    for day, records in daily_data.items():
        # Calculate average outside temperature for the day
        temps = [r.get("outside_temp") for r in records if r.get("outside_temp") is not None]
        avg_temp = sum(temps) / len(temps) if temps else None

        # Store average temperature in each record for that day
        for r in records:
            r["avg_outside_temp"] = avg_temp

        # Get daily consumption from the sensor
        daily_consumption = sum(r.get("heatpump_consumption_sensor", 0) or 0 for r in records)
        # Get daily production from the power output sensor
        daily_production = sum((r.get("power_output_sensor") or 0) for r in records)

        # Calculate COP (if possible)
        cop = (daily_production / daily_consumption) if daily_consumption > 0 else 1

        # Sort hours by price (descending)
        sorted_hours = sorted(records, key=lambda r: r["price_consumption"], reverse=True)

        # Determine hours to skip
        hours_to_skip = []
        if avg_temp is not None and avg_temp > temp_threshold:
            hours_to_skip = sorted_hours[:stop_hours]

        # Adjust consumption for skipped hours, but do not exceed daily total
        adjusted_consumption = 0
        for r in records:
            ts = r["timestamp"]
            # Save original for reference
            r["original_heatpump_consumption"] = r.get("heatpump_consumption_sensor", 0)
            # Use the corrected value from the function
            r["corrected_heatpump_consumption"] = corrected_heatpump_consumption.get(ts, 0)
            if r in hours_to_skip:
                r["heatpump_consumption_adjusted"] = 0
                r["heatpump_stopped"] = True
            else:
                est = r["corrected_heatpump_consumption"]
                # Cap so we don't exceed daily total
                r["heatpump_consumption_adjusted"] = min(est, r.get(daily_consumption_sensor, 1))
                r["heatpump_stopped"] = False
            adjusted_consumption += r["heatpump_consumption_adjusted"]

        # If adjusted total > daily_consumption, scale down
        if adjusted_consumption > daily_consumption and adjusted_consumption > 0:
            scale = daily_consumption / adjusted_consumption
            for r in records:
                r["heatpump_consumption_adjusted"] *= scale

        # Calculate cost savings for the day
        original_cost = sum(r["price_consumption"] * r.get(daily_consumption_sensor, 0) for r in records)
        adjusted_cost = sum(r["price_consumption"] * r["heatpump_consumption_adjusted"] for r in records)
        savings = original_cost - adjusted_cost

        month = day[:7]
        monthly_savings[month] += savings

    return monthly_savings, hourly_data

def redistribute_skipped_kwh(hourly_data, config):
    """
    For each day, redistribute skipped kWh evenly over the remaining hours.
    Adds 'heatpump_consumption_final' to each record.
    Ensures the daily sum of 'heatpump_consumption_final' matches the original daily total.
    """
    from collections import defaultdict
    daily_data = defaultdict(list)
    for record in hourly_data:
        day = record["timestamp"][:10]
        daily_data[day].append(record)

    for day, records in daily_data.items():
        skipped = [r for r in records if r.get("heatpump_stopped")]
        active = [r for r in records if not r.get("heatpump_stopped")]
        total_skipped = sum(r.get("corrected_heatpump_consumption", 0) for r in skipped)
        n_active = len(active)
        for r in records:
            if r.get("heatpump_stopped"):
                r["heatpump_consumption_final"] = 0
            else:
                add_kwh = total_skipped / n_active if n_active > 0 else 0
                r["heatpump_consumption_final"] = r.get("heatpump_consumption_adjusted", 0) + add_kwh

        # --- Normalize so daily sum matches original daily total ---
        original_total = sum((r.get("original_heatpump_consumption") or 0) for r in records)
        final_total = sum(r["heatpump_consumption_final"] for r in records)
        if final_total > 0 and abs(final_total - original_total) > 1e-6:
            scale = original_total / final_total
            for r in records:
                r["heatpump_consumption_final"] *= scale

    return hourly_data

def calculate_hourly_energy_prices(base_price, total_annual_consumption, total_annual_production, cumulative_production, salderen):
    """
    Calculate the hourly energy prices for consumption and production.

    Args:
        base_price (float): The base energy price (€/kWh).
        total_annual_consumption (float): Total annual consumption in kWh.
        total_annual_production (float): Total annual production in kWh.
        cumulative_production (float): Cumulative production up to the current hour in kWh.
        salderen (bool): Whether salderen is enabled.

    Returns:
        tuple: (hourly_price_consumption, hourly_price_production)
    """

    # Calculate the hourly energy price for consumption
    hourly_price_consumption = base_price + TAXES["STORAGE_COSTS"] + TAXES["ENERGY_TAX"]
    hourly_price_consumption *= (1 + TAXES["VAT"] / 100)

    # Calculate the hourly energy price for production
    if salderen == False:
        # If salderen is disabled, production price excludes energy tax and VAT for all hours
        hourly_price_production = base_price + TAXES["STORAGE_COSTS_PRODUCTION"]
    else:
        if cumulative_production > total_annual_consumption:
            # For excess production (above total annual consumption), exclude energy tax and VAT
            hourly_price_production = base_price + TAXES["STORAGE_COSTS_PRODUCTION"]
        else:
            # For production within total annual consumption, include energy tax and VAT
            hourly_price_production = base_price + TAXES["STORAGE_COSTS_PRODUCTION"] + TAXES["ENERGY_TAX"]
            hourly_price_production *= (1 + TAXES["VAT"] / 100)
    
    return hourly_price_consumption, hourly_price_production

def get_heatpump_sensor_id(sensor_objs, name):
    for s in sensor_objs:
        if s.get("name") == name:
            return s["sensor"]
    return None

def calculate_costs(consumption_data, production_data, price_data):
    """Calculate energy costs, income, and total consumption/production, with battery simulation and smart heating."""

    # Get heatpump sensor IDs from config
    heatpump_sensor_objs = config["HEATPUMP"]["HEATPUMP_SENSORS"]
    outside_temp_sensor_id = get_heatpump_sensor_id(heatpump_sensor_objs, "OUTSIDE_TEMPERATURE_SENSOR")
    heatpump_consumption_sensor_id = get_heatpump_sensor_id(heatpump_sensor_objs, "HEATPUMP_CONSUMPTION_SENSOR")
    power_output_sensor_id = get_heatpump_sensor_id(heatpump_sensor_objs, "HEATPUMP_POWER_OUTPUT_SENSOR")

    # --- Step 1: Build initial hourly_data with all relevant info ---
    hourly_data = []
    # Convert START_DATE and END_DATE to datetime objects
    start_datetime = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_datetime = datetime.strptime(END_DATE, "%Y-%m-%d") + timedelta(days=1)

    # Use only the combined_counter key for calculations
    consumption_key = next(iter(consumption_data.keys()), None)
    production_key = next(iter(production_data.keys()), None)

    # Calculate total annual consumption and production
    total_annual_consumption = sum(
        consumption_data.get(consumption_key, {}).get(ts, 0)
        for ts in consumption_data.get(consumption_key, {})
    ) if consumption_key else 0

    total_annual_production = sum(
        production_data.get(production_key, {}).get(ts, 0)
        for ts in production_data.get(production_key, {})
    ) if production_key else 0

    cumulative_production = 0

    for price_entry in price_data:
        timestamp_str = price_entry["datum"]
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H")
        if not (start_datetime <= timestamp < end_datetime):
            continue

        base_price = float(price_entry["prijs_excl_belastingen"].replace(",", "."))

        # Use only the combined_counter for each hour
        hourly_consumption = consumption_data.get(consumption_key, {}).get(timestamp_str, 0) if consumption_key else 0
        hourly_production = production_data.get(production_key, {}).get(timestamp_str, 0) if production_key else 0

        # Get extra sensors for smart heating
        outside_temp = consumption_data.get(outside_temp_sensor_id, {}).get(timestamp_str, None) if outside_temp_sensor_id else None
        heatpump_consumption_sensor = consumption_data.get(heatpump_consumption_sensor_id, {}).get(timestamp_str, None) if heatpump_consumption_sensor_id else None
        power_output_sensor = consumption_data.get(power_output_sensor_id, {}).get(timestamp_str, None) if power_output_sensor_id else None

        cumulative_production += hourly_production

        hourly_price_consumption, hourly_price_production = calculate_hourly_energy_prices(
            base_price, total_annual_consumption, total_annual_production, cumulative_production, config["PARAMETERS"]["SALDEREN"]
        )

        adjusted_hourly_production = hourly_production
        if STOP_PRODUCTION_NEGATIVE_PRICES and hourly_price_production < 0 and hourly_production > 0:
            debug_print(f"Negative price detected at {timestamp_str}: {hourly_price_production:.2f}. Stopping production.")
            adjusted_hourly_production = 0

        record = {
            "timestamp": timestamp_str,
            "production": hourly_production,
            "adjusted_production": adjusted_hourly_production,
            "consumption": hourly_consumption,
            "price_consumption": hourly_price_consumption,
            "price_production": hourly_price_production,
            "outside_temp": outside_temp,
            "heatpump_consumption_sensor": heatpump_consumption_sensor,
            "power_output_sensor": power_output_sensor,
        }
        hourly_data.append(record)

    # --- Step 2: Apply smart heating savings and redistribution ---
    monthly_savings, hourly_data = calculate_smart_heating_savings(hourly_data, config)
    hourly_data = redistribute_skipped_kwh(hourly_data, config)

    # --- Step 2b: Adjust main consumption kWh for heatpump saving mode ---
    if config["HEATPUMP"].get("ENABLE_SMART_HEATING", False):
        for record in hourly_data:
            # If the original heatpump consumption is None, treat as 0
            orig_hp = record.get("original_heatpump_consumption", 0) or 0
            final_hp = record.get("heatpump_consumption_final", 0) or 0
            # Remove the original heatpump part (if present), add the redistributed one
            record["consumption"] = (record.get("consumption", 0) or 0) - orig_hp + final_hp

    # --- Step 3: Use adjusted heatpump consumption for further calculations ---
    costs = 0
    income = 0
    total_consumption = 0
    total_production = 0
    battery_adjusted_costs = 0
    battery_adjusted_income = 0
    total_energy_loss = 0
    monthly_breakdown = {}

    # Battery simulation variables
    battery_enabled = config["BATTERY_SIMULATION"]["ENABLE"]
    battery_state = {
        "level": config["BATTERY_SIMULATION"].get("DISCHARGE_LIMIT", 0.1) * config["BATTERY_SIMULATION"]["BATTERY_SIZE_KWH"],
        "total_charged": 0,
        "total_discharged": 0,
        "charge_cycles": 0
    }
    strategy = config["BATTERY_SIMULATION"].get("BATTERY_CHARGE_STRATEGY", "self-sufficiency")
    cumulative_production = 0

    for record in hourly_data:
        timestamp_str = record["timestamp"]
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H")
        # Always use the adjusted household consumption
        hourly_consumption = record["consumption"]
        hourly_production = record["adjusted_production"]
        price_consumption = record["price_consumption"]
        price_production = record["price_production"]

        cumulative_production += hourly_production

        # Battery simulation
        if battery_enabled:
            battery_consumption, battery_production, battery_state, energy_loss = simulate_battery(
                hourly_consumption,
                hourly_production,
                battery_state,
                config,
                price_production,
                price_consumption,
                timestamp_str,
                strategy
            )
            total_energy_loss += energy_loss
            consumption_adjusted = battery_consumption != hourly_consumption
            production_adjusted = battery_production != hourly_production
        else:
            battery_consumption = hourly_consumption
            battery_production = hourly_production
            consumption_adjusted = False
            production_adjusted = False

        total_consumption += hourly_consumption
        total_production += hourly_production
        costs += hourly_consumption * price_consumption
        income += hourly_production * price_production
        battery_adjusted_costs += battery_consumption * price_consumption
        battery_adjusted_income += battery_production * price_production

        record.update({
            "final_consumption": hourly_consumption,
            "final_production": hourly_production,
            "simulated_consumption": battery_consumption if battery_enabled else None,
            "simulated_production": battery_production if battery_enabled else None,
            "consumption_adjusted": consumption_adjusted,
            "production_adjusted": production_adjusted,
            "total_cost_or_income": (
                hourly_production * price_production -
                hourly_consumption * price_consumption
            ),
            "battery_total_cost_or_income": (
                battery_production * price_production -
                battery_consumption * price_consumption
            ) if battery_enabled else None,
            "battery_charge": battery_state.get("charge_amount", 0),
            "battery_discharge": battery_state.get("discharge_amount", 0),
            "state_of_charge": (battery_state["level"] / config["BATTERY_SIMULATION"]["BATTERY_SIZE_KWH"]) * 100
        })

        month_key = timestamp.strftime("%Y-%m")
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
        # Use the original total household consumption for monthly aggregation
        monthly_breakdown[month_key]["costs"] += hourly_consumption * price_consumption
        monthly_breakdown[month_key]["income"] += hourly_production * price_production
        monthly_breakdown[month_key]["consumption"] += record["consumption"]
        monthly_breakdown[month_key]["production"] += hourly_production
        monthly_breakdown[month_key]["battery_adjusted_costs"] += battery_consumption * price_consumption
        monthly_breakdown[month_key]["battery_adjusted_income"] += battery_production * price_production

    # Add fixed monthly costs to the total costs
    for month, data in monthly_breakdown.items():
        data["costs"] += data["fixed_supply_costs"] + data["transport_costs"] + data["energy_tax_compensation"]
        data["battery_adjusted_costs"] += data["fixed_supply_costs"] + data["transport_costs"] + data["energy_tax_compensation"]
        costs += data["fixed_supply_costs"] + data["transport_costs"] + data["energy_tax_compensation"]
        battery_adjusted_costs += data["fixed_supply_costs"] + data["transport_costs"] + data["energy_tax_compensation"]

    return (
        costs,
        income,
        total_consumption,
        total_production,
        monthly_breakdown,
        battery_adjusted_costs,
        battery_adjusted_income,
        hourly_data,
        total_energy_loss,
        battery_state["total_charged"],
        battery_state["total_discharged"],
        battery_state["charge_cycles"],
        monthly_savings
    )

def write_results_to_excel(
    total_costs, total_income, total_consumption, total_production, monthly_breakdown,
    battery_adjusted_costs, battery_adjusted_income, hourly_data, total_energy_loss,
    total_charged, total_discharged, charge_cycles, monthly_savings):
    """
    Write the results to an Excel file with multiple sheets: 'settings', 'summary', 'monthly data', and 'hourly data'.
    """
    # Create a new Excel workbook
    workbook = openpyxl.Workbook()

    # Add the 'settings' sheet as the first sheet
    settings_sheet = workbook.active
    settings_sheet.title = "settings"

    # Write the settings to the 'settings' sheet
    settings_sheet.append(["Setting", "Value"])
    for key, value in config.items():
        if key == "DATA":
            continue  # Skip the DATA section
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, (list, dict)):
                    sub_value = json.dumps(sub_value)  # Convert to JSON string for readability
                settings_sheet.append([f"{key}.{sub_key}", sub_value])
        else:
            if isinstance(value, (list, dict)):
                value = json.dumps(value)  # Convert to JSON string for readability
            settings_sheet.append([key, value])

    # Adjust column widths for the 'settings' sheet
    for column in settings_sheet.columns:
        max_length = 0
        column_letter = column[0].column_letter  # Get the column letter
        for cell in column:
            try:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            except:
                pass
        settings_sheet.column_dimensions[column_letter].width = max_length + 2  # Add padding

    # Add the 'summary' sheet explicitly
    summary_sheet = workbook.create_sheet(title="summary")

    # Write the header for the 'summary' sheet
    summary_sheet.append(["Without Battery", "Value", "", "With Battery", "Value"])

    # Calculate total simulated consumption and production for battery data
    total_simulated_consumption = sum(record["simulated_consumption"] for record in hourly_data if record["simulated_consumption"] is not None)
    total_simulated_production = sum(record["simulated_production"] for record in hourly_data if record["simulated_production"] is not None)

    # Calculate the time period in years
    start_date = datetime.strptime(config["PARAMETERS"]["START_DATE"], "%Y-%m-%d")
    end_date = datetime.strptime(config["PARAMETERS"]["END_DATE"], "%Y-%m-%d")
    time_period_years = (end_date - start_date).days / 365.0

    # Calculate the annual savings
    final_cost_non_battery = total_costs - total_income
    final_cost_battery = battery_adjusted_costs - battery_adjusted_income
    annual_savings = (final_cost_non_battery - final_cost_battery) / time_period_years

    # Calculate the payback period for the battery
    battery_price = config["BATTERY_SIMULATION"]["BATTERY_PRICE"]
    payback_period_years = battery_price / annual_savings if annual_savings > 0 else float('inf')

    # Calculate weighted average hourly energy prices
    weighted_avg_price_consumption_non_battery = (
        sum(record["price_consumption"] * record["consumption"] for record in hourly_data if record["consumption"] > 0)
        / total_consumption
        if total_consumption > 0
        else 0
    )

    weighted_avg_price_production_non_battery = (
        sum(record["price_production"] * record["production"] for record in hourly_data if record["production"] > 0)
        / total_production
        if total_production > 0
        else 0
    )

    weighted_avg_price_consumption_battery = (
        sum(record["price_consumption"] * record["simulated_consumption"] for record in hourly_data if record["simulated_consumption"] > 0)
        / total_simulated_consumption
        if total_simulated_consumption > 0
        else 0
    )

    weighted_avg_price_production_battery = (
        sum(record["price_production"] * record["simulated_production"] for record in hourly_data if record["simulated_production"] > 0)
        / total_simulated_production
        if total_simulated_production > 0
        else 0
    )

    # Define the data for both sections
    non_battery_data = [
        ["Total Costs (€)", total_costs],
        ["Total Income (€)", total_income],
        ["Final Annual Cost (€)", final_cost_non_battery],
        ["Total Consumption (kWh)", total_consumption],
        ["Total Production (kWh)", total_production],
        ["Weighted Avg Price (Consumption €/kWh)", round(weighted_avg_price_consumption_non_battery, 4)],
        ["Weighted Avg Price (Production €/kWh)", round(weighted_avg_price_production_non_battery, 4)]
    ]

    battery_data = [
        ["Total Costs (€)", battery_adjusted_costs],
        ["Total Income (€)", battery_adjusted_income],
        ["Final Annual Cost (€)", final_cost_battery],
        ["Total Consumption (kWh)", total_simulated_consumption],
        ["Total Production (kWh)", total_simulated_production],
        ["Weighted Avg Price (Consumption €/kWh)", round(weighted_avg_price_consumption_battery, 4)],
        ["Weighted Avg Price (Production €/kWh)", round(weighted_avg_price_production_battery, 4)],
        ["Total Energy Loss (kWh)", total_energy_loss],
        ["Total kWh Charged by Battery", total_charged],
        ["Total kWh Discharged by Battery", total_discharged],
        ["Number of Charge Cycles", charge_cycles],
        ["Payback Period (Years)", round(payback_period_years, 2)]
    ]

    # Write the data side by side with an empty column in between
    for i in range(max(len(non_battery_data), len(battery_data))):
        non_battery_row = non_battery_data[i] if i < len(non_battery_data) else ["", ""]
        battery_row = battery_data[i] if i < len(battery_data) else ["", ""]
        summary_sheet.append([non_battery_row[0], non_battery_row[1], "", battery_row[0], battery_row[1]])

    # Adjust column widths for the 'summary' sheet
    for column in summary_sheet.columns:
        max_length = 0
        column_letter = column[0].column_letter
        for cell in column:
            try:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            except:
                pass
        summary_sheet.column_dimensions[column_letter].width = max_length + 2

    # Add the 'monthly data' sheet
    monthly_sheet = workbook.create_sheet(title="monthly data")

    # Write the header for the 'monthly data' sheet
    monthly_sheet.append([
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
        "Net Monthly Costs (€)",
        "Heatpump-Adjusted Costs (€)"
    ])

    # Write the monthly breakdown data
    for month, data in monthly_breakdown.items():
        net_monthly_costs = data["costs"] - data["income"]
        monthly_sheet.append([
            month,
            data["costs"],
            data["income"],
            data["consumption"],
            data["production"],
            data["battery_adjusted_costs"],
            data["battery_adjusted_income"],
            data["fixed_supply_costs"],
            data["transport_costs"],
            data["energy_tax_compensation"],
            net_monthly_costs,
            monthly_savings.get(month, 0)  # New column: heatpump-adjusted-costs
        ])

    # Adjust column widths for the 'monthly data' sheet
    for column in monthly_sheet.columns:
        max_length = 0
        column_letter = column[0].column_letter
        for cell in column:
            try:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            except:
                pass
        monthly_sheet.column_dimensions[column_letter].width = max_length + 2

    # Add the 'hourly data' sheet
    hourly_sheet = workbook.create_sheet(title="hourly data")

    # Write the header for the 'hourly data' sheet
    header = [
        "Date + Hour", 
        "Production (kWh)", 
        "Adjusted Production (kWh)", 
        "Consumption (kWh)", 
        "Original Heatpump Consumption (kWh)",  # <-- Add this
        "Corrected Heatpump Consumption (kWh)", # <-- Add this
        "Hourly Energy Price (Consumption €/kWh)", 
        "Hourly Energy Price (Production €/kWh)", 
        "Outside Temperature (°C)",
        "Average Outside Temperature (°C)",
        "Final Heatpump Consumption (kWh)",
        "Heatpump Stopped (Yes/No)",
        "Is Top N Most Expensive Hour",
        "Cost (Non-Battery)",
        "Income (Non-Battery)",
    ]

    if config["BATTERY_SIMULATION"]["ENABLE"]:
        header.extend([
            "Cost (Battery)",
            "Income (Battery)",
            "Simulated Consumption (kWh)", 
            "Simulated Production (kWh)", 
            "Battery-Adjusted Total Cost/Income (€)",
            "Consumption Adjusted", 
            "Production Adjusted",
            "Battery Action",
            "Battery kWh",
            "State of Charge (%)"
        ])
    hourly_sheet.append(header)

    # --- Calculate Top N Most Expensive Hours Per Day ---
    stop_hours = int(config["HEATPUMP"].get("STOP_HEATPUMP_ON_NUMBER_OF_MOST_EXPENSIVE_HOURS", 0))
    from collections import defaultdict
    daily_prices = defaultdict(list)
    for idx, record in enumerate(hourly_data):
        day = record["timestamp"][:10]
        daily_prices[day].append((idx, record["price_consumption"]))

    top_n_indices = set()
    for day, price_list in daily_prices.items():
        # Sort by price descending, get indices of top N
        top_n = sorted(price_list, key=lambda x: x[1], reverse=True)[:stop_hours]
        for idx, _ in top_n:
            top_n_indices.add(idx)

    # Write the hourly data
    for idx, record in enumerate(hourly_data):
        non_battery_cost = record["consumption"] * record["price_consumption"]
        non_battery_income = record["adjusted_production"] * record["price_production"]

        is_top_n = 1 if idx in top_n_indices else 0

        row = [
            record.get("timestamp", ""),
            record.get("production", 0),
            record.get("adjusted_production", 0),
            record.get("consumption", 0),
            record.get("original_heatpump_consumption", ""),   # <-- Add this
            record.get("corrected_heatpump_consumption", ""),  # <-- Add this
            record.get("price_consumption", 0),
            record.get("price_production", 0),
            record.get("outside_temp", ""),
            record.get("avg_outside_temp", ""),
            record.get("heatpump_consumption_final", ""),
            "Yes" if record.get("heatpump_stopped") else "No",
            is_top_n,
            round(non_battery_cost, 2),
            round(non_battery_income, 2)
        ]

        if config["BATTERY_SIMULATION"]["ENABLE"]:
            battery_cost = (
                record["simulated_consumption"] * record["price_consumption"]
                if record["simulated_consumption"] is not None
                else 0
            )
            battery_income = (
                record["simulated_production"] * record["price_production"]
                if record["simulated_production"] is not None
                else 0
            )
            battery_action = "Idle"
            battery_kwh = 0
            if record.get("battery_charge", 0) >= 0.1:
                battery_action = "Charged"
                battery_kwh = record["battery_charge"]
            elif record.get("battery_discharge", 0) >= 0.1:
                battery_action = "Discharged"
                battery_kwh = record["battery_discharge"]

            row.extend([
                round(battery_cost, 2),
                round(battery_income, 2),
                record.get("simulated_consumption"),
                record.get("simulated_production"),
                record.get("battery_total_cost_or_income"),
                record.get("consumption_adjusted"),
                record.get("production_adjusted"),
                battery_action,
                battery_kwh,
                round(record.get("state_of_charge", 0), 2)
            ])

        hourly_sheet.append(row)

    # Adjust column widths for the 'hourly data' sheet
    for column in hourly_sheet.columns:
        max_length = 0
        column_letter = column[0].column_letter
        for cell in column:
            try:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            except:
                pass
        hourly_sheet.column_dimensions[column_letter].width = max_length + 2

    # Add the 'prices' sheet
    prices_sheet = workbook.create_sheet(title="prices")

    # Determine the range for energy prices dynamically
    consumption_prices = [record["price_consumption"] for record in hourly_data]
    production_prices = [record["price_production"] for record in hourly_data]

    # Find the minimum and maximum prices across both consumption and production
    min_price = min(min(consumption_prices), min(production_prices))
    max_price = max(max(consumption_prices), max(production_prices))

    # Create bins for energy prices (20 bins maximum)
    num_bins = 20
    bin_size = (max_price - min_price) / num_bins
    bins = [min_price + i * bin_size for i in range(num_bins + 1)]

    def bin_data(data, bins):
        binned_data = [0] * (len(bins) - 1)
        for value in data:
            for i in range(len(bins) - 1):
                if bins[i] <= value < bins[i + 1]:
                    binned_data[i] += 1
                    break
        return binned_data

    # Bin the data
    binned_consumption = bin_data(consumption_prices, bins)
    binned_production = bin_data(production_prices, bins)

    # Write binned data to the 'prices' sheet
    prices_sheet.append(["Price Range (Consumption)", "kWh Bought", "Price Range (Production)", "kWh Sold"])
    for i in range(num_bins):
        consumption_range = f"{bins[i]:.2f} - {bins[i + 1]:.2f}"
        production_range = f"{bins[i]:.2f} - {bins[i + 1]:.2f}"
        prices_sheet.append([
            consumption_range,
            binned_consumption[i],
            production_range,
            binned_production[i]
        ])

    # Create a bar chart for consumption
    consumption_chart = BarChart()
    consumption_chart.title = "Distribution of Energy Prices (Consumption)"
    consumption_chart.x_axis.title = "Price Range (€/kWh)"
    consumption_chart.y_axis.title = "kWh Bought"
    consumption_data = Reference(prices_sheet, min_col=2, min_row=2, max_row=num_bins + 1)
    consumption_categories = Reference(prices_sheet, min_col=1, min_row=2, max_row=num_bins + 1)
    consumption_chart.add_data(consumption_data, titles_from_data=False)
    consumption_chart.set_categories(consumption_categories)

    # Position the consumption chart in column E
    prices_sheet.add_chart(consumption_chart, "E2")

    # Create a bar chart for production
    production_chart = BarChart()
    production_chart.title = "Distribution of Energy Prices (Production)"
    production_chart.x_axis.title = "Price Range (€/kWh)"
    production_chart.y_axis.title = "kWh Sold"
    production_data = Reference(prices_sheet, min_col=4, min_row=2, max_row=num_bins + 1)
    production_categories = Reference(prices_sheet, min_col=3, min_row=2, max_row=num_bins + 1)
    production_chart.add_data(production_data, titles_from_data=False)
    production_chart.set_categories(production_categories)

    # Position the production chart in column E below the consumption chart
    prices_sheet.add_chart(production_chart, "E20")

    # Generate a human-readable file name
    year = config["PARAMETERS"]["START_DATE"].split("-")[0]  # Extract the year from the start date
    datetime_now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")  # Current date and time
    salderen = "yes" if config["PARAMETERS"].get("SALDEREN", False) else "no"
    battery_size = config["BATTERY_SIMULATION"]["BATTERY_SIZE_KWH"] if config["BATTERY_SIMULATION"]["ENABLE"] else "no-battery"
    file_name = f"{year}_salderen_{salderen}_battery_{battery_size}kWh_{datetime_now}.xlsx"

    # Save the Excel file
    results_folder = "results"
    os.makedirs(results_folder, exist_ok=True)
    excel_filename = os.path.join(results_folder, file_name)
    workbook.save(excel_filename)

    print(f"Results written to {excel_filename}")

def convert_raw_list_to_dict(raw_list):
    """
    Convert a list of dicts (with 'statistic_id', 'd', 'value') to a dict of dicts:
    {sensor_id: {timestamp: value, ...}, ...}
    """
    result = {}
    for record in raw_list:
        sensor = record.get("statistic_id")
        ts = record.get("d")
        val = record.get("value")
        if sensor and ts is not None:
            if sensor not in result:
                result[sensor] = {}
            result[sensor][ts] = val
    return result

def main():
    # Fetch sensor data from export.json or VictoriaMetrics
    use_export_json = config["DATA"].get("USE_EXPORT_JSON", True)
    sensor_start_date = f"{START_DATE}T00:00:00Z"
    sensor_end_date = f"{END_DATE}T23:59:59Z"

    # Prepare sensor objects from config
    consumption_sensor_objs = config["CONSUMPTION_SENSORS"]
    production_sensor_objs = config["PRODUCTION_SENSORS"]
    heatpump_sensor_objs = config["HEATPUMP"]["HEATPUMP_SENSORS"]
    heatpump_sensor_ids = [s["sensor"] for s in heatpump_sensor_objs]
    all_consumption_sensor_objs = [s for s in consumption_sensor_objs if s["sensor"] not in heatpump_sensor_ids]

    # Try to load raw data from JSON files first
    raw_consumption_path = os.path.join(script_dir, "data", "raw_consumption_data.json")
    raw_production_path = os.path.join(script_dir, "data", "raw_production_data.json")
    raw_heatpump_path = os.path.join(script_dir, "data", "raw_heatpump_data.json")

    def load_json_data(filepath):
        try:
            with open(filepath, "r") as f:
                return json.load(f)
        except Exception:
            return None

    # Try to load raw data
    raw_consumption_data = load_json_data(raw_consumption_path)
    raw_production_data = load_json_data(raw_production_path)
    raw_heatpump_data = load_json_data(raw_heatpump_path)

    # Convert list to dict-of-dicts if needed
    def convert_if_needed(raw_data):
        if raw_data and isinstance(raw_data, list):
            return convert_raw_list_to_dict(raw_data)
        elif raw_data:
            return raw_data
        else:
            return None

    consumption_data = convert_if_needed(raw_consumption_data)
    production_data = convert_if_needed(raw_production_data)
    heatpump_data = convert_if_needed(raw_heatpump_data)

    # Fetch data if not loaded from file
    if consumption_data is None or production_data is None or heatpump_data is None:
        if use_export_json:
            if consumption_data is None:
                print("Fetching consumption data from export.json")
                consumption_data = fetch_sensor_data_from_json(
                    config["DATA"].get("EXPORT_JSON_PATH", "data/export.json"),
                    START_DATE, END_DATE, [s["sensor"] for s in all_consumption_sensor_objs]
                )
                print("Consumption data fetched from export.json.")

            if production_data is None:
                print("Fetching production data from export.json")
                production_data = fetch_sensor_data_from_json(
                    config["DATA"].get("EXPORT_JSON_PATH", "data/export.json"),
                    START_DATE, END_DATE, [s["sensor"] for s in production_sensor_objs]
                )
                print("Production data fetched from export.json.")

            if heatpump_data is None:
                print("Fetching heatpump data from export.json")
                heatpump_data = fetch_sensor_data_from_json(
                    config["DATA"].get("EXPORT_JSON_PATH", "data/export.json"),
                    START_DATE, END_DATE, [s["sensor"] for s in heatpump_sensor_objs]
                )
                print("Heatpump data fetched from export.json.")
        else:
            if consumption_data is None:
                print(f"Fetching consumption data from VictoriaMetrics from {sensor_start_date} to {sensor_end_date}")
                consumption_data = fetch_sensor_data_victoriametrics(
                    all_consumption_sensor_objs, sensor_start_date, sensor_end_date, "raw_consumption_data.json"
                )
                print("Consumption data fetched and saved to raw_consumption_data.json.")

            if production_data is None:
                print("Fetching production data from VictoriaMetrics")
                production_data = fetch_sensor_data_victoriametrics(
                    production_sensor_objs, sensor_start_date, sensor_end_date, "raw_production_data.json"
                )
                print("Production data fetched and saved to raw_production_data.json.")

            if heatpump_data is None:
                print("Fetching heatpump data from VictoriaMetrics (per-sensor data_gap_fill)")
                heatpump_data = fetch_sensor_data_victoriametrics(
                    heatpump_sensor_objs, sensor_start_date, sensor_end_date, "raw_heatpump_data.json"
                )
                print("Heatpump data fetched and saved to raw_heatpump_data.json.")

    # Merge heatpump_data into consumption_data for downstream processing
    for sensor in heatpump_sensor_ids:
        if sensor not in consumption_data and sensor in heatpump_data:
            consumption_data[sensor] = heatpump_data[sensor]

    # Fetch dynamic prices
    price_data = fetch_dynamic_prices(START_DATE, END_DATE)

    # --- Only use the combined result for calculations ---
    # For consumption and production, use the "combined_counter" key in your calculations
    # If not present, fallback to the first available key (for single sensors)
    combined_consumption_key = "combined_counter" if "combined_counter" in consumption_data else next(iter(consumption_data.keys()), None)
    combined_production_key = "combined_counter" if "combined_counter" in production_data else next(iter(production_data.keys()), None)

    # Prepare new dicts with only the combined result for calculations
    consumption_data_combined = {combined_consumption_key: consumption_data[combined_consumption_key]} if combined_consumption_key else {}
    production_data_combined = {combined_production_key: production_data[combined_production_key]} if combined_production_key else {}

    # --- Pass the full dicts to calculate_costs so all sensor data is available ---
    (
        total_costs,
        total_income,
        total_consumption,
        total_production,
        monthly_breakdown,
        battery_adjusted_costs,
        battery_adjusted_income,
        hourly_data,
        total_energy_loss,
        total_charged,
        total_discharged,
        charge_cycles,
        monthly_savings
    ) = calculate_costs(consumption_data, production_data, price_data)

    # Write results to an Excel file
    write_results_to_excel(
        total_costs,
        total_income,
        total_consumption,
        total_production,
        monthly_breakdown,
        battery_adjusted_costs,
        battery_adjusted_income,
        hourly_data,
        total_energy_loss,
        total_charged,
        total_discharged,
        charge_cycles,
        monthly_savings
    )

if __name__ == "__main__":
    main()