# Dynamic Energy Prices

This project calculates energy costs and income based on dynamic energy prices, consumption, and production data. It includes features such as monthly breakdowns, configurable taxes, and the ability to stop production during negative energy prices. Additionally, it supports battery simulation with configurable strategies to optimize energy usage and costs.

---

## Table of Contents
- [Configuration](#configuration)
- [How to Run the Script](#how-to-run-the-script)
- [Interpreting the Results](#interpreting-the-results)
- [Features](#features)
- [Folder Structure](#folder-structure)

---

## Configuration

Before running the script, you need to configure the `config.json` file. Below is an explanation of each setting, with details based on the code in `main.py`:

### PARAMETERS

- **`START_DATE`**: The start date for the analysis (format: `YYYY-MM-DD`). Used to filter all data and price queries.
- **`END_DATE`**: The end date for the analysis (format: `YYYY-MM-DD`). Used to filter all data and price queries.
- **`BASELOAD_CONSUMPTION`**: The minimum (base) consumption in kWh per hour. Used for filling gaps or as a fallback.
- **`STOP_PRODUCTION_NEGATIVE_PRICES`**: If `true`, production is stopped (set to zero) during hours when the calculated price for production (including taxes) is negative.
- **`SALDEREN`**: If `true`, enables net metering (salderen) logic for production prices.
- **`DEBUG`**: If `true`, enables debug print statements throughout the script.

### DATA

- **`USE_EXPORT_JSON`**: If `true`, loads sensor data from a local export JSON file. If `false`, fetches data from VictoriaMetrics.
- **`EXPORT_JSON_PATH`**: Path to the export JSON file (default: `data/export.json`).
- **`RAW_PRODUCTION_DATA_SQLITE_CSV`**: Path to raw production data exported from SQLite (optional).
- **`RAW_CONSUMPTION_DATA_SQLITE_CSV`**: Path to raw consumption data exported from SQLite (optional).
- **`VICTORIAMETRICS_URL`**: URL for the VictoriaMetrics API (used if `USE_EXPORT_JSON` is `false`).
- **`DYNAMIC_PRICES_API_URL`**: URL for the dynamic energy prices API.
- **`DYNAMIC_PRICES_API_KEY`**: API key for accessing the dynamic energy prices API.


### Get the data

## Victoria Metrics database on home assistant
Configure the victoria metrics url in the config.json. The data will be downloaded automatically for the PRODUCTION_SENSORS and the CONSUMPTION_SENSORS

- **`VICTORIAMETRICS_URL`**: "http://[home assistant ip]:8428/api/v1/query_range"

#### Download from SQLite Database on Home Assistant

Install the SQLite Web add-on and run the query below. Replace the sensor IDs to retrieve all sensor data required. These sensor names should be identical to the `CONSUMPTION_SENSORS` and `PRODUCTION_SENSORS` in the `config.json`.

```sql
SELECT 
    statistic_id, 
    DATETIME(start_ts, 'unixepoch', 'localtime') AS d, 
    state AS value, 
    state - LAG(state) OVER (PARTITION BY statistic_id ORDER BY start_ts) AS increment, 
    [sum] AS total, 
    unit_of_measurement 
FROM 
    statistics 
INNER JOIN 
    statistics_meta m 
ON 
    m.id = statistics.metadata_id 
WHERE 
    m.statistic_id IN (
        'sensor.energy_consumption_tarif_1', 
        'sensor.energy_consumption_tarif_2', 
        'sensor.energy_production_tarif_1', 
        'sensor.energy_production_tarif_2'
    );
```

Then export the result as json and move it to the data folder as export.json


### Adding Consumption and Production Sensors

You can add your consumption and production sensors as objects in the `CONSUMPTION_SENSORS` and `PRODUCTION_SENSORS` arrays. Each sensor object can have the following parameters:

- **`name`**: A friendly name for the sensor.
- **`sensor`**: The sensor ID as used in your Home Assistant or data source.
- **`type`**: The type of sensor. Use `"counter"` for cumulative energy meters, `"gauge"` for instantaneous values.
- **`data_gap_fill`**: How to fill missing data (e.g., `"1d"` for daily).
- **`resets`** (optional): `"yes"` if the counter resets periodically.

**Example:**
```json
"CONSUMPTION_SENSORS": [
    { "name": "energy_consumption_tarif_1", "sensor": "sensor.energy_consumption_tarif_1", "type": "counter", "data_gap_fill": "1d"},
    { "name": "energy_consumption_tarif_2", "sensor": "sensor.energy_consumption_tarif_2", "type": "counter", "data_gap_fill": "1d"}
],
"PRODUCTION_SENSORS": [
    { "name": "energy_production_tarif_1", "sensor": "sensor.energy_production_tarif_1", "type": "counter", "data_gap_fill": "1d"},
    { "name": "energy_production_tarif_2", "sensor": "sensor.energy_production_tarif_2", "type": "counter", "data_gap_fill": "1d"}   
],
```

### Heatpump Settings

To enable and configure smart heating for your heatpump, use the `HEATPUMP` object in your config. The main options are:

- **`ENABLE_SMART_HEATING`**: Set to `true` to enable shifting heatpump consumption to cheaper hours.
- **`STOP_HEATPUMP_ON_NUMBER_OF_MOST_EXPENSIVE_HOURS`**: Number of most expensive hours per day to stop the heatpump.
- **`FULL_TIME_RUNNING_MINIMUM_THRESHOLD_BASED_ON_OUTSIDE_TEMPERATURE`**: Minimum outside temperature (°C) below which the heatpump always runs.
- **`HEATPUMP_SENSORS`**: List of sensor objects for the heatpump. Each object can have:
  - `name`: Friendly name (e.g., `"HEATPUMP_POWER_OUTPUT_SENSOR"`, `"HEATPUMP_CONSUMPTION_SENSOR"`, `"OUTSIDE_TEMPERATURE_SENSOR"`).
  - `sensor`: The sensor ID.
  - `type`: `"gauge"` or `"counter"`.
  - `data_gap_fill`: How to fill missing data (e.g., `"1d"`).
  - `resets`: `"yes"` if the counter resets (optional).

**Example:**
```json
"HEATPUMP" : {
    "ENABLE_SMART_HEATING": true,
    "STOP_HEATPUMP_ON_NUMBER_OF_MOST_EXPENSIVE_HOURS": 4,
    "FULL_TIME_RUNNING_MINIMUM_THRESHOLD_BASED_ON_OUTSIDE_TEMPERATURE": 0,
    "HEATPUMP_SENSORS": [
        { "name": "HEATPUMP_POWER_OUTPUT_SENSOR", "sensor": "sensor.boiler_compressor_power_output", "type": "gauge",  "data_gap_fill": "1d"},
        { "name": "HEATPUMP_CONSUMPTION_SENSOR", "sensor": "sensor.daily_heatpump_energy_consumption", "type": "counter", "resets": "yes", "data_gap_fill": "1d"},
        { "name": "OUTSIDE_TEMPERATURE_SENSOR", "sensor": "sensor.boiler_outside_temperature", "type": "gauge", "data_gap_fill": "1d"}
    ]
},
```

### Smart Car Charging

The script supports **smart car charging**, which automatically shifts car charging to the cheapest hours of the day based on dynamic energy prices. This feature helps minimize charging costs by prioritizing hours with the lowest (even negative) prices, while respecting the maximum charging rate per hour.

#### How it works

- The total car charging energy per day is calculated.
- Charging is redistributed to the hours with the lowest price, up to the configured maximum charging rate per hour (`MAX_CHARGING_RATE_KWH`).
- The logic ensures that the total annual car charging energy remains unchanged; only the timing is optimized.
- If solar production is available during a charging hour, it is used first before drawing from the grid.

#### Configuration

Configure smart car charging in your `config.json` under the `CAR` section:

```json
"CAR": {
    "ENABLE_SMART_CHARGING": true,
    "MAX_CHARGING_RATE_KWH": 4.4,
    "CAR_SENSORS": [
        { "name": "CAR_BATTERY_CHARGE_POWER", "sensor": "sensor.myenergi_zappi_17051981_energy_used_today", "type": "counter", "resets": "no", "data_gap_fill": "1h"}
    ]
}
```

- **ENABLE_SMART_CHARGING**: Set to `true` to enable smart charging optimization.
- **MAX_CHARGING_RATE_KWH**: The maximum kWh that can be charged per hour.
- **CAR_SENSORS**: List your car charging sensors here.

#### Notes

- Smart car charging only shifts the timing of charging; it does **not** reduce total annual consumption.
- The script ensures that car charging is never double-counted or lost during shifting.
- Charging is always prioritized in the hours with the lowest price, including negative price hours.


### Taxes and Costs

- **`ENERGY_TAX`**: Energy tax per kWh (in euro). Used in price calculations for both consumption and production.
- **`STORAGE_COSTS`**: Storage costs per kWh for consumption (in euro).
- **`STORAGE_COSTS_PRODUCTION`**: Storage costs per kWh for production (in euro, typically negative).
- **`VAT`**: VAT percentage applied to the total price.
- **`FIXED_SUPPLY_COSTS`**: Fixed supply costs per month (in euro).
- **`TRANSPORT_COSTS`**: Transport costs per month (in euro).
- **`ENERGY_TAX_COMPENSATION`**: Energy tax compensation per month (in euro, typically negative).

### Battery Simulation

- **`BATTERY_SIMULATION.ENABLE`**: Set to `true` to enable battery simulation.
- **`BATTERY_SIMULATION.BATTERY_CHARGE_STRATEGY`**: The strategy for charging and discharging the battery. Options:
  - `self-sufficiency`: The battery charges when there is excess production and discharges to meet consumption.
  - `dynamic_cost_optimization`: The battery charges when prices are low and discharges when prices are high.
- **`BATTERY_SIMULATION.BATTERY_NAME`**: Name of the battery (for reference).
- **`BATTERY_SIMULATION.BATTERY_SIZE_KWH`**: The total capacity of the battery in kWh.
- **`BATTERY_SIMULATION.BATTERY_PRICE`**: The price of the battery (used for payback calculation).
- **`BATTERY_SIMULATION.MAX_CHARGING_RATE_KWH`**: The maximum charging rate of the battery in kWh per hour.
- **`BATTERY_SIMULATION.MAX_DISCHARGING_RATE_KWH`**: The maximum discharging rate of the battery in kWh per hour.
- **`BATTERY_SIMULATION.ROUND_TRIP_EFFICIENCY`**: The round-trip efficiency of the battery (e.g., `0.8` for 80% efficiency).
- **`BATTERY_SIMULATION.DISCHARGE_MINIMUM_PERCENTAGE`**: The minimum battery level as a percentage of total capacity (e.g., `10` for 10%).
- **`BATTERY_SIMULATION.CHARGE_MAXIMUM_PERCENTAGE`**: The maximum battery level as a percentage of total capacity (e.g., `90` for 90%).
- **`BATTERY_SIMULATION.DYNAMIC_PRICE_THRESHOLD_LOW`**: The price threshold (€/kWh) below which the battery will charge during the `dynamic_cost_optimization` strategy.
- **`BATTERY_SIMULATION.DYNAMIC_PRICE_THRESHOLD_HIGH`**: The price threshold (€/kWh) above which the battery will discharge during the `dynamic_cost_optimization` strategy.

---

## How to Run the Script

1. **Install Dependencies**:
   - Ensure you have Python 3 installed.
   - Install the required dependencies:
     ```bash
     pip install -r requirements.txt
     ```

2. **Configure `config.json`**:
   - Copy the `config.template.json` file to `config.json`:
     ```bash
     cp config.template.json config.json
     ```
   - Edit the `config.json` file and fill in the required values for your setup.

3. **Prepare Data**:
   - Ensure your data files (from Victoria Metrics or SQLite export) are in the correct location as described above.

4. **Run the Script**:
   - Execute the script:
     ```bash
     python main.py
     ```

5. **View Results**:
   - The results will be saved as a CSV file in the `results` folder with a timestamped filename (e.g., `results/results_20250407_123456.csv`).

---

## Interpreting the Results

The script generates a CSV file with the following sections:

### Total Metrics
- **Total Costs (€)**: The total energy costs for the specified period.
- **Total Income (€)**: The total income from energy production for the specified period.
- **Battery-Adjusted Costs (€)**: The total costs adjusted for battery usage.
- **Battery-Adjusted Income (€)**: The total income adjusted for battery usage.
- **Total Consumption (kWh)**: The total energy consumed during the specified period.
- **Total Production (kWh)**: The total energy produced during the specified period.

### Monthly Breakdown
The monthly breakdown includes the following columns:
- **Month**: The calendar month (e.g., `2024-12`).
- **Costs (€)**: The total costs for the month.
- **Income (€)**: The total income for the month.
- **Consumption (kWh)**: The total energy consumed during the month.
- **Production (kWh)**: The total energy produced during the month.
- **Battery-Adjusted Costs (€)**: The total costs adjusted for battery usage during the month.
- **Battery-Adjusted Income (€)**: The total income adjusted for battery usage during the month.
- **Fixed Supply Costs (€)**: The fixed supply costs for the month.
- **Transport Costs (€)**: The transport costs for the month.
- **Energy Tax Compensation (€)**: The energy tax compensation for the month.
- **Net Monthly Costs (€)**: The total monthly costs minus the income.

---

## Features

1. **Dynamic Energy Prices**:
   - Fetches hourly energy prices from an external API.
   - Includes taxes, storage costs, and VAT in the calculations.

2. **Battery Simulation**:
   - Simulates battery behavior based on the selected strategy:
     - `self-sufficiency`: Focuses on using the battery to meet consumption needs.
     - `dynamic_cost_optimization`: Optimizes battery usage based on energy prices.

3. **Stop Production for Negative Prices**:
   - Stops energy production when prices (including taxes) are negative, if enabled in the configuration.

4. **Smart Heatpump Shifting**:
   - Shifts heatpump consumption to cheaper hours without changing total annual consumption.

5. **Monthly Breakdown**:
   - Provides a detailed breakdown of costs, income, consumption, and production for each calendar month.

6. **CSV Output**:
   - Saves the results in a timestamped CSV file for easy analysis in Excel.

7. **Debugging**:
   - Enables detailed debug print statements when `DEBUG` is set to `true`.

---

## Folder Structure

```plaintext
DynamicEnergyPrices/
├── main.py               # Main script
├── config.json           # Configuration file
├── requirements.txt      # Python dependencies
├── results/              # Folder for output CSV files
├── README.md             # Documentation
└── other_files/          # Additional scripts or utilities
```

---

## Troubleshooting

- **Total consumption does not match expectations:**  
  Ensure that your data files are complete and that the configuration matches your sensor IDs. If using smart heating or battery simulation, the script should only shift or buffer energy, not reduce total annual consumption.
- **API errors or missing data:**  
  Check your API URLs, keys, and network connectivity.
- **Debugging:**  
  Set `"DEBUG": true` in your `config.json` to enable detailed output.

If you encounter issues, please check the debug output and review your configuration and data files.