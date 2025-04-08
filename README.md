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

Before running the script, you need to configure the `config.json` file. Below is an explanation of each setting:

### General Settings
- **`HOME_ASSISTANT_API_URL`**: The URL of your Home Assistant API for fetching historical data.
- **`HOME_ASSISTANT_API_TOKEN`**: The API token for authenticating with Home Assistant.
- **`DYNAMIC_PRICES_API_URL`**: The URL of the dynamic energy prices API.
- **`DYNAMIC_PRICES_API_KEY`**: The API key for accessing the dynamic energy prices API.
- **`START_DATE`**: The start date for the analysis (format: `YYYY-MM-DD`).
- **`END_DATE`**: The end date for the analysis (format: `YYYY-MM-DD`).

### Sensors
- **`CONSUMPTION_SENSORS`**: A list of sensor IDs for energy consumption.
- **`PRODUCTION_SENSORS`**: A list of sensor IDs for energy production.

### Taxes and Costs
- **`ENERGY_TAX`**: Energy tax per kWh (in euro).
- **`STORAGE_COSTS`**: Storage costs per kWh for consumption (in euro).
- **`STORAGE_COSTS_PRODUCTION`**: Storage costs per kWh for production (in euro, typically negative).
- **`VAT`**: VAT percentage applied to the total price.
- **`FIXED_SUPPLY_COSTS`**: Fixed supply costs per month (in euro).
- **`TRANSPORT_COSTS`**: Transport costs per month (in euro).
- **`ENERGY_TAX_COMPENSATION`**: Energy tax compensation per month (in euro, typically negative).

### Battery Simulation
- **`BATTERY_SIMULATION.ENABLE`**: Set to `true` to enable battery simulation.
- **`BATTERY_SIMULATION.BATTERY_SIZE_KWH`**: The total capacity of the battery in kWh.
- **`BATTERY_SIMULATION.MAX_CHARGING_RATE_KWH`**: The maximum charging rate of the battery in kWh per hour.
- **`BATTERY_SIMULATION.MAX_DISCHARGING_RATE_KWH`**: The maximum discharging rate of the battery in kWh per hour.
- **`BATTERY_SIMULATION.ROUND_TRIP_EFFICIENCY`**: The round-trip efficiency of the battery (e.g., `0.96` for 96% efficiency).
- **`BATTERY_SIMULATION.DISCHARGE_LIMIT_PERCENTAGE`**: The minimum battery level as a percentage of total capacity (e.g., `10` for 10%).

### Battery Charge Strategy
- **`BATTERY_CHARGE_STRATEGY`**: The strategy for charging and discharging the battery. Options:
  - `self-sufficiency`: The battery charges when there is excess production and discharges to meet consumption.
  - `dynamic_cost_optimization`: The battery charges when prices are low and discharges when prices are high.
- **`DYNAMIC_PRICE_THRESHOLD_LOW`**: The price threshold (€/kWh) below which the battery will charge during the `dynamic_cost_optimization` strategy.
- **`DYNAMIC_PRICE_THRESHOLD_HIGH`**: The price threshold (€/kWh) above which the battery will discharge during the `dynamic_cost_optimization` strategy.

### Debugging and Features
- **`DEBUG`**: Set to `true` to enable debug print statements.
- **`STOP_PRODUCTION_NEGATIVE_PRICES`**: Set to `true` to stop production when energy prices (including taxes) are negative.

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
   - Edit the [config.json](http://_vscodecontentref_/3) file and fill in the required values.

3. **Run the Script**:
   - Execute the script:
     ```bash
     python main.py
     ```

4. **View Results**:
   - The results will be saved as a CSV file in the [results](http://_vscodecontentref_/4) folder with a timestamped filename (e.g., `results/results_20250407_123456.csv`).

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

4. **Monthly Breakdown**:
   - Provides a detailed breakdown of costs, income, consumption, and production for each calendar month.

5. **CSV Output**:
   - Saves the results in a timestamped CSV file for easy analysis in Excel.

6. **Debugging**:
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