"""
Standalone test for fetch_battery_simulation_data + calculate_optimal_battery_sizing.

Usage:
    python test_battery_sizing.py

The script reads VICTORIAMETRICS_URL and BATTERY_SIZING from config.json if it exists,
otherwise it falls back to the values hard-coded below.
"""

import json
import os
import requests
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Defaults – edit these if you do not have a config.json yet
# ---------------------------------------------------------------------------
DEFAULT_VICTORIAMETRICS_URL = "http://<homeassistant-ip>:8428/api/v1/query_range"

DEFAULT_BATTERY_SIZING = {
    "SIMULATIONS": [
        {
            "name": "22.1kWh_5kW",
            "prefix": "battery_20w",
            "size_kwh": 22.1,
            "inverter_kw": 5.0,
            "price_eur": 8000,
        },
        {
            "name": "10.2kWh_5.7kW",
            "prefix": "battery_sim_byd_battery_box_hvs_10_2kwh",
            "size_kwh": 10.2,
            "inverter_kw": 5.7,
            "price_eur": 4500,
        },
        {
            "name": "5.1kWh_5.7kW",
            "prefix": "battery_sim_byd_battery_box_hvs_5_1kwh",
            "size_kwh": 5.1,
            "inverter_kw": 5.7,
            "price_eur": 2500,
        },
    ]
}

# Test date range – last 30 days is a reasonable default
DEFAULT_START_DATE = "2026-04-18"
DEFAULT_END_DATE   = "2026-05-18"

# ---------------------------------------------------------------------------
# Load config.json if present
# ---------------------------------------------------------------------------
script_dir  = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, "config.json")

if os.path.exists(config_path):
    with open(config_path) as f:
        config = json.load(f)
    VICTORIAMETRICS_URL = config["DATA"]["VICTORIAMETRICS_URL"]
    BATTERY_SIZING      = config.get("BATTERY_SIZING", DEFAULT_BATTERY_SIZING)
    START_DATE = config["PARAMETERS"].get("START_DATE", DEFAULT_START_DATE)
    END_DATE   = config["PARAMETERS"].get("END_DATE",   DEFAULT_END_DATE)
    print(f"Loaded config from {config_path}")
else:
    VICTORIAMETRICS_URL = DEFAULT_VICTORIAMETRICS_URL
    BATTERY_SIZING      = DEFAULT_BATTERY_SIZING
    START_DATE = DEFAULT_START_DATE
    END_DATE   = DEFAULT_END_DATE
    print("config.json not found – using defaults hard-coded in this script.")

DEBUG = True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def debug_print(msg):
    if DEBUG:
        print(msg)


def _percentile(values, pct):
    if not values:
        return 0.0
    s = sorted(values)
    idx = int(len(s) * pct / 100)
    return s[min(idx, len(s) - 1)]


# ---------------------------------------------------------------------------
# check_data_granularity
# ---------------------------------------------------------------------------
def check_data_granularity(hours_back=24):
    """
    Detects the native recording interval stored in VictoriaMetrics and reports
    true instantaneous peak power — not the hourly-average used in sizing.

    Uses /api/v1/export (raw points, no interpolation) to find the actual
    interval between stored data points per sensor.
    """
    simulations = BATTERY_SIZING.get("SIMULATIONS", [])
    if not simulations:
        print("No SIMULATIONS configured.")
        return

    end_dt   = datetime.now(timezone.utc).replace(tzinfo=None)
    start_dt = datetime.fromtimestamp(end_dt.timestamp() - hours_back * 3600, tz=timezone.utc).replace(tzinfo=None)
    start_ts = int(start_dt.timestamp()) * 1000   # VictoriaMetrics export uses ms
    end_ts   = int(end_dt.timestamp())   * 1000

    # Build the export URL (base URL without /query_range)
    base_url = VICTORIAMETRICS_URL.replace("/api/v1/query_range", "").rstrip("/")
    export_url = f"{base_url}/api/v1/export"

    # cumulative=True  → compute per-step deltas from the raw increasing counter
    # cumulative=False → values are already instantaneous (kW gauge)
    GRANULARITY_SENSORS = [
        ("_battery_energy_in",        "kWh/min", True),
        ("_battery_energy_out",       "kWh/min", True),
        ("_current_charging_rate",    "kW",      False),
        ("_current_discharging_rate", "kW",      False),
    ]

    print("\n" + "=" * 80)
    print(f"  DATA GRANULARITY CHECK  (last {hours_back} hours, raw stored points)")
    print(f"  Export URL : {export_url}")
    print("=" * 80)

    for sim in simulations:
        prefix   = sim["prefix"]
        sim_name = sim["name"]
        print(f"\n  Simulation: {sim_name}  (prefix: {prefix})")
        print(f"  {'Sensor':<32} {'Interval':>10} {'Points':>7} {'Max':>8} {'P95':>8} {'P50':>8}  Unit")
        print("  " + "-" * 78)

        for suffix, unit, cumulative in GRANULARITY_SENSORS:
            sensor_id  = f"sensor.{prefix}{suffix}"
            metric_name = f"{sensor_id}_value"

            try:
                response = requests.get(
                    export_url,
                    params={"match[]": metric_name, "start": start_ts, "end": end_ts},
                    timeout=15,
                    stream=True,
                )
                if response.status_code != 200:
                    print(f"  {suffix:<32}  HTTP {response.status_code}: {response.text[:120]}")
                    continue

                # /api/v1/export returns NDJSON — one JSON object per line
                timestamps_all = []
                values_all     = []
                raw_lines = list(response.iter_lines())
                debug_print(f"  {suffix}: {len(raw_lines)} raw lines from export endpoint")
                for line in raw_lines:
                    if not line:
                        continue
                    obj = json.loads(line)
                    timestamps_all.extend(obj.get("timestamps", []))
                    values_all.extend(obj.get("values", []))

                if not timestamps_all:
                    print(f"  {suffix:<32}  no data returned")
                    continue

                # Sort by timestamp
                pairs = sorted(zip(timestamps_all, values_all))
                ts_list = [t for t, _ in pairs]
                val_list = [v for _, v in pairs]

                # Native interval = most common gap between consecutive timestamps (in seconds)
                gaps = [(ts_list[i+1] - ts_list[i]) / 1000
                        for i in range(len(ts_list) - 1)]
                if gaps:
                    from collections import Counter
                    most_common_gap = Counter(int(g) for g in gaps).most_common(1)[0][0]
                    interval_str = f"{most_common_gap}s"
                else:
                    interval_str = "n/a"

                # For cumulative sensors (energy counters), compute per-step deltas
                # so Max/P95 show actual kWh transferred per minute, not running totals.
                if cumulative:
                    analysis_vals = [
                        val_list[i+1] - val_list[i]
                        for i in range(len(val_list) - 1)
                        if val_list[i+1] - val_list[i] > 0
                    ]
                else:
                    analysis_vals = [v for v in val_list if v > 0]

                max_val = max(analysis_vals, default=0)
                p95_val = _percentile(analysis_vals, 95)
                p50_val = _percentile(analysis_vals, 50)

                print(
                    f"  {suffix:<32} {interval_str:>10} {len(pairs):>7}"
                    f" {max_val:>8.4f} {p95_val:>8.4f} {p50_val:>8.4f}  {unit}"
                )

            except requests.exceptions.ConnectionError as e:
                print(f"  {suffix:<32}  connection error: {e}")

    print()
    print("  NOTE: 'Max kW' = true instantaneous peak at native recording interval.")
    print("  If Max >> P95, brief high-power events (oven, kettle) are present.")
    print("  An inverter must handle Max kW to capture 100% of those events.")
    print("=" * 80 + "\n")


# ---------------------------------------------------------------------------
# fetch_battery_simulation_data
# ---------------------------------------------------------------------------
# (suffix, type, gap_fill, step)
# Rate sensors use max_over_time with a 1h window at step=3600s to capture the
# highest rate seen within each hour — better for inverter sizing than last_over_time.
SENSOR_DEFS = [
    ("_total_money_saved",             "counter",   "1d", "3600s"),
    ("_money_saved_on_imports",        "counter",   "1d", "3600s"),
    ("_extra_money_earned_on_exports", "counter",   "1d", "3600s"),
    ("_battery_energy_in",             "counter",   "1d", "3600s"),
    ("_battery_energy_out",            "counter",   "1d", "3600s"),
    ("_current_charging_rate",         "gauge_max", "1h", "3600s"),
    ("_current_discharging_rate",      "gauge_max", "1h", "3600s"),
]


def fetch_battery_simulation_data(start_date, end_date):
    simulations = BATTERY_SIZING.get("SIMULATIONS", [])
    if not simulations:
        print("No SIMULATIONS configured.")
        return {}

    start_ts = int(datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ").timestamp())
    end_ts   = int(datetime.strptime(end_date,   "%Y-%m-%dT%H:%M:%SZ").timestamp())

    results = {}

    for sim in simulations:
        prefix   = sim["prefix"]
        sim_name = sim["name"]
        results[sim_name] = {}
        print(f"\n--- Simulation: {sim_name}  (prefix: {prefix}) ---")

        for suffix, sensor_type, gap_fill, step in SENSOR_DEFS:
            sensor_id = f"sensor.{prefix}{suffix}"

            if sensor_type == "counter":
                query = f'increase(last_over_time({sensor_id}_value[{gap_fill}]))'
            elif sensor_type == "gauge_max":
                query = f'max_over_time({sensor_id}_value[{gap_fill}])'
            else:
                query = f'last_over_time({sensor_id}_value[{gap_fill}])'

            params = {
                "query": query,
                "start": start_ts,
                "end":   end_ts,
                "step":  step,
            }

            print(f"  Query: {query}")
            try:
                response = requests.get(VICTORIAMETRICS_URL, params=params, timeout=10)
                if response.status_code == 200:
                    raw = response.json().get("data", {}).get("result", [])
                    hourly_values = {}
                    for result in raw:
                        for ts, val in result.get("values", []):
                            ts_str = datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H")
                            hourly_values[ts_str] = float(val)
                    results[sim_name][sensor_id] = hourly_values
                    non_zero  = sum(1 for v in hourly_values.values() if v > 0)
                    total_val = sum(v for v in hourly_values.values() if v > 0)
                    sorted_ts = sorted(hourly_values.keys())
                    sample_first = [(t, hourly_values[t]) for t in sorted_ts[:3]]
                    sample_last  = [(t, hourly_values[t]) for t in sorted_ts[-3:]]
                    print(f"  -> {len(hourly_values)} pts | {non_zero} non-zero | sum = {total_val:.3f}")
                    if non_zero > 0:
                        print(f"     first: {sample_first}")
                        print(f"     last : {sample_last}")
                else:
                    print(f"  -> HTTP {response.status_code}: {response.text[:200]}")
                    results[sim_name][sensor_id] = {}
            except requests.exceptions.ConnectionError as e:
                print(f"  -> Connection error: {e}")
                results[sim_name][sensor_id] = {}

    # Save raw data for inspection
    data_folder = os.path.join(script_dir, "data")
    os.makedirs(data_folder, exist_ok=True)
    out_path = os.path.join(data_folder, "raw_battery_sizing_data.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=4)
    print(f"\nRaw data saved to {out_path}")

    return results


# ---------------------------------------------------------------------------
# calculate_optimal_battery_sizing
# ---------------------------------------------------------------------------
def calculate_optimal_battery_sizing(simulation_data, start_date, end_date):
    simulations = BATTERY_SIZING.get("SIMULATIONS", [])
    if not simulations:
        print("No simulations to analyse.")
        return []

    start_dt     = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt       = datetime.strptime(end_date,   "%Y-%m-%d")
    period_years = max((end_dt - start_dt).days / 365.0, 1 / 365)

    analysis = []

    for sim in simulations:
        sim_name    = sim["name"]
        prefix      = sim["prefix"]
        size_kwh    = sim["size_kwh"]
        inverter_kw = sim["inverter_kw"]
        price_eur   = sim.get("price_eur", 0)

        sensors = simulation_data.get(sim_name, {})

        def total(suffix):
            sid = f"sensor.{prefix}{suffix}"
            return sum(v for v in sensors.get(sid, {}).values() if v > 0)

        def vals(suffix):
            sid = f"sensor.{prefix}{suffix}"
            return [v for v in sensors.get(sid, {}).values() if v > 0]

        total_money_saved    = total("_total_money_saved")
        money_saved_imports  = total("_money_saved_on_imports")
        extra_earned_exports = total("_extra_money_earned_on_exports")
        energy_in_kwh        = total("_battery_energy_in")
        energy_out_kwh       = total("_battery_energy_out")

        # Fallback: total_money_saved is unreliable for some batteries (rarely updated).
        # Use money_saved_on_imports + extra_money_earned_on_exports when it's near zero.
        if total_money_saved < money_saved_imports * 0.5:
            total_money_saved = money_saved_imports + extra_earned_exports

        # Rate sensors give peak kW per hour — used only to determine Max and P95 thresholds.
        rate_charge_vals    = vals("_current_charging_rate")
        rate_discharge_vals = vals("_current_discharging_rate")
        # Energy sensors give actual kWh per hour — used for energy sums and Chg@P95 kWh.
        energy_charge_vals    = vals("_battery_energy_in")
        energy_discharge_vals = vals("_battery_energy_out")

        annual_savings  = total_money_saved / period_years
        payback_years   = price_eur / annual_savings if annual_savings > 0 else float("inf")

        # Max kW and P95 kW from rate sensors (fall back to energy if rate unavailable).
        rate_vals_chg = rate_charge_vals    if rate_charge_vals    else energy_charge_vals
        rate_vals_dis = rate_discharge_vals if rate_discharge_vals else energy_discharge_vals

        max_charge_kw    = max(rate_vals_chg, default=0)
        p90_charge_kw    = _percentile(rate_vals_chg, 90)
        p95_charge_kw    = _percentile(rate_vals_chg, 95)
        max_discharge_kw = max(rate_vals_dis, default=0)
        p90_discharge_kw = _percentile(rate_vals_dis, 90)
        p95_discharge_kw = _percentile(rate_vals_dis, 95)

        recommended_inverter_kw = min(
            round(max(p95_charge_kw, p95_discharge_kw) * 2 + 0.5) / 2,
            inverter_kw
        )

        # kWh captured if inverter is limited to P90/P95 rate:
        # sum over hourly energy, clipping any hour where energy_kwh > threshold.
        p90_captured_charge_kwh    = sum(min(v, p90_charge_kw)    for v in energy_charge_vals)
        p90_captured_discharge_kwh = sum(min(v, p90_discharge_kw) for v in energy_discharge_vals)
        p95_captured_charge_kwh    = sum(min(v, p95_charge_kw)    for v in energy_charge_vals)
        p95_captured_discharge_kwh = sum(min(v, p95_discharge_kw) for v in energy_discharge_vals)

        analysis.append({
            "name":                        sim_name,
            "size_kwh":                    size_kwh,
            "inverter_kw":                 inverter_kw,
            "price_eur":                   price_eur,
            "period_years":                round(period_years, 2),
            "total_money_saved_eur":       round(total_money_saved, 2),
            "money_saved_imports_eur":     round(money_saved_imports, 2),
            "extra_earned_exports_eur":    round(extra_earned_exports, 2),
            "annual_savings_eur":          round(annual_savings, 2),
            "payback_years":               round(payback_years, 2) if payback_years != float("inf") else None,
            "energy_in_kwh":               round(energy_in_kwh, 1),
            "energy_out_kwh":              round(energy_out_kwh, 1),
            "max_charge_kw":               round(max_charge_kw, 2),
            "p90_charge_kw":               round(p90_charge_kw, 2),
            "p90_captured_charge_kwh":     round(p90_captured_charge_kwh, 1),
            "p95_charge_kw":               round(p95_charge_kw, 2),
            "p95_captured_charge_kwh":     round(p95_captured_charge_kwh, 1),
            "max_discharge_kw":            round(max_discharge_kw, 2),
            "p90_discharge_kw":            round(p90_discharge_kw, 2),
            "p90_captured_discharge_kwh":  round(p90_captured_discharge_kwh, 1),
            "p95_discharge_kw":            round(p95_discharge_kw, 2),
            "p95_captured_discharge_kwh":  round(p95_captured_discharge_kwh, 1),
            "recommended_inverter_kw":     recommended_inverter_kw,
        })

    print("\n" + "=" * 80)
    print("  BATTERY SIZING ANALYSIS")
    print("=" * 80)
    print(f"  Period : {start_date} → {end_date}  ({period_years:.2f} years)")
    print()

    # Table 1 – energy throughput
    print(f"  {'Simulation':<26} {'Size kWh':>9} {'Charged kWh':>12} {'Discharged kWh':>15}")
    print("  " + "-" * 64)
    for e in analysis:
        print(
            f"  {e['name']:<26} {e['size_kwh']:>9.1f}"
            f" {e['energy_in_kwh']:>12.1f} {e['energy_out_kwh']:>15.1f}"
        )
    print()

    # Table 2 – P90/P95 inverter sizing
    print(f"  {'Simulation':<26} {'Max chg kW':>11} {'P90chg kW':>10} {'Chg@P90 kWh':>12} {'P95chg kW':>10} {'Chg@P95 kWh':>12} {'Max dis kW':>11} {'P90dis kW':>10} {'Dis@P90 kWh':>12} {'P95dis kW':>10} {'Dis@P95 kWh':>12} {'Rec.inv kW':>11}")
    print("  " + "-" * 133)
    for e in analysis:
        print(
            f"  {e['name']:<26}"
            f" {e['max_charge_kw']:>11.2f} {e['p90_charge_kw']:>10.2f} {e['p90_captured_charge_kwh']:>12.1f}"
            f" {e['p95_charge_kw']:>10.2f} {e['p95_captured_charge_kwh']:>12.1f}"
            f" {e['max_discharge_kw']:>11.2f} {e['p90_discharge_kw']:>10.2f} {e['p90_captured_discharge_kwh']:>12.1f}"
            f" {e['p95_discharge_kw']:>10.2f} {e['p95_captured_discharge_kwh']:>12.1f}"
            f" {e['recommended_inverter_kw']:>11.1f}"
        )
    print()

    # Table 3 – financials
    print(f"  {'Simulation':<26} {'Total €':>9} {'Ann.€/yr':>9} {'Payback':>8}")
    print("  " + "-" * 54)
    for e in analysis:
        pb = f"{e['payback_years']:.1f}" if e["payback_years"] is not None else "n/a"
        print(
            f"  {e['name']:<26}"
            f" {e['total_money_saved_eur']:>9.2f} {e['annual_savings_eur']:>9.2f} {pb:>8}"
        )
    print("=" * 80)

    valid = [e for e in analysis if e["payback_years"] is not None]
    if valid:
        best = min(valid, key=lambda x: x["payback_years"])
        print(f"\n  Optimal battery size : {best['size_kwh']} kWh  ({best['name']})")
        print(f"  Annual savings       : €{best['annual_savings_eur']:.2f}")
        print(f"  Payback period       : {best['payback_years']:.1f} years")
        print(f"  Recommended inverter : {best['recommended_inverter_kw']} kW")
        print(f"  (P90 charge {best['p90_charge_kw']} kW / P95 charge {best['p95_charge_kw']} kW  |  P90 dis {best['p90_discharge_kw']} kW / P95 dis {best['p95_discharge_kw']} kW)")
    else:
        print("\n  No savings data – check sensor prefixes and date range.")

    print("=" * 72 + "\n")
    return analysis


# ---------------------------------------------------------------------------
# DSMR smart meter – sensor definitions
# ---------------------------------------------------------------------------
DSMR_ENERGY_SENSORS = [
    # (sensor_id, label)
    ("sensor.energy_consumption_tarif_1", "Consumption T1 (off-peak)"),
    ("sensor.energy_consumption_tarif_2", "Consumption T2 (peak)"),
    ("sensor.energy_production_tarif_1",  "Production  T1 (off-peak)"),
    ("sensor.energy_production_tarif_2",  "Production  T2 (peak)"),
]

DSMR_POWER_SENSORS = [
    # (sensor_id, label, direction)  direction = "consumption" | "production"
    ("sensor.power_consumption",          "Total",    "consumption"),
    ("sensor.power_consumption_phase_l1", "Phase L1", "consumption"),
    ("sensor.power_consumption_phase_l2", "Phase L2", "consumption"),
    ("sensor.power_consumption_phase_l3", "Phase L3", "consumption"),
    ("sensor.power_production",           "Total",    "production"),
    ("sensor.power_production_phase_l1",  "Phase L1", "production"),
    ("sensor.power_production_phase_l2",  "Phase L2", "production"),
    ("sensor.power_production_phase_l3",  "Phase L3", "production"),
]


def fetch_dsmr_data(start_date, end_date):
    """
    Fetch DSMR smart meter data from VictoriaMetrics.

    Energy sensors: daily increase (kWh) sampled hourly.
    Power sensors:  hourly max (kW) – captures within-hour peaks for sizing.

    Returns dict with keys "energy" and "power", each mapping sensor_id -> {ts: value}.
    """
    start_ts = int(datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ").timestamp())
    end_ts   = int(datetime.strptime(end_date,   "%Y-%m-%dT%H:%M:%SZ").timestamp())

    def query_sensor(query, step="3600s"):
        params = {"query": query, "start": start_ts, "end": end_ts, "step": step}
        try:
            r = requests.get(VICTORIAMETRICS_URL, params=params, timeout=10)
            if r.status_code == 200:
                raw = r.json().get("data", {}).get("result", [])
                values = {}
                for result in raw:
                    for ts, val in result.get("values", []):
                        ts_str = datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H")
                        values[ts_str] = float(val)
                return values
            else:
                print(f"  -> HTTP {r.status_code}: {r.text[:120]}")
                return {}
        except requests.exceptions.ConnectionError as e:
            print(f"  -> Connection error: {e}")
            return {}

    result = {"energy": {}, "power": {}, "power_avg": {}}

    print("\n--- DSMR energy sensors ---")
    for sensor_id, label in DSMR_ENERGY_SENSORS:
        q = f'increase(last_over_time({sensor_id}_value[1d]))'
        print(f"  {label:<30}  {q}")
        vals = query_sensor(q)
        result["energy"][sensor_id] = vals
        total = sum(v for v in vals.values() if v > 0)
        print(f"  -> {len(vals)} pts | sum = {total:.1f} kWh")

    print("\n--- DSMR power sensors ---")
    for sensor_id, label, direction in DSMR_POWER_SENSORS:
        # max_over_time -> peak per hour (for Max/P90/P95 columns)
        q_max = f'max_over_time({sensor_id}_value[1h])'
        print(f"  {label:<10} ({direction:<11})  {q_max}")
        vals_max = query_sensor(q_max)
        result["power"][sensor_id] = vals_max
        non_zero = [v for v in vals_max.values() if v > 0]
        peak = max(non_zero, default=0)
        print(f"  -> {len(vals_max)} pts | {len(non_zero)} non-zero | max = {peak:.3f} kW")

        # avg_over_time -> average kW per hour; sum × 1 h = kWh throughput estimate
        q_avg = f'avg_over_time({sensor_id}_value[1h])'
        vals_avg = query_sensor(q_avg)
        result["power_avg"][sensor_id] = vals_avg
        kwh_est = sum(v for v in vals_avg.values() if v > 0)
        print(f"     avg query -> {len(vals_avg)} pts | kWh est = {kwh_est:.1f}")

    data_folder = os.path.join(script_dir, "data")
    os.makedirs(data_folder, exist_ok=True)
    out_path = os.path.join(data_folder, "raw_dsmr_data.json")
    with open(out_path, "w") as f:
        json.dump(result, f, indent=4)
    print(f"\nRaw DSMR data saved to {out_path}")

    return result


def calculate_dsmr_overview(dsmr_data, start_date, end_date):
    """
    Print an overview of DSMR smart meter data:
      1. Energy summary (kWh consumed / produced by tariff)
      2. Power peaks table: Max, P90, P95 per phase and total,
         for both consumption and production
    """
    start_dt     = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt       = datetime.strptime(end_date,   "%Y-%m-%d")
    period_years = max((end_dt - start_dt).days / 365.0, 1 / 365)

    energy     = dsmr_data.get("energy",     {})
    power      = dsmr_data.get("power",      {})
    power_avg  = dsmr_data.get("power_avg",  {})

    def energy_total(sensor_id):
        return sum(v for v in energy.get(sensor_id, {}).values() if v > 0)

    def power_vals(sensor_id):
        return [v for v in power.get(sensor_id, {}).values() if v > 0]

    def power_kwh(sensor_id):
        """kWh through this sensor: sum of hourly average kW values (avg kW × 1 h each)."""
        return sum(v for v in power_avg.get(sensor_id, {}).values() if v > 0)

    # ── Energy totals ────────────────────────────────────────────────────────
    con_t1    = energy_total("sensor.energy_consumption_tarif_1")
    con_t2    = energy_total("sensor.energy_consumption_tarif_2")
    pro_t1    = energy_total("sensor.energy_production_tarif_1")
    pro_t2    = energy_total("sensor.energy_production_tarif_2")
    con_total = con_t1 + con_t2
    pro_total = pro_t1 + pro_t2
    net       = con_total - pro_total

    print("\n" + "=" * 72)
    print("  DSMR SMART METER OVERVIEW")
    print("=" * 72)
    print(f"  Period : {start_date} → {end_date}  ({period_years:.2f} years)")
    print()

    # Table 1 – energy summary
    print(f"  {'Energy summary':<38} {'kWh':>10}  {'kWh/yr':>10}")
    print("  " + "-" * 61)
    rows = [
        ("Consumption T1 (off-peak)",  con_t1),
        ("Consumption T2 (peak)",      con_t2),
        ("Consumption total",          con_total),
        ("Production  T1 (off-peak)",  pro_t1),
        ("Production  T2 (peak)",      pro_t2),
        ("Production  total",          pro_total),
        ("Net (consumption − export)", net),
    ]
    for label, kwh in rows:
        ann = kwh / period_years
        sep = "  " + "-" * 61 if label.startswith("Net") else ""
        if sep:
            print(sep)
        print(f"  {label:<38} {kwh:>10.1f}  {ann:>10.1f}")
    print()

    # Table 2 – power peaks
    con_sensors = [(sid, lbl) for sid, lbl, d in DSMR_POWER_SENSORS if d == "consumption"]
    pro_sensors = [(sid, lbl) for sid, lbl, d in DSMR_POWER_SENSORS if d == "production"]

    # Normalize per-phase avg values so they sum to the total avg for each hour.
    # avg_over_time can over/undercount phases vs total if HA records them at different
    # rates: total may update every 60 s while phases update every 10 s, causing
    # sum(phases) to diverge from total in either direction.
    def build_normalized_avg(total_sid, phase_sids):
        total_dict = power_avg.get(total_sid, {})
        norm = {sid: dict(power_avg.get(sid, {})) for sid in phase_sids}
        for ts, total_val in total_dict.items():
            phase_sum = sum(norm[sid].get(ts, 0.0) for sid in phase_sids)
            if phase_sum > 0:
                scale = total_val / phase_sum
                for sid in phase_sids:
                    if ts in norm[sid]:
                        norm[sid][ts] *= scale
        return norm

    phase_con_sids = [s for s, l, d in DSMR_POWER_SENSORS if d == "consumption" and l != "Total"]
    phase_pro_sids = [s for s, l, d in DSMR_POWER_SENSORS if d == "production"  and l != "Total"]
    norm_con = build_normalized_avg("sensor.power_consumption",  phase_con_sids)
    norm_pro = build_normalized_avg("sensor.power_production",   phase_pro_sids)

    def power_row(sensor_id, label, norm_avg_dict=None):
        """
        Returns (label, max_kw, p90pk_kw, p95pk_kw, p90avg_kw, at_p90_kwh, p95avg_kw, at_p95_kwh).

        Two separate percentile sets:
          pk  = percentile of hourly MAX power  → inverter / cable sizing (must handle peak)
          avg = percentile of hourly AVG power  → @kWh threshold (average kW cap per hour)

        Using avg-based thresholds for @kWh avoids the mismatch where a brief spike raises
        the hourly max above P90 while the hourly average stays well below it, making the
        @kWh difference between Max and P90 caps appear deceptively small.
        """
        max_vals = [v for v in power.get(sensor_id, {}).values() if v > 0]
        if norm_avg_dict is not None:
            avg_vals = [v for v in norm_avg_dict.get(sensor_id, {}).values() if v > 0]
        else:
            avg_vals = [v for v in power_avg.get(sensor_id, {}).values() if v > 0]
        if not max_vals or not avg_vals:
            return None
        mx       = max(max_vals)
        p90pk    = _percentile(max_vals, 90)   # P90 of hourly peaks  → sizing
        p95pk    = _percentile(max_vals, 95)   # P95 of hourly peaks  → sizing
        p90avg   = _percentile(avg_vals, 90)   # P90 of hourly averages → @kWh cap
        p95avg   = _percentile(avg_vals, 95)   # P95 of hourly averages → @kWh cap
        # @X kWh = sum of min(avg_h, X) for every hour:
        # hours where avg_h < X pass all their energy; hours where avg_h >= X are capped.
        at_p90   = sum(min(v, p90avg) for v in avg_vals)
        at_p95   = sum(min(v, p95avg) for v in avg_vals)
        return label, mx, p90pk, p95pk, p90avg, at_p90, p95avg, at_p95

    # Column widths
    W_LBL = 12   # sensor label
    W_KW  = 9    # kW values
    W_KWH = 10   # kWh values

    def sep_line(w): return "  " + "-" * w

    # Sub-table A: peak sizing
    hdr_a = (f"  {'Sensor':<{W_LBL}}"
             f" {'Max kW':>{W_KW}}"
             f" {'P90 kW':>{W_KW}} {'P95 kW':>{W_KW}}")
    # Sub-table B: energy at average-power cap
    hdr_b = (f"  {'Sensor':<{W_LBL}}"
             f" {'P90 cap kW':>{W_KW}} {'@P90 kWh':>{W_KWH}}"
             f" {'P95 cap kW':>{W_KW}} {'@P95 kWh':>{W_KWH}}")

    for direction, sensors, norm_dict in (
        ("consumption", con_sensors, norm_con),
        ("production",  pro_sensors, norm_pro),
    ):
        rows = []
        for sid, lbl in sensors:
            is_total = (lbl == "Total")
            row = power_row(sid, lbl, norm_avg_dict=None if is_total else norm_dict)
            if row is not None:
                rows.append((lbl, is_total, row))

        if not rows:
            continue

        # ── Sub-table A: peak power for inverter / cable sizing ──────────────
        print(f"  {direction.capitalize()} – peak power (inverter / cable sizing)")
        print(hdr_a)
        print(sep_line(len(hdr_a) - 2))
        for lbl, is_total, row in rows:
            label, mx, p90pk, p95pk, p90avg, at_p90, p95avg, at_p95 = row
            tag = "* " if is_total else "  "
            print(f"{tag}{label:<{W_LBL}}"
                  f" {mx:{W_KW}.3f}"
                  f" {p90pk:{W_KW}.3f} {p95pk:{W_KW}.3f}")
        print()

        # ── Sub-table B: energy if average power capped at P90/P95 ───────────
        print(f"  {direction.capitalize()} – energy at average-power cap")
        print(hdr_b)
        print(sep_line(len(hdr_b) - 2))
        phase_rows = []
        for lbl, is_total, row in rows:
            label, mx, p90pk, p95pk, p90avg, at_p90, p95avg, at_p95 = row
            tag = "* " if is_total else "  "
            print(f"{tag}{label:<{W_LBL}}"
                  f" {p90avg:{W_KW}.3f} {at_p90:{W_KWH}.1f}"
                  f" {p95avg:{W_KW}.3f} {at_p95:{W_KWH}.1f}")
            if not is_total:
                phase_rows.append(row)
        if phase_rows:
            print(sep_line(len(hdr_b) - 2))
            s_at_p90 = sum(r[5] for r in phase_rows)
            s_at_p95 = sum(r[7] for r in phase_rows)
            print(f"  {'Sum L1+L2+L3':<{W_LBL}}"
                  f" {'---':>{W_KW}} {s_at_p90:{W_KWH}.1f}"
                  f" {'---':>{W_KW}} {s_at_p95:{W_KWH}.1f}")
        print()

    print("  * Total = whole-house sensor (authoritative).")
    print("  Inverter sizing: P90/P95 kW = 90th/95th percentile of hourly peak power.")
    print("  Energy cap: P90/P95 cap kW = 90th/95th percentile of hourly average power;")
    print("              @kWh = total energy if the average per hour was limited to that cap.")
    print("  Phase @kWh are normalized per-hour to sum to Total.")
    print("=" * 72 + "\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    sensor_start = f"{START_DATE}T00:00:00Z"
    sensor_end   = f"{END_DATE}T23:59:59Z"

    print(f"\nVictoriaMetrics URL : {VICTORIAMETRICS_URL}")
    print(f"Date range          : {START_DATE} → {END_DATE}\n")

    if "<homeassistant-ip>" in VICTORIAMETRICS_URL:
        print("ERROR: VICTORIAMETRICS_URL still contains the placeholder.")
        print("       Set it in config.json or edit DEFAULT_VICTORIAMETRICS_URL in this script.")
        raise SystemExit(1)

    check_data_granularity(hours_back=24)

    data = fetch_battery_simulation_data(sensor_start, sensor_end)
    calculate_optimal_battery_sizing(data, START_DATE, END_DATE)

    dsmr_data = fetch_dsmr_data(sensor_start, sensor_end)
    calculate_dsmr_overview(dsmr_data, START_DATE, END_DATE)
