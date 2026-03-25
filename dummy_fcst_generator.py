"""
dummy_fcst_generator.py
========================
Synthetic data generator for the EU Logistics Forecast Health-Check pipeline.

PURPOSE
-------
Produces two CSV files that mimic the structure of real Amazon EU
transportation forecast exports and their corresponding actuals. These files
are used to test the health-check pipeline end-to-end without exposing any
real operational data.

OUTPUT FILES
------------
weekly_forecast_data.csv
    Two versions of the weekly forecast (v_current, v_prior) for ~1,500
    routes across 7 forecasted dates.
    Columns: version, route, date, qty, volume, country, lane_type

recent_actuals.csv
    Actual shipment volumes recorded per route and date, used to validate
    whether the forecast is grounded in recent demand reality.
    Columns: route, date, actual_qty, actual_volume

INJECTED ANOMALIES
------------------
Two deliberate anomalies are "rigged" into the data so that the health-check
pipeline has real signals to detect:

    1. VARIANCE SPIKE (Germany · Rail)
       All German Rail routes receive a +40–50% uplift in v_current vs
       v_prior, simulating a sudden demand shift (e.g. modal shift from road).

    2. REALITY GAP (Air lanes)
       All Air routes have actuals that are 20–30% below the prior forecast,
       simulating systematic Air over-forecasting (e.g. post-COVID air demand
       collapse not yet reflected in the statistical model).

USAGE
-----
    python dummy_fcst_generator.py           # standalone
    import dummy_fcst_generator              # called automatically by master.py
    dummy_fcst_generator.generate_massive_logistics_data()
"""

import pandas as pd
import random
from datetime import datetime, timedelta


def generate_massive_logistics_data():
    """
    Generate synthetic EU logistics forecast and actuals CSVs.

    Network topology
    ----------------
    7 countries × 5-6 cities each → up to 1,500 unique origin-destination
    routes. Lane types are assigned at route creation with a realistic
    modal split: Road 60%, Rail/Air/Sea 40% combined.

    Data volume
    -----------
    Each route × 7 dates × 2 versions = ~21,000 forecast rows.
    Each route × 7 dates (~88% coverage) = ~9,000 actuals rows.

    Noise model
    -----------
    Normal routes:   week-on-week change ±5%, actuals deviation ±3%.
    Anomalous routes: see module docstring above.
    """
    print("Generating enterprise-scale logistics data...")

    # 1. Base network definitions (Major European Hubs)
    # --------------------------------------------------
    # Maps country → list of major city hubs used as route endpoints.
    cities = {
        "Germany": ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne", "Stuttgart"],
        "France": ["Paris", "Marseille", "Lyon", "Toulouse", "Nice", "Nantes"],
        "Spain": ["Madrid", "Barcelona", "Valencia", "Seville", "Zaragoza", "Malaga"],
        "Italy": ["Rome", "Milan", "Naples", "Turin", "Palermo", "Genoa"],
        "UK": ["London", "Birmingham", "Manchester", "Glasgow", "Liverpool", "Bristol"],
        "Netherlands": ["Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Eindhoven"],
        "Poland": ["Warsaw", "Krakow", "Lodz", "Wroclaw", "Poznan"]
    }
    
    lane_types = ["Road", "Rail", "Air", "Sea"]

    # 7 consecutive forecasted delivery dates starting 2026-04-01
    dates = [(datetime(2026, 4, 1) + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]

    # 2. Generate ~1500 Unique Routes
    # --------------------------------
    # Routes are identified by "OriginCity-DestinationCity" strings.
    # Origin country is recorded for regional aggregation in the analysis.
    master_routes = {}
    while len(master_routes) < 1500:
        orig_country = random.choice(list(cities.keys()))
        dest_country = random.choice(list(cities.keys()))
        orig_city = random.choice(cities[orig_country])
        dest_city = random.choice(cities[dest_country])
        
        # Skip self-routes (same origin and destination city)
        if orig_city == dest_city: 
            continue
            
        route_name = f"{orig_city}-{dest_city}"
        
        # Bias towards Road (60%), then distribute the rest
        lane = "Road" if random.random() < 0.6 else random.choice(["Rail", "Air", "Sea"])
        
        master_routes[route_name] = {
            "route": route_name,
            "country": orig_country,  # We track Origin Country for the analysis
            "lane_type": lane
        }

    master_routes = list(master_routes.values())
    
    forecast_data = []
    actuals_data = []

    # 3. Populate Data with Realistic Overlaps and Rigged Anomalies
    # --------------------------------------------------------------
    # Not every route appears in both datasets, mimicking real-world
    # data coverage gaps (new routes, data ingestion failures, etc.).
    for r in master_routes:
        # Probabilistic overlap: 
        # ~92% of routes have forecasts, ~88% have actuals
        # This guarantees some lanes only exist in one of the tables.
        has_forecast = random.random() < 0.92
        has_actuals  = random.random() < 0.88
        
        # Failsafe to ensure it goes into at least one dataset
        if not has_forecast and not has_actuals:
            has_forecast = True

        for date in dates:
            # Baseline volume for this route on this date (50–3,500 packages)
            base_qty = random.randint(50, 3500)
            
            # ── FORECAST GENERATION ──────────────────────────────────────
            if has_forecast:
                # v_prior: use the baseline volume as-is (last week's forecast)
                v_prior_qty = base_qty
                
                # RIGGED VARIANCE ANOMALY:
                # All German Rail routes spike by ~45% in the new version,
                # simulating an unexpected modal shift or model recalibration.
                if r["country"] == "Germany" and r["lane_type"] == "Rail":
                    v_current_qty = int(base_qty * random.uniform(1.40, 1.50))
                else:
                    # Normal week-over-week noise (+/- 5%) for all other routes
                    v_current_qty = int(base_qty * random.uniform(0.95, 1.05))

                # Append v_prior row
                forecast_data.append({
                    "version": "v_prior",
                    "route": r["route"],
                    "date": date,
                    "qty": v_prior_qty,
                    "volume": round(v_prior_qty * 1.5, 2),  # Assuming 1.5 cubic volume per package
                    "country": r["country"],
                    "lane_type": r["lane_type"]
                })
                
                # Append v_current row
                forecast_data.append({
                    "version": "v_current",
                    "route": r["route"],
                    "date": date,
                    "qty": v_current_qty,
                    "volume": round(v_current_qty * 1.5, 2),
                    "country": r["country"],
                    "lane_type": r["lane_type"]
                })

            # ── ACTUALS GENERATION ───────────────────────────────────────
            if has_actuals:
                # RIGGED REALITY ANOMALY:
                # All Air routes underperformed the forecast by ~25%,
                # simulating systematic Air over-forecasting not yet corrected
                # in the statistical model.
                if r["lane_type"] == "Air":
                    actual_qty = int(base_qty * random.uniform(0.70, 0.80))
                else:
                    # Normal reality vs forecast noise (+/- 3%) for other routes
                    actual_qty = int(base_qty * random.uniform(0.97, 1.03))

                actuals_data.append({
                    "route": r["route"],
                    "date": date,
                    "actual_qty": actual_qty,
                    "actual_volume": round(actual_qty * 1.5, 2)
                })

    # 4. Save to CSV
    # ---------------
    df_forecasts = pd.DataFrame(forecast_data)
    df_actuals   = pd.DataFrame(actuals_data)

    df_forecasts.to_csv("weekly_forecast_data.csv", index=False)
    df_actuals.to_csv("recent_actuals.csv", index=False)
    
    print(f"✅ Created 'weekly_forecast_data.csv' ({len(df_forecasts):,} rows)")
    print(f"✅ Created 'recent_actuals.csv' ({len(df_actuals):,} rows)")
    print(f"📊 Total Unique Routes: {len(master_routes)}")
    print("🎯 Anomalies Injected: Germany (Rail) Variance Spike, Air Lane Reality Drop.")


if __name__ == "__main__":
    # Run standalone to regenerate the CSV files independently of the pipeline
    generate_massive_logistics_data()