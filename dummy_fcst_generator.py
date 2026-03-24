import pandas as pd
import random
from datetime import datetime, timedelta

def generate_massive_logistics_data():
    print("Generating enterprise-scale logistics data...")

    # 1. Base network definitions (Major European Hubs)
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
    dates = [(datetime(2026, 4, 1) + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]

    # 2. Generate ~1500 Unique Routes
    master_routes = {}
    while len(master_routes) < 1500:
        orig_country = random.choice(list(cities.keys()))
        dest_country = random.choice(list(cities.keys()))
        orig_city = random.choice(cities[orig_country])
        dest_city = random.choice(cities[dest_country])
        
        if orig_city == dest_city: 
            continue
            
        route_name = f"{orig_city}-{dest_city}"
        
        # Bias towards Road (60%), then distribute the rest
        lane = "Road" if random.random() < 0.6 else random.choice(["Rail", "Air", "Sea"])
        
        master_routes[route_name] = {
            "route": route_name,
            "country": orig_country, # We track Origin Country for the analysis
            "lane_type": lane
        }

    master_routes = list(master_routes.values())
    
    forecast_data = []
    actuals_data = []

    # 3. Populate Data with Realistic Overlaps and Rigged Anomalies
    for r in master_routes:
        # Probabilistic overlap: 
        # ~92% of routes have forecasts, ~88% have actuals
        # This guarantees some lanes only exist in one of the tables.
        has_forecast = random.random() < 0.92
        has_actuals = random.random() < 0.88
        
        # Failsafe to ensure it goes into at least one dataset
        if not has_forecast and not has_actuals:
            has_forecast = True

        for date in dates:
            # Baseline volume for this route on this date
            base_qty = random.randint(50, 3500)
            
            # --- FORECAST GENERATION ---
            if has_forecast:
                v_prior_qty = base_qty
                
                # RIGGED VARIANCE ANOMALY: All German Rail routes spike by ~45% in the new version
                if r["country"] == "Germany" and r["lane_type"] == "Rail":
                    v_current_qty = int(base_qty * random.uniform(1.40, 1.50))
                else:
                    # Normal week-over-week noise (+/- 5%)
                    v_current_qty = int(base_qty * random.uniform(0.95, 1.05))

                # Append v_prior
                forecast_data.append({
                    "version": "v_prior",
                    "route": r["route"],
                    "date": date,
                    "qty": v_prior_qty,
                    "volume": round(v_prior_qty * 1.5, 2), # Assuming 1.5 cubic volume per package
                    "country": r["country"],
                    "lane_type": r["lane_type"]
                })
                
                # Append v_current
                forecast_data.append({
                    "version": "v_current",
                    "route": r["route"],
                    "date": date,
                    "qty": v_current_qty,
                    "volume": round(v_current_qty * 1.5, 2),
                    "country": r["country"],
                    "lane_type": r["lane_type"]
                })

            # --- ACTUALS GENERATION ---
            if has_actuals:
                # RIGGED REALITY ANOMALY: All Air routes underperformed the prior forecast by ~25%
                if r["lane_type"] == "Air":
                    actual_qty = int(base_qty * random.uniform(0.70, 0.80))
                else:
                    # Normal reality vs forecast noise (+/- 3%)
                    actual_qty = int(base_qty * random.uniform(0.97, 1.03))

                actuals_data.append({
                    "route": r["route"],
                    "date": date,
                    "actual_qty": actual_qty,
                    "actual_volume": round(actual_qty * 1.5, 2)
                })

    # 4. Save to CSV
    df_forecasts = pd.DataFrame(forecast_data)
    df_actuals = pd.DataFrame(actuals_data)

    df_forecasts.to_csv("weekly_forecast_data.csv", index=False)
    df_actuals.to_csv("recent_actuals.csv", index=False)
    
    print(f"✅ Created 'weekly_forecast_data.csv' ({len(df_forecasts):,} rows)")
    print(f"✅ Created 'recent_actuals.csv' ({len(df_actuals):,} rows)")
    print(f"📊 Total Unique Routes: {len(master_routes)}")
    print("🎯 Anomalies Injected: Germany (Rail) Variance Spike, Air Lane Reality Drop.")

if __name__ == "__main__":
    generate_massive_logistics_data()