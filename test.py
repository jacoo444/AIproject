import pandas as pd
import numpy as np
import json
import math

# --- CONFIGURATION ---
INPUT_FILE = 'places_database.csv'
OUTPUT_FILE = 'places_database.csv' # We save to a new file to be safe
NEIGHBORS_TO_CONSIDER = 3 # How many nearby places to average

def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate distance (in meters) between two points
    """
    R = 6371e3 # Earth radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def parse_schedule(json_str):
    """Returns a numpy array (7 days x 24 hours) or None if empty"""
    try:
        data = json.loads(json_str)
        if not data or len(data) < 7:
            return None
        
        # Extract just the hourly data into a clean matrix
        # Matrix shape: (7, 24)
        matrix = []
        days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        
        # Sort by day to ensure Monday is index 0
        sorted_data = sorted(data, key=lambda x: days_order.index(x['name']) if x['name'] in days_order else 0)
        
        for day in sorted_data:
            matrix.append(day['data'])
            
        return np.array(matrix)
    except:
        return None

def format_schedule_back_to_json(numpy_matrix):
    """Converts the numpy array back to the JSON format Google/HTML expects"""
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    output = []
    
    for i, day_name in enumerate(days):
        output.append({
            "name": day_name,
            "data": numpy_matrix[i].tolist() # Convert numpy row to list
        })
    return json.dumps(output)

# ---------------------------------------------------------
# MAIN PROCESS
# ---------------------------------------------------------
print(f"--- 🧠 Starting Spatial Imputation for {INPUT_FILE} ---")

# 1. Load Data
df = pd.read_csv(INPUT_FILE)
print(f"Total Places: {len(df)}")

# 2. Identify Sources and Targets
# We check if 'popular_times' has enough data (length > 20 chars is a safe heuristic for non-empty JSON)
df['has_data'] = df['popular_times'].apply(lambda x: len(str(x)) > 20 if pd.notna(x) else False)

sources = df[df['has_data']].copy()
targets = df[~df['has_data']].copy()

print(f"✅ Sources (Have Data): {len(sources)}")
print(f"⚠️ Targets (Need Data): {len(targets)}")

if len(sources) == 0:
    print("❌ Error: No source data found to spread! Run the scraper first.")
    exit()

# Pre-parse source schedules to avoid parsing them 1000 times
source_schedules = {}
for idx, row in sources.iterrows():
    sched = parse_schedule(row['popular_times'])
    if sched is not None:
        source_schedules[idx] = sched

# 3. Iterate through Empty Places
count = 0
for idx, target in targets.iterrows():
    t_lat, t_lon = target['latitude'], target['longitude']
    
    distances = []
    
    # Calculate distance to ALL sources
    # (For 500 items this is instant. For 50k items, we'd use a KDTree)
    for s_idx, source in sources.iterrows():
        dist = haversine(t_lat, t_lon, source['latitude'], source['longitude'])
        distances.append((s_idx, dist))
    
    # Sort by distance and take top K
    distances.sort(key=lambda x: x[1])
    nearest_k = distances[:NEIGHBORS_TO_CONSIDER]
    
    # Calculate Weighted Average
    # Weight = 1 / distance (Inverse Distance Weighting)
    # To avoid divide by zero, max(dist, 10 meters)
    
    weighted_sum = np.zeros((7, 24))
    total_weight = 0
    
    for s_idx, dist in nearest_k:
        weight = 1 / (max(dist, 50) ** 2) # Power of 2 makes local neighbors MUCH stronger
        
        sched = source_schedules.get(s_idx)
        if sched is not None:
            weighted_sum += (sched * weight)
            total_weight += weight
            
    if total_weight > 0:
        # Compute final average matrix
        final_matrix = (weighted_sum / total_weight).astype(int) # Round to nearest integer
        
        # Convert back to JSON string
        final_json = format_schedule_back_to_json(final_matrix)
        
        # Update the main DataFrame
        df.at[idx, 'popular_times'] = final_json
        # Optional: Tag it so we know it's fake
        # df.at[idx, 'attributes'] = str(df.at[idx, 'attributes']).replace("]", ', "estimated_crowd"]') 
        
        count += 1
        if count % 10 == 0:
            print(f"\r⏳ Imputed {count}/{len(targets)} locations...", end="")

# 4. Cleanup and Save
df.drop(columns=['has_data'], inplace=True)
df.to_csv(OUTPUT_FILE, index=False)

print(f"\n\n🎉 Success! Filled {count} missing locations.")
print(f"💾 Saved to: {OUTPUT_FILE}")
print("👉 Rename this file to 'places_database.csv' to use it in your website.")