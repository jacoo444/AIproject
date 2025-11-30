import csv
import json
import os

INPUT_FILE = 'places_database.csv'
OUTPUT_FILE = 'heatmap_database.json'

print(f"--- 📦 Exporting Heatmap Data from {INPUT_FILE} ---")

heatmap_dataset = []

try:
    with open(INPUT_FILE, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            try:
                # 1. Parse Schedule
                pop_times_str = row.get('popular_times', '[]')
                pop_times = json.loads(pop_times_str)
                
                # Only keep valid places with data
                if pop_times and len(pop_times) == 7:
                    heatmap_dataset.append({
                        "lat": float(row['latitude']),
                        "lon": float(row['longitude']),
                        # We only need the 'data' array (0-23 hours) for each day
                        # Structure: [ [0..23 values], [0..23 values] ... 7 days ]
                        "schedule": [day['data'] for day in pop_times]
                    })
                    
            except (ValueError, json.JSONDecodeError):
                continue

    # Save compact JSON
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(heatmap_dataset, f, separators=(',', ':')) # Minified

    print(f"✅ Success! Saved {len(heatmap_dataset)} locations to {OUTPUT_FILE}")
    print("👉 Now the website can read this file to animate the map.")

except FileNotFoundError:
    print(f"❌ Error: {INPUT_FILE} not found.")