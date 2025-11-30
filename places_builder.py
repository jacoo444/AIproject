"""
places_database_builder.py

Builds the master 'places_database.csv' by:
1. Fetching base POIs from Barcelona OpenData.
2. Enriching them with Google Maps data (ID, Name, Attributes).
3. Fetching granular Popular Times data.

Output Columns: name, longitude, latitude, google_id, attributes, popular_times
"""

import requests
import pandas as pd
import googlemaps
import populartimes
import json
import sys
import time

# --------------------------------------------------------
# CONFIGURATION
# --------------------------------------------------------
GOOGLE_API_KEY = ""
OUTPUT_FILE = "places_database.csv"

# --------------------------------------------------------
# UTILS
# --------------------------------------------------------
def print_progress(iteration, total, prefix='', suffix='', decimals=1, length=50, fill='█'):
    """
    Call in a loop to create terminal progress bar
    """
    if total == 0:
        return
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    sys.stdout.write(f'\r{prefix} |{bar}| {percent}% {suffix}')
    if iteration == total: 
        sys.stdout.write('\n')
    sys.stdout.flush()

def main():
    print(f"\n--- 🚀 STARTING BUILDER FOR {OUTPUT_FILE} ---")
    
    # --------------------------------------------------------
    # STEP 1: INITIALIZE BASE DATA (OpenData BCN)
    # --------------------------------------------------------
    print("\n[Step 1/3] Fetching Base POIs from OpenData BCN...")
    
    try:
        # Fetch Cultural Interest Points
        url = "https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=31431b23-d5b9-42b8-bcd0-a84da9d8c7fa&limit=32000"
        response = requests.get(url)
        response.raise_for_status() 
        
        data = response.json()["result"]["records"]
        raw_df = pd.DataFrame(data)

        # Clean and Select Columns
        df = pd.DataFrame()
        df['name'] = raw_df['name'] 
        df['latitude'] = pd.to_numeric(raw_df['geo_epgs_4326_lat'], errors='coerce')
        df['longitude'] = pd.to_numeric(raw_df['geo_epgs_4326_lon'], errors='coerce')
        
        # Drop invalid coordinates
        df = df.dropna(subset=['latitude', 'longitude'])
        
        # Initialize empty columns
        df['google_id'] = ""
        df['attributes'] = ""
        df['popular_times'] = ""

        # Save Checkpoint
        df.to_csv(OUTPUT_FILE, index=False)
        print(f"✅ Step 1 Complete. Base database saved with {len(df)} locations.")

    except Exception as e:
        print(f"❌ Error in Step 1: {e}")
        return

    # --------------------------------------------------------
    # STEP 2: GOOGLE ENRICHMENT (ID, Name, Attributes)
    # --------------------------------------------------------
    print("\n[Step 2/3] Enriching with Google Maps (ID, Name, Attributes)...")
    
    gmaps = googlemaps.Client(key=GOOGLE_API_KEY)
    
    # Reload to ensure fresh state
    df = pd.read_csv(OUTPUT_FILE)
    
    # FIX: Force columns to object (string) type to avoid "incompatible dtype" errors
    df['google_id'] = df['google_id'].astype('object')
    df['attributes'] = df['attributes'].astype('object')
    df['name'] = df['name'].astype('object')
    
    valid_rows = []
    total_rows = len(df)
    
    # Error tracking
    error_printed = False 

    for index, row in df.iterrows():
        original_name = str(row['name']).strip()
        search_query = original_name if "Barcelona" in original_name else f"{original_name}, Barcelona"
        
        try:
            # 1. FIND ID
            find_res = gmaps.find_place(input=search_query, input_type="textquery")
            
            if find_res['status'] == 'OK' and len(find_res['candidates']) > 0:
                place_id = find_res['candidates'][0]['place_id']
                row['google_id'] = place_id
                
                # 2. GET DETAILS (Name + Types)
                try:
                    # Request singular 'type' field
                    details_res = gmaps.place(place_id=place_id, fields=['name', 'type'])
                    
                    if details_res['status'] == 'OK':
                        result = details_res['result']
                        row['name'] = result.get('name', original_name) 
                        
                        # API returns list in 'types'
                        row['attributes'] = json.dumps(result.get('types', [])) 
                        
                except Exception as detail_err:
                    row['attributes'] = "[]"
                    if not error_printed:
                        print(f"\n⚠️ Warning on details fetch: {detail_err}")
                        error_printed = True

                valid_rows.append(row)

            else:
                pass # Not found -> Dropped

        except Exception as e:
            if not error_printed:
                print(f"\n❌ CRITICAL API ERROR: {e}")
                error_printed = True
    
        print_progress(index + 1, total_rows, prefix='Progress:', suffix=f'Found: {len(valid_rows)}', length=40)

    # Save only valid rows
    df_enriched = pd.DataFrame(valid_rows)
    df_enriched.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ Step 2 Complete. {len(valid_rows)} places matched & saved. ({total_rows - len(valid_rows)} dropped)")

    # --------------------------------------------------------
    # STEP 3: POPULAR TIMES FETCH
    # --------------------------------------------------------
    print("\n[Step 3/3] Fetching Popular Times (Scraping)...")
    
    # Reload enriched data
    df = pd.read_csv(OUTPUT_FILE)
    
    # FIX: Force popular_times to object (string) type
    df['popular_times'] = df['popular_times'].astype('object')
    
    total_rows = len(df)
    success_count = 0
    
    for index, row in df.iterrows():
        place_id = str(row['google_id'])
        
        try:
            data = populartimes.get_id(GOOGLE_API_KEY, place_id)
            
            if 'populartimes' in data:
                # Save as JSON string
                row['popular_times'] = json.dumps(data['populartimes'])
                success_count += 1
            else:
                row['popular_times'] = "[]"
                
        except Exception:
            row['popular_times'] = "[]"

        # Update row in DataFrame
        df.iloc[index] = row
        
        print_progress(index + 1, total_rows, prefix='Progress:', suffix=f'Got Data: {success_count}', length=40)

    # Final Save
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ Step 3 Complete. Popular times gathered for {success_count} places.")
    print(f"\n🎉 DATABASE BUILD COMPLETE. Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()