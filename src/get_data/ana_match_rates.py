# -*- coding: utf-8 -*-
"""
=============================================================================
 Automated Correlation Matrix Analysis Script (V10 - Independent Denominator)
=============================================================================
 Purpose:
 1. Automatically loop through 6 input windows (Events) and 5 output windows (Price).
 2. For each of the 30 combinations:
    a. Roll time by input window.
    b. Filter out time windows with "No Events".
    c. Calculate "price_trend".
    d. (V10) Calculate 3 types of "majority_sentiment" (All, Med+High, High Only).
    e. (V10) If a subset (e.g., 'High Only') is empty, 
       the row is marked as NA (skipped) in that column, instead of 'consolidation'.
 3. Save 30 detailed CSVs containing NA values.
 4. (V10) Calculate 3 match rates, each with its own independent total sample size (denominator).
 5. Save the 90 match rates (and their respective denominators) to a "match_rates.txt" summary file.
=============================================================================
"""

import os
import json
import pandas as pd
from tqdm import tqdm
import sys
from collections import Counter
import traceback

# --- 1. Global Configuration ---

# --- [!] File Path Configuration (Anonymized) ---
# Please update these paths to match the user's local environment
EVENTS_BASE_DIR = r'Project\data_access\events_c'
PRICE_CSV_PATH = r'Project\data_access\ohlcv\1_Bitcoin(BTC)_BTCUSDT_5m.csv'

# --- [!] New Output Directory (Script will create it automatically) ---
OUTPUT_DIR = r'Project\correlation_matrix_v10'

# --- [!] Logic Configuration ---
EVENT_DIRS_TO_SCAN = ['ALL', 'BTC']
PRICE_RESOLUTION = pd.Timedelta(minutes=5)
PRICE_THRESHOLD = 0.1  # 0.1% threshold for price change

# --- [!] Automated Experiment Parameters ---
INPUT_WINDOWS_MIN = [5, 10, 20, 30, 60, 120]
OUTPUT_WINDOWS_MIN = [10, 20, 30, 60, 120]

# --- 2. Helper Functions ---

def load_events(base_dir, subfolders):
    """
    Recursively scan all specified event subfolders and load all events into a DataFrame.
    """
    event_list = []
    print(f"Start scanning event folders: {subfolders}...")
    
    for folder in subfolders:
        full_path = os.path.join(base_dir, folder)
        if not os.path.isdir(full_path):
            print(f"⚠️ Warning: Folder {full_path} not found, skipping.")
            continue
            
        # Use os.scandir() for better performance
        for entry in tqdm(os.scandir(full_path), desc=f"Loading {folder} events"):
            if entry.name.endswith('.json') and entry.is_file():
                try:
                    with open(entry.path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    event_time_str = data.get('original_time')
                    analysis = data.get('analysis', {})
                    sentiment = analysis.get('sentiment')
                    impact = analysis.get('predicted_impact')
                    
                    if event_time_str and sentiment and impact:
                        event_list.append({
                            'time': pd.to_datetime(event_time_str),
                            'sentiment': sentiment,
                            'impact': impact
                        })
                except Exception as e:
                    print(f"❌ Error: Unable to parse {entry.name}: {e}")
                
    if not event_list:
        print("❌ Critical Error: Failed to load any event data. Please check paths.")
        sys.exit()
        
    print(f"\n🎉 Successfully loaded {len(event_list)} events.")
    
    # Convert to DataFrame and set time as index for fast lookup
    df_events = pd.DataFrame(event_list)
    df_events = df_events.set_index('time').sort_index()
    return df_events


def load_price_data(csv_path):
    """
    Load and prepare price data (OHLCV CSV).
    """
    print(f"\nStart loading price data: {csv_path}...")
    try:
        df_price = pd.read_csv(csv_path)
    except FileNotFoundError:
        print(f"❌ Critical Error: Price file {csv_path} not found.")
        sys.exit()
        
    df_price['open_time'] = pd.to_datetime(df_price['open_time'])
    df_price = df_price.set_index('open_time').sort_index()
    
    print(f"🎉 Successfully loaded {len(df_price)} price records.")
    return df_price


def get_price_trend(df_price, start_time, end_time, resolution, threshold):
    """
    Calculate price trend based on P_start(open) and P_end(close).
    """
    end_bar_time = end_time - resolution
    
    try:
        P_start = df_price.at[start_time, 'open']
        P_end = df_price.at[end_bar_time, 'close']
        
        if P_start == 0: return 'consolidation'
            
        percent_change = ((P_end - P_start) / P_start) * 100
        
        if percent_change >= threshold: return 'bullish'
        elif percent_change <= -threshold: return 'bearish'
        else: return 'consolidation'
            
    except KeyError:
        # Missing price data
        return None
    except Exception as e:
        print(f"Error calculating trend: {e}")
        return None

def get_majority_sentiment(event_list_tuples, level='all'):
    """
    Calculate majority sentiment.
    V10 Rule: If the subset is empty, return pd.NA (skip), do not return 'consolidation'.
    """
    # 1. Filter
    if level == 'medhigh':
        filtered_list = [e for e in event_list_tuples if e[1] in ['Medium', 'High']]
    elif level == 'high':
        filtered_list = [e for e in event_list_tuples if e[1] == 'High']
    else: # 'all'
        filtered_list = event_list_tuples
        
    # 2. V10 "Empty Subset" Rule
    if not filtered_list:
        return pd.NA  # Return 'Not Available', pandas treats this as null
        
    # 3. Extract sentiments
    sentiments = [e[0] for e in filtered_list]
    
    # 4. Count
    counts = Counter(sentiments)
    
    # 5. Tie-breaker rule (bullish vs bearish)
    bullish_count = counts.get('bullish', 0)
    bearish_count = counts.get('bearish', 0)
    
    if bullish_count > 0 and bullish_count == bearish_count:
        return 'consolidation'
        
    # 6. Return majority
    return counts.most_common(1)[0][0]


def process_scenario(df_events, df_price, input_min, output_min, output_dir):
    """
    Run full analysis for a single (input, output) combination.
    V10: Returns 3 sets of (match, total).
    """
    
    # 1. Set time parameters
    event_window = pd.Timedelta(minutes=input_min)
    price_window = pd.Timedelta(minutes=output_min)
    
    # 2. Generate time windows
    overall_start_time = df_price.index.min()
    overall_end_time = df_price.index.max()
    
    time_windows = pd.date_range(
        start=overall_start_time, 
        end=overall_end_time, 
        freq=event_window
    )
    
    results = []
    
    # 3. Iterate through all time windows
    for window_start in tqdm(time_windows, desc=f"Input={input_min}m, Output={output_min}m", leave=False):
        
        # --- A. Define all time points ---
        event_window_start = window_start
        event_window_end = window_start + event_window
        
        price_window_start = event_window_end
        price_window_end = price_window_start + price_window
        
        if price_window_end > overall_end_time + PRICE_RESOLUTION:
            break
            
        # --- B. Core Filtering: Keep only windows with events ---
        events_in_window = df_events[
            (df_events.index >= event_window_start) &
            (df_events.index < event_window_end)
        ]
        
        if events_in_window.empty:
            continue
            
        # --- C. Calculate price trend (if events exist) ---
        trend = get_price_trend(
            df_price,
            price_window_start,
            price_window_end,
            PRICE_RESOLUTION,
            PRICE_THRESHOLD
        )
        
        if trend is None: # (e.g., missing price data)
            continue
            
        # --- D. Calculate 3 types of majority sentiment ---
        event_list = events_in_window[['sentiment', 'impact']].values.tolist()
        
        sent_all = get_majority_sentiment(event_list, 'all')
        sent_medhigh = get_majority_sentiment(event_list, 'medhigh')
        sent_high = get_majority_sentiment(event_list, 'high')
        
        # --- E. Record results ---
        time_window_str = (
            f"{event_window_start.strftime('%Y%m%dT%H%M')}-"
            f"{event_window_end.strftime('%Y%m%dT%H%M')}"
        )
        
        results.append({
            'time_window': time_window_str,
            'price_trend': trend,
            'events_list': str(event_list), # Full list for archiving
            'sentiment_all': sent_all,
            'sentiment_medhigh': sent_medhigh,
            'sentiment_high': sent_high
        })

    # 4. Save CSV
    if not results:
        tqdm.write("  -> Warning: No overlapping data found.")
        return (0, 0, 0, 0, 0, 0) # (Return 0 matches)

    df_scenario = pd.DataFrame(results)
    
    csv_filename = f"input_{input_min}m_output_{output_min}m.csv"
    csv_path = os.path.join(output_dir, csv_filename)
    df_scenario.to_csv(csv_path, index=False)
    
    # 5. V10 Match Rate Calculation (Independent Denominator)
    
    # 5.1 'All' Stats (It will never be NA because we filtered empty windows)
    df_all = df_scenario.dropna(subset=['sentiment_all'])
    total_all = len(df_all)
    match_all = (df_all['sentiment_all'] == df_all['price_trend']).sum() if total_all > 0 else 0

    # 5.2 'Med+High' Stats (Drop rows where 'sentiment_medhigh' is NA)
    df_medhigh = df_scenario.dropna(subset=['sentiment_medhigh'])
    total_medhigh = len(df_medhigh)
    match_medhigh = (df_medhigh['sentiment_medhigh'] == df_medhigh['price_trend']).sum() if total_medhigh > 0 else 0

    # 5.3 'High Only' Stats (Drop rows where 'sentiment_high' is NA)
    df_high = df_scenario.dropna(subset=['sentiment_high'])
    total_high = len(df_high)
    match_high = (df_high['sentiment_high'] == df_high['price_trend']).sum() if total_high > 0 else 0
    
    return (match_all, total_all, match_medhigh, total_medhigh, match_high, total_high)

# --- 3. Main Execution Logic (V10.1 Bugfix) ---
if __name__ == "__main__":
    
    # --- Step 1: Create Output Directory ---
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    report_txt_path = os.path.join(OUTPUT_DIR, "match_rates_summary_v10.txt")
    print(f"Output will be saved to: {OUTPUT_DIR}")
    
    # --- Step 2: Load Data (Once) ---
    df_events = load_events(EVENTS_BASE_DIR, EVENT_DIRS_TO_SCAN)
    df_price = load_price_data(PRICE_CSV_PATH)
    
    print("\n" + "="*80)
    print("🚀 Start Automated Correlation Matrix Analysis (V10.1 Bugfix)...")
    print(f"  -> {len(INPUT_WINDOWS_MIN)} (Input) x {len(OUTPUT_WINDOWS_MIN)} (Output) = {len(INPUT_WINDOWS_MIN) * len(OUTPUT_WINDOWS_MIN)} scenarios to process")
    print("="*80 + "\n")
    
    # --- Step 3: Loop & Write Report ---
    try:
        with open(report_txt_path, 'w', encoding='utf-8') as f_report:
            
            # Use tqdm for the outer loop
            for input_min in tqdm(INPUT_WINDOWS_MIN, desc="Total Input Windows"):
                for output_min in OUTPUT_WINDOWS_MIN:
                    
                    scenario_name = f"Scenario: Input={input_min}m, Output={output_min}m"
                    tqdm.write(f"\nProcessing {scenario_name}...")
                    
                    # --- Step 4: Execute Single Scenario Analysis ---
                    (match_all, total_all, 
                     match_medhigh, total_medhigh, 
                     match_high, total_high) = process_scenario(
                        df_events, 
                        df_price, 
                        input_min, 
                        output_min, 
                        OUTPUT_DIR
                    )
                    
                    # --- Step 5: Write to TXT Report (V10.1 Improved Robust Logic) ---
                    f_report.write("="*60 + "\n")
                    f_report.write(f"{scenario_name}\n")
                    
                    # (Prepare strings for console and file)
                    console_lines = []
                    
                    if total_all == 0:
                        f_report.write("(No overlapping data, 0 non-empty windows)\n")
                        tqdm.write("  -> Result: 0 rows")
                    else:
                        # --- 'All' Stats (Base) ---
                        rate_all = match_all / total_all
                        report_line_1 = f"  - All Events:      Match Rate: {rate_all:.2%} ({match_all}/{total_all} rows)"
                        console_line_1 = f"(All): {rate_all:.2%}"

                        # --- 'Med+High' Stats ---
                        if total_medhigh == 0:
                            report_line_2 = "  - Med+High Events: Match Rate: N/A (0 rows)"
                            console_line_2 = "(Med+High): N/A"
                        else:
                            rate_medhigh = match_medhigh / total_medhigh
                            report_line_2 = f"  - Med+High Events: Match Rate: {rate_medhigh:.2%} ({match_medhigh}/{total_medhigh} rows)"
                            console_line_2 = f"(Med+High): {rate_medhigh:.2%}"
                        
                        # --- 'High Only' Stats ---
                        if total_high == 0:
                            report_line_3 = "  - High Only Events: Match Rate: N/A (0 rows)"
                            console_line_3 = "(High): N/A"
                        else:
                            rate_high = match_high / total_high
                            report_line_3 = f"  - High Only Events: Match Rate: {rate_high:.2%} ({match_high}/{total_high} rows)"
                            console_line_3 = f"(High): {rate_high:.2%}"

                        # --- Write to File ---
                        f_report.write(f"(Based on {total_all} non-empty windows)\n")
                        f_report.write(report_line_1 + "\n")
                        f_report.write(report_line_2 + "\n")
                        f_report.write(report_line_3 + "\n")
                        
                        # --- Print to Console (Now Safe) ---
                        tqdm.write(f"  -> Result: {console_line_1} | {console_line_2} | {console_line_3}")

                    f_report.write("="*60 + "\n\n")
                    f_report.flush() # Ensure real-time writing to file

    except Exception as e:
        print(f"\n❌❌❌ Critical Error Occurred: {e}")
        traceback.print_exc()

    print("\n" + "="*80)
    print("✨✨✨ All Automated Experiments Completed! ✨✨✨")
    print(f"{len(INPUT_WINDOWS_MIN) * len(OUTPUT_WINDOWS_MIN)} CSV files saved to: {OUTPUT_DIR}")
    print(f"Summary report saved to: {report_txt_path}")