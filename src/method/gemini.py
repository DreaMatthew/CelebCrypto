#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import time
from collections import deque
from tqdm import tqdm
from openai import OpenAI

# ================= Configuration Area =================

# 1. API Configuration
API_KEYS = []
API_BASE_URL = "https://hiapi.online/v1" 
MODEL_NAME = "gemini-3-pro-preview"    

# 2. File Path Configuration
# Anonymized paths: replace these with your actual local paths
INPUT_FILE = "gold_standard_events.json"
OUTPUT_FILE = "impact_features_step1.jsonl"
FAILED_FILE = "impact_failed.jsonl"

# ================= Prompt Template Definition =================

SYSTEM_PROMPT = """Role: Senior Macro & Crypto Strategist.
Task: Analyze 'Actual' vs 'Previous' data and evaluate the SHORT-TERM (within 24 hours) impact on Bitcoin (BTC) price and market liquidity.

Rules:
1. ONLY compare 'Actual' vs 'Previous'. Ignore 'Forecast'.
2. Intensity (1-10): Rate the market-moving weight for the NEXT 24 HOURS (e.g., CPI/NFP=9-10, GDP=7-8, Jobless/PMI=3-5).
3. Direction: 1 (Bullish for BTC within 24h), -1 (Bearish for BTC within 24h), 0 (Neutral).
4. Surprise Type: 
   - Hawkish: Higher inflation/growth, suggests tighter policy (Bearish for BTC).
   - Dovish: Lower inflation/growth, suggests looser policy (Bullish for BTC).
5. Logic Chain: English only, max 20 words. 
   Format: [Fact vs Previous]; [Policy Shift]; [24h Liquidity Effect]; [Short-term BTC Impact].

Output Format (Strict JSON):
{
    "event_meta": {
        "category": "Inflation/Employment/Monetary/Growth/Sentiment"
    },
    "quantitative_pulse": {
        "surprise_val": float, // Numerical difference (Actual - Previous). 0 if text.
        "intensity": int
    },
    "qualitative_logic": {
        "direction": int,
        "surprise_type": "Hawkish/Dovish/Neutral",
        "logic_chain": "string"
    }
}"""

# ================= Utility Classes & Functions =================

class GeminiKeyManager:
    def __init__(self, keys):
        if not keys:
            raise ValueError("❌ API_KEYS cannot be empty")
        self.keys = deque(keys)

    def get_current_key(self):
        return self.keys[0]

    def rotate_key(self):
        failed_key = self.keys.popleft()
        self.keys.append(failed_key)
        tqdm.write(f"🔄 Key Exhausted/Failed. Rotating... (Next: ...{self.keys[0][-4:]})")

# ================= Main Logic =================

def main():
    print("🚀 Task Started: 24h BTC Impact Feature Extraction")
    
    key_manager = GeminiKeyManager(API_KEYS)
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ File not found: {INPUT_FILE}")
        return
        
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        events_data = json.load(f)

    processed_ids = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    processed_ids.add(data['id'])
                except: pass
    
    events_to_process = []
    for item in events_data:
        item_id = f"{item['date']}_{item['time']}_{item['event']}"
        if item_id not in processed_ids:
            item['id'] = item_id
            events_to_process.append(item)
    
    print(f"📊 Total: {len(events_data)} | Remaining: {len(events_to_process)}")

    with open(OUTPUT_FILE, 'a', encoding='utf-8') as fout, \
         open(FAILED_FILE, 'a', encoding='utf-8') as ffail:

        pbar = tqdm(events_to_process, desc="Processing Events")
        
        for item in pbar:
            item_id = item['id']
            
            current_key = key_manager.get_current_key()
            try:
                client = OpenAI(api_key=current_key, base_url=API_BASE_URL)
                
                response = client.chat.completions.create(
                    model=MODEL_NAME,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": f"Event: {item['event']}\nActual: {item.get('actual')}\nPrevious: {item.get('previous')}\nFocus on the next 24-hour BTC price impact."},
                    ],
                    stream=False
                )
                
                response_json_str = response.choices[0].message.content.strip()

                # Handle potential Markdown code block wrapping in LLM response
                if "```json" in response_json_str:
                    response_json_str = response_json_str.split("```json")[1].split("```")[0].strip()
                elif "```" in response_json_str:
                    response_json_str = response_json_str.split("```")[1].split("```")[0].strip()

                llm_res = json.loads(response_json_str)
                
                final_sample = {
                    "id": item_id,
                    "timestamp": f"{item['date']} {item['time']}",
                    "event_meta": {
                        "event_name": item['event'],
                        "category": llm_res.get("event_meta", {}).get("category")
                    },
                    "quantitative_pulse": {
                        "actual_raw": item.get('actual'),
                        "previous_raw": item.get('previous'),
                        "surprise": llm_res.get("quantitative_pulse", {}).get("surprise_val"),
                        "intensity": llm_res.get("quantitative_pulse", {}).get("intensity")
                    },
                    "qualitative_logic": llm_res.get("qualitative_logic", {}),
                    "original_item": item 
                }
                
                fout.write(json.dumps(final_sample, ensure_ascii=False) + '\n')
                fout.flush()

            except Exception as e:
                tqdm.write(f"⚠️ Error {item_id}: {str(e)[:100]}")
                # Rotate key if API error occurs (e.g., quota, rate limit, or auth error)
                if "api_key" in str(e).lower() or "limit" in str(e).lower() or "401" in str(e) or "429" in str(e):
                    key_manager.rotate_key()
                ffail.write(json.dumps({"id": item_id, "error": str(e)}) + "\n")
                ffail.flush()
                time.sleep(1)

    print("\n🎉 Process Completed.")

if __name__ == "__main__":
    main()