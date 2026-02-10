# -*- coding: utf-8 -*-
import os
import json
import openai  # DeepSeek API uses the openai library
import time
import sys
import traceback
from tqdm import tqdm

# --- 1. Global Configuration ---

# --- ❗️ Your DeepSeek API Configuration (from your script) ---
API_KEY = ""
BASE_URL = "https://api.deepseek.com/v1"
API_CALL_DELAY = 0.2  # Delay between API calls (in seconds)

# --- ❗️ Your File Path Configuration ---
# Directory containing the original news JSON files
SOURCE_DATA_DIR = r'lab\content6'
# New directory to store the analyzed JSON files
ANALYZED_DATA_DIR = r'lab\results_analyzed6'


# --- 2. Core AI Analysis Function ---

def analyze_news_event(client, title, summary, content):
    """
    Analyzes a news event using the DeepSeek AI to extract entities,
    cryptocurrencies, and market sentiment.
    Returns a dictionary with the analysis results.
    """

    # --- This is the new prompt, designed for your task, in English ---
    prompt_template = """
You are an expert cryptocurrency news analyst. Your task is to analyze the following news event (including its title, summary, and content) and extract information strictly according to the required JSON format.

--- News Content ---
Title: {title}
Summary: {summary}
Content: {content}

--- Tasks ---
1.  **Extract Entities:** Identify all mentioned persons, positions, companies, and organizations. If a category is empty, return an empty list [].
2.  **Extract Cryptocurrencies:** Extract all explicitly mentioned cryptocurrency names or tickers (e.g., Bitcoin, BTC, Ethereum). If none, return an empty list [].
3.  **Analyze Market Impact (Impact Sentiment):** Determine the event's overall impact on the cryptocurrency market.
    * If the event is positive, suggests price increases, or indicates market confidence (e.g., "rebounds," "institutional buying," "technical breakout," "positive policy"), label it as "Bullish".
    * If the event is negative, suggests price decreases, or indicates market panic (e.g., "plunges," "liquidations," "regulatory crackdown," "security breach"), label it as "Bearish".
    * If the event's impact is unclear, neutral, or sideways (e.g., "market waits," "minor fluctuations," "mixed analyst opinions"), label it as "Consolidation".
4.  **Provide Reasoning:** Briefly explain in one sentence how you reached the market impact decision, based strictly on the news content.

--- Output Format (Must be strict JSON) ---
You must return ONLY a JSON object. Do not include any explanations or text outside of the JSON block.

{{
  "entities": {{
    "persons": ["...", "..."],
    "positions": ["...", "..."],
    "companies": ["...", "..."],
    "organizations": ["...", "..."]
  }},
  "cryptocurrencies": ["...", "..."],
  "impact_sentiment": "Bullish/Bearish/Consolidation",
  "reasoning": "..."
}}
"""
    # --- End of Prompt Template ---

    # Format the full prompt
    full_prompt = prompt_template.format(
        title=title,
        summary=summary,
        content=content
    )

    try:
        prompt_messages = [
            {"role": "user", "content": full_prompt}
        ]

        response = client.chat.completions.create(
            model="deepseek-chat",  # DeepSeek's high-performance model
            messages=prompt_messages,
            max_tokens=1024,  # Increased token limit for entities and reasoning
            temperature=0.01,
            response_format={"type": "json_object"},  # Ensure JSON output
        )

        raw_output = response.choices[0].message.content.strip()
        time.sleep(API_CALL_DELAY)  # Adhere to rate limits

        # --- Parse the model's JSON response ---
        try:
            result = json.loads(raw_output)
            
            # Simple validation for key fields
            if "impact_sentiment" not in result or "reasoning" not in result:
                print(f"      > ⚠️ WARNING: Model's JSON response is missing key fields.")
                return None
            
            # print(f"      > AI Analysis: {result.get('impact_sentiment')}") # (Uncomment for detailed logging)
            return result  # Return the full JSON dictionary

        except json.JSONDecodeError:
            print(f"      > ❌ ERROR: Failed to parse the model's JSON response. Raw output: {raw_output}")
            return None
        except Exception as e:
            print(f"      > ❌ An unexpected error occurred while parsing JSON: {e}")
            return None

    except Exception as e:
        print(f"      > ❌ ERROR: An error occurred during the DeepSeek AI API call: {e}")
        traceback.print_exc()
        time.sleep(5)  # Wait longer if the API call fails
        return None

# --- 3. Main Execution Logic ---
if __name__ == "__main__":
    if not API_KEY or "sk-df0b" not in API_KEY:
        print("🚨 Please set your DeepSeek API Key in the configuration section at the top of the script.")
        sys.exit()
        
    print("🚀 Starting the news event analysis pipeline...")

    # --- Preparation: Ensure output directory exists ---
    os.makedirs(ANALYZED_DATA_DIR, exist_ok=True)
    print(f"Source directory: {SOURCE_DATA_DIR}")
    print(f"Analyzed results will be saved to: {ANALYZED_DATA_DIR}")

    # --- Initialize DeepSeek (OpenAI) client ---
    try:
        deepseek_client = openai.OpenAI(
            api_key=API_KEY,
            base_url=BASE_URL
        )
    except Exception as e:
        print(f"❌ Failed to initialize DeepSeek client: {e}")
        sys.exit()

    # --- Filter files that need processing ---
    try:
        all_source_files = [f for f in os.listdir(SOURCE_DATA_DIR) if f.endswith('.json')]
        # Check for already processed files to allow resuming
        processed_files = set(os.listdir(ANALYZED_DATA_DIR))
        
        files_to_process = [f for f in all_source_files if f not in processed_files]
        
        if not all_source_files:
            print("🤷‍♂️ No .json files found in the source directory.")
            sys.exit()
            
        if not files_to_process:
            print("✨ All files have already been processed.")
            sys.exit()

        print(f"Found {len(all_source_files)} total files, {len(files_to_process)} new files to process.")

    except FileNotFoundError:
        print(f"❌ ERROR: Source directory not found: {SOURCE_DATA_DIR}")
        sys.exit()
    except Exception as e:
        print(f"❌ Error while scanning files: {e}")
        sys.exit()

    # --- Iterate over files and process them ---
    for filename in tqdm(files_to_process, desc="Analyzing news events"):
        input_path = os.path.join(SOURCE_DATA_DIR, filename)
        output_path = os.path.join(ANALYZED_DATA_DIR, filename)
        
        # print(f"\n📄 Processing: {filename}")

        try:
            # 1. Read the original JSON
            with open(input_path, 'r', encoding='utf-8') as f:
                news_data = json.load(f)

            # Extract content needed for analysis
            title = news_data.get("title", "")
            summary = news_data.get("summary", "")
            content = news_data.get("content", "")

            if not title and not summary and not content:
                print(f"   -> ⚠️ WARNING: File {filename} is empty or missing key content fields, skipping.")
                continue

            # 2. Call the AI analysis function
            analysis_result = analyze_news_event(deepseek_client, title, summary, content)

            # 3. Merge and save
            if analysis_result:
                # We add a new top-level key "llm_analysis"
                # to store all the AI-returned content.
                news_data["llm_analysis"] = analysis_result

                # 4. Write the new, combined JSON file
                with open(output_path, 'w', encoding='utf-8') as f:
                    # ensure_ascii=False ensures correct character encoding
                    # indent=4 makes the file human-readable
                    json.dump(news_data, f, ensure_ascii=False, indent=4)
                
                # print(f"   -> 🎉 Analysis successful, saved to: {output_path}")
            else:
                print(f"   -> ❌ AI analysis failed for file {filename}, skipping.")

        except json.JSONDecodeError:
            print(f"   -> ❌ ERROR: Could not parse {filename}, file may be corrupt.")
        except Exception as e:
            print(f"   -> ❌ An unknown error occurred while processing {filename}: {e}")

    print("\n" + "="*80)
    print("✨✨✨ All news events processed successfully! ✨✨✨")