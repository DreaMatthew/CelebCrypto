import os
import json
import re
from datetime import datetime
import time
import openai  # DeepSeek API uses the openai library
from tqdm import tqdm # Import tqdm for progress bars
import sys
import traceback

# --- 1. Global Configuration ---

# --- [!] Core Modification: Define the *SINGLE* JSON file path to analyze ---
# [!] Please replace 'your_influencer_file.json' with your actual file name and path
TARGET_JSON_FILE_PATH = r'./data/profile_data/CynthiaMLummis_tweets.json'

# Temporary directory for sorted TXT files (Kept unchanged)
INTERMEDIATE_TXT_DIR = r'./data/tweet_temp_influencer'
# Directory for final AI-analyzed and cleaned TXT files (Kept unchanged)
FINAL_ANALYZED_DIR = r'./data/tweet_clean_influencer'

# --- [!] Core Modification: Define Global Analysis Theme ---
ANALYSIS_THEME = "Potential impact on any cryptocurrency price or the overall crypto market"


# --- [!] Replace with DeepSeek API Configuration ---
API_KEY = "YOUR_DEEPSEEK_API_KEY_HERE"
BASE_URL = "https://api.deepseek.com/v1"
API_CALL_DELAY = 0.2

# JSON File Sorting Preference
SORT_PREFERENCE = 'ascending'

# --- 2. Phase 1 Function: JSON -> TXT (From your script, unchanged) ---

def create_finetuning_dataset(input_json_path, output_txt_path, sort_order='ascending'):
    """
    Reads raw tweet data from JSON, sorts it, formats it, and saves to a TXT file.
    """
    try:
        with open(input_json_path, 'r', encoding='utf-8') as f:
            tweets_data = json.load(f)
        print(f"   -> Successfully read {len(tweets_data)} tweets.")

        def get_datetime_obj(tweet):
            created_at_str = tweet.get('createdAt')
            if created_at_str:
                try:
                    return datetime.strptime(created_at_str, '%a %b %d %H:%M:%S %z %Y')
                except (ValueError, TypeError): return None
            return None

        valid_tweets = [t for t in tweets_data if get_datetime_obj(t) is not None]
        is_descending = sort_order == 'descending'
        valid_tweets.sort(key=get_datetime_obj, reverse=is_descending)
        
        formatted_entries = []
        for tweet in valid_tweets:
            text = tweet.get('fullText') or tweet.get('text', '')
            dt_object = get_datetime_obj(tweet)
            formatted_date = dt_object.strftime('%Y-%m-%d %H:%M:%S')
            
            entry = f"""[TWEET START]
Text: "{text.replace('"', '""')}"
---
[METADATA]
- Author Username: {tweet.get('author', {}).get('userName', 'N/A')}
- Author Followers: {tweet.get('author', {}).get('followers', 0)}
- Created At: {formatted_date}
- Views: {tweet.get('viewCount', 0)}
- Likes: {tweet.get('likeCount', 0)}
- Retweets: {tweet.get('retweetCount', 0)}
- Replies: {tweet.get('replyCount', 0)}
- Quotes: {tweet.get('quoteCount', 0)}
[TWEET END]"""
            formatted_entries.append(entry)
            
        with open(output_txt_path, 'w', encoding='utf-8') as f:
            f.write("\n\n".join(formatted_entries))
            
        print(f"   -> Sorted and formatted {len(formatted_entries)} valid tweets, saved to: {output_txt_path}")

    except FileNotFoundError:
        print(f"   -> ❌ Error: Input file '{input_json_path}' not found.")
    except json.JSONDecodeError:
        print(f"   -> ❌ Error: Unable to parse '{input_json_path}'.")
    except Exception as e:
        print(f"   -> ❌ Unknown error occurred: {e}")

# --- 3. Phase 2 Function: AI Analysis (Combined Relevance & Sentiment, Unchanged) ---

def parse_full_tweet_blocks(filepath):
    """Parses the TXT file and stores each complete tweet block as an independent element in a list."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError: return []
    tweet_blocks = re.split(r'(\[TWEET START\].*?\[TWEET END\])', content, flags=re.DOTALL)
    # Return all matched complete blocks
    return [block.strip() for block in tweet_blocks if '[TWEET START]' in block and block.strip()]

def analyze_relevance_and_sentiment(client, full_tweet_block, theme):
    """
    Uses DeepSeek AI for "Two-in-One" analysis:
    1. Determine if the tweet is relevant to the 'theme' (using loose criteria).
    2. If relevant, perform sentiment analysis (Bullish/Bearish/Consolidation).
    Returns a dictionary containing the analysis results.
    """
    if not full_tweet_block:
        return None

    # --- [!] Core Modification: New Prompt designed for "Influencers" and "Loose Criteria" ---
    prompt_template = """
You are an expert crypto market sentiment analyst. Your task is to analyze the following tweet from a **highly influential person**.

**Analysis Goal (Theme):** "{theme}"

--- STAGE 1: RELEVANCE CHECK (LOOSE FILTER) ---
First, determine if the tweet's core content (from `Text:`) has the **potential to cause even a small impact** (positive or negative) on the price of **any cryptocurrency** or the **overall crypto market**.

* **'YES' (Relevant) if:** The text mentions *any* crypto asset (e.g., Bitcoin, BTC, ETH, DOGE), NFTs, market trends, regulations, adoption, new technology, exchanges (e.g., Binance), or other crypto-related topics, **even briefly or vaguely**.
* **'NO' (Irrelevant) if:** The text is **PURELY** about non-crypto topics (e.g., personal life, sports, unrelated politics) AND has **no identifiable link** to the crypto market.

**If 'NO' (Irrelevant), stop here and output ONLY the 'Irrelevant' JSON format.**

--- STAGE 2: SENTIMENT ANALYSIS (Only if RELEVANT) ---
If 'YES' (Relevant), you must strictly follow this 5-step analysis checklist to classify its *market sentiment*:

--- ANALYSIS CHECKLIST ---
**STEP 1: CORE JUDGMENT (Find Direct Signals in `Text:`)**
* **1. Check for 'Bullish' Signals:** "buy", "hold", "long", "accumulate", "bullish", "rally", "breakout", "partnership", "launch", "upgrade", "positive news", "BTD", "🚀". If found, classify as `Bullish` and move to Step 2.
* **2. Check for 'Bearish' Signals:** If no Bullish signals, scan for: "sell", "short", "take profit", "dump", "bearish", "drop", "crash", "delay", "security issue", "bug", "negative news", "FUD". If found, classify as `Bearish` and move to Step 2.
* **3. Default to 'Consolidation' (Neutral):** If NO strong Bullish (1) or Bearish (2) signals are found, classify as `Consolidation`.

**STEP 2: AUXILIARY JUDGMENT (Check `Text:` Context)**
* Refine classification using context (e.g., negation "NOT bullish").

**STEP 3: AUTHOR IDENTITY (Check `[METADATA]`)**
* Analyze `Author Username`. An official project/exchange posting 'launch' is **Strongly Bullish**.

**STEP 4: INTERACTION PROOF (Check `[METADATA]`)**
* Analyze `Likes`, `Retweets`. High interactions (e.g., Likes > 200) **strengthens** the classification.

**STEP 5: SYNTHESIZE & OUTPUT**
* Combine all findings for the final label.

--- OUTPUT FORMAT ---
You must provide your final analysis ONLY in one of the two following JSON formats. Do not add any text or explanation before or after the JSON block.

**Format 1: If RELEVANT (from Stage 2):**
{{
  "relevant": true,
  "label": "Your final classification: Bullish, Bearish, or Consolidation",
  "key_word_used": "The word/phrase from the `Text:` that was the primary signal (from Step 1 or 2). If Consolidation, write 'N/A'.",
  "reasoning": "A brief explanation of how you reached your final label. You MUST mention if metadata (Author or Interactions) was used. **If 'Consolidation', state that no strong Bullish/Bearish signals were found.**"
}}

**Format 2: If NOT RELEVANT (from Stage 1):**
{{
  "relevant": false,
  "label": "Irrelevant",
  "key_word_used": "N/A",
  "reasoning": "The tweet's core content was not related to the crypto market, based on the loose filter criteria."
}}

--- TWEET TO ANALYZE ---
{full_tweet_block}
"""
    # --- End of Prompt Template ---

    # Format the full prompt
    full_prompt = prompt_template.format(theme=ANALYSIS_THEME, full_tweet_block=full_tweet_block)
    
    try:
        prompt_messages = [
            {"role": "user", "content": full_prompt}
        ]
        
        response = client.chat.completions.create(
            model="deepseek-chat",  # DeepSeek high-performance model
            messages=prompt_messages,
            max_tokens=300,  # Increased token limit to accommodate reasoning
            temperature=0.01,
            response_format={"type": "json_object"}, # Ensure JSON response
        )
        
        raw_output = response.choices[0].message.content.strip()
        time.sleep(API_CALL_DELAY) # Respect rate limits
        
        # --- Parse JSON returned by model ---
        try:
            result = json.loads(raw_output)
            
            if "relevant" not in result or "label" not in result:
                # Print detailed log, but treat as non-fatal
                tqdm.write(f"     > ⚠️ Warning: JSON returned by model is missing 'relevant' or 'label' keys. Skipping.")
                return None
            
            # Use tqdm.write to avoid breaking progress bar
            tqdm.write(f"     > AI Analysis: Relevant={result['relevant']}, Label={result['label']}")
            return result # Return full JSON dictionary

        except json.JSONDecodeError:
            tqdm.write(f"     > ❌ Error: Failed to parse JSON returned by model. Raw output: {raw_output}")
            return None
        except Exception as e:
            tqdm.write(f"     > ❌ Unexpected error during JSON parsing: {e}")
            return None

    except Exception as e:
        tqdm.write(f"     > ❌ Error calling DeepSeek AI API: {e}")
        traceback.print_exc()
        time.sleep(5) # Wait longer if API fails
        return None

# --- 4. Core Execution Logic (Modified) ---

# --- [!] New: Helper function to extract timestamp ID from tweet block ---
def extract_timestamp_from_block(block):
    """
    Extracts 'Created At' timestamp from [METADATA] block to use as a unique ID.
    """
    # Use re.search to find matches in the 'block' string
    # r"Created At: (.*?)\n" is a regex:
    # - "Created At: " : Matches literal string
    # - (.*?) : Non-greedy capture group
    #   - . : Matches any character except newline
    #   - *? : Matches previous character zero or more times, as few as possible
    # - \n : Matches a newline character
    match = re.search(r"Created At: (.*?)\n", block)
    if match:
        # match.group(0) is the full match (e.g., "Created At: 2025-11-05 18:51:01\n")
        # match.group(1) is the content of the first group (e.g., "2025-11-05 18:51:01")
        return match.group(1).strip()
    return None

def process_target_json(full_json_path, client):
    """
    Executes the full JSON -> TXT -> Cleaned_Analyzed_TXT pipeline for a *SINGLE SPECIFIED* JSON file.
    (Added checkpoint/resume functionality)
    """
    
    # Extract filename and base name from full path
    json_filename = os.path.basename(full_json_path)
    base_name = os.path.splitext(json_filename)[0]

    print("\n" + "="*30 + f" Processing File: {json_filename} " + "="*30)

    # --- Phase 1: JSON -> TXT ---
    print("\n--- Phase 1: JSON -> TXT (Formatting) ---")
    
    input_json_path = full_json_path
    
    # Output TXT filename
    output_txt_filename = f"{base_name}.txt"
    intermediate_txt_path = os.path.join(INTERMEDIATE_TXT_DIR, output_txt_filename)
    
    # Call function (This overwrites old txt to ensure data is fresh)
    create_finetuning_dataset(input_json_path, intermediate_txt_path, sort_order=SORT_PREFERENCE)
            
    # --- Phase 2: TXT -> Cleaned TXT (AI Filtering & Analysis) ---
    print("\n--- Phase 2: TXT -> Cleaned TXT (AI Filtering & Analysis) ---")
    
    # Final output filename
    final_cleaned_filename = f"{base_name}_clean.txt"
    final_output_path = os.path.join(FINAL_ANALYZED_DIR, final_cleaned_filename)
    
    # --- [!] Core Modification: Load processed timestamps ---
    processed_timestamps = set()
    if os.path.exists(final_output_path):
        print(f"   -> 🔎 Found existing analysis file, loading processed tweets...")
        # Parse the final clean file, not the intermediate one
        processed_blocks = parse_full_tweet_blocks(final_output_path)
        for block in processed_blocks:
            ts = extract_timestamp_from_block(block)
            if ts:
                processed_timestamps.add(ts)
        print(f"   -> ✅ Loaded {len(processed_timestamps)} previously processed tweets.")
    else:
        print(f"   -> 🆕 No existing analysis file found, creating new file.")
    
    # --- Load *ALL* tweets to be processed ---
    print(f"   -> Loading source TXT file: {output_txt_filename}")
    all_tweet_blocks = parse_full_tweet_blocks(intermediate_txt_path)
    if not all_tweet_blocks:
        print("   -> 🤷‍♂️ No tweets in file, skipping.")
        return # End processing for this file
    
    total_tweets_to_check = len(all_tweet_blocks)
    print(f"   -> Preparing to check {total_tweets_to_check} tweets (Skipping {len(processed_timestamps)} already processed).")

    # Counters
    relevant_count_session = 0 # Relevant tweets added in this run
    skipped_count = 0          # Tweets skipped in this run
    
    # --- [!] Core Modification: Open file in 'a' (append) mode ---
    try:
        with open(final_output_path, 'a', encoding='utf-8') as outfile:
            
            # Use tqdm to show AI classification progress
            for block in tqdm(all_tweet_blocks, desc=f"AI Analyzing {json_filename}"):
                
                # --- [!] Core Modification: Checkpoint Logic ---
                timestamp = extract_timestamp_from_block(block)
                
                # If no timestamp or timestamp already processed, skip
                if not timestamp or timestamp in processed_timestamps:
                    skipped_count += 1
                    continue # Skip this block

                # --- If not skipped, it's a new entry, start processing ---
                analysis_result = analyze_relevance_and_sentiment(client, block, ANALYSIS_THEME)
                
                # Check if API call succeeded and tweet is relevant
                if analysis_result and analysis_result.get("relevant") == True:
                    # --- Tweet is relevant, extract sentiment analysis and build [ANALYZEDATA] block ---
                    label = analysis_result.get("label", "Unknown")
                    keyword = analysis_result.get("key_word_used", "N/A")
                    reasoning = analysis_result.get("reasoning", "N/A")
                    
                    analysis_block = (
                        f"\n---\n"
                        f"[ANALYZEDATA]\n"
                        f"- label: {label}\n"
                        f"- key_word_used: {keyword}\n"
                        f"- reasoning: {reasoning}\n"
                    )
                    
                    modified_block = block.replace("[TWEET END]", analysis_block + "[TWEET END]")
                    
                    # --- [!] Core Modification: Write to file immediately ---
                    # Ensure newlines between blocks
                    outfile.write(modified_block + "\n\n") 
                    
                    relevant_count_session += 1
                    
                    # Immediately add this newly processed timestamp to set just in case (though optional)
                    processed_timestamps.add(timestamp)
                
                # else: (Irrelevant or API Error)
                    # We simply 'pass'.
                    # If API error (result is None), it won't be written,
                    # next run it will still not be in processed_timestamps, so it will retry.
                    # If Irrelevant (relevant: false), it won't be written,
                    # next run it will be *re-analyzed* (this is acceptable as relevance check is fast).
                    pass 

    except Exception as e:
        print(f"\n🚨 Critical error while writing to file: {e}")
        traceback.print_exc()
        return

    # --- Loop finished, print final report ---
    print("\n--- Processing Complete ---")
    print(f"   -> Checked {total_tweets_to_check} tweets.")
    print(f"   -> Skipped {skipped_count} tweets (Already in file).")
    print(f"   -> Added {relevant_count_session} relevant tweets in this run.")
    
    total_in_file = len(processed_timestamps) # Contains both old and new
    if skipped_count + relevant_count_session != total_tweets_to_check:
         # This number might not match exactly because some tweets might have failed API calls
         failed_this_run = total_tweets_to_check - skipped_count - relevant_count_session
         print(f"   -> {failed_this_run} tweets were not saved (Likely irrelevant or API errors).")
    
    print(f"   -> 🎉 Final file '{final_cleaned_filename}' now contains {total_in_file} tweets total.")


# --- 5. Main Execution Logic (Modified to process single file) ---
if __name__ == "__main__":
    if not API_KEY or "sk-" not in API_KEY: # Simple check
        print("🚨 Please enter your DeepSeek API Key in the configuration section at the top of the code.")
    else:
        print("🚀 Starting *SINGLE FILE* processing pipeline (with checkpointing)...")
        
        # --- Preparation: Ensure all *output* directories exist ---
        os.makedirs(INTERMEDIATE_TXT_DIR, exist_ok=True)
        os.makedirs(FINAL_ANALYZED_DIR, exist_ok=True)
        print(f"Intermediate TXT Directory: {INTERMEDIATE_TXT_DIR}")
        print(f"Final Output Directory: {FINAL_ANALYZED_DIR}")

        # --- Core Modification: Check *SINGLE* target JSON file ---
        try:
            if not os.path.exists(TARGET_JSON_FILE_PATH):
                print(f"\n🚨 Error: Target file not found:")
                print(f"{TARGET_JSON_FILE_PATH}")
                print("Please check if the 'TARGET_JSON_FILE_PATH' is correct.")
            else:
                # Initialize DeepSeek (OpenAI) Client
                deepseek_client = openai.OpenAI(
                    api_key=API_KEY,
                    base_url=BASE_URL
                )
                
                # --- Core Modification: Call processing function directly, no loop ---
                process_target_json(TARGET_JSON_FILE_PATH, deepseek_client)
            
            print("\n" + "="*80)
            print("✨✨✨ File processing finished! ✨✨✨")

        except Exception as e:
            print(f"🚨 An unknown error occurred: {e}")
            traceback.print_exc()