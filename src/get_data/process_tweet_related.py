import os
import json
import re
from datetime import datetime
import time
import openai 
from tqdm import tqdm 
import sys
import traceback


ASSET_LIST = [
    '1_Bitcoin(BTC)',
    # '2_Ethereum(ETH)',
    # '3_XRP(XRP)',
    # '4_Tether(USDT)',
    # '5_BNB(BNB)',
    # '6_Solana(SOL)',
    # '7_USDC(USDC)',
    # '8_Dogecoin(DOGE)',
    # '9_Lido Staked Ether(STETH)',
    # '10_Cardano(ADA)',
    # '11_TRON(TRX)',
    # '12_Wrapped stETH(WSTETH)',
    # '13_Chainlink(LINK)',
    # '14_Hyperliquid(HYPE)',
    # '15_Wrapped Beacon ETH(WBETH)',
    # '16_Wrapped Bitcoin(WBTC)',
    # '17_Avalanche(AVAX)',
    # '18_Ethena USDe(USDE)',
    # '19_Sui(SUI)',
    # '20_Figure Heloc(FIGR_HELOC)',
    # '21_Bitcoin Cash(BCH)',
    # '22_Stellar(XLM)',
    # '23_Wrapped eETH(WEETH)',
    # '24_WETH(WETH)',
    # '25_Hedera(HBAR)',
    # '26_LEO Token(LEO)',
    # '27_Litecoin(LTC)',
    # '28_Cronos(CRO)',
    # '29_Toncoin(TON)',
    # '30_USDS(USDS)',
    # '31_Shiba Inu(SHIB)',
    # '32_Coinbase Wrapped BTC(CBBTC)',
    # '33_Polkadot(DOT)',
    # '34_Binance Bridged USDT(BNB Smart Chain)',
    # '35_WhiteBIT Coin(WBT)',
    # '36_World Liberty Financial(WLFI)',
    # '37_Ethena Staked USDe(SUSDE)',
    # '38_Uniswap(UNI)',
    # '39_Mantle(MNT)',
    # '40_Monero(XMR)',
    # '41_MemeCore(M)',
    # '42_Ethena(ENA)',
    # '43_Pepe(PEPE)',
    # '44_Aave(AAVE)',
    # '45_Bitget Token(BGB)',
    # '46_Dai(DAI)',
    # '47_OKB(OKB)',
    # '48_Jito Staked SOL(JITOSOL)',
    # '49_NEAR Protocol(NEAR)',
    # '50_Bittensor(TAO)',
    # '51_Story(IP)',
    # '52_Ondo(ONDO)',
    # '53_Worldcoin(WLD)',
    # '54_Aptos(APT)',
    # '55_MYX Finance(MYX)',
    # '56_Ethereum Classic(ETC)',
    # '57_Binance Staked SOL(BNSOL)',
    # '58_Pi Network(PI)',
    # '59_Pump.fun(PUMP)',
    # '60_USDT0(USDT0)',
    # '61_Binance-Peg WETH(WETH)',
    # '62_Arbitrum(ARB)',
    # '63_POL(ex-MATIC)(POL)',
    # '64_USD1(USD1)',
    # '65_Internet Computer(ICP)',
    # '66_Pudgy Penguins(PENGU)',
    # '67_Kinetiq Staked HYPE(KHYPE)',
    # '68_Kaspa(KAS)',
    # '69_Jupiter Perpetuals Liquidity Provider Token(JLP)',
    # '70_VeChain(VET)',
    # '71_BlackRock USD Institutional Digital Liquidity Fund(BUIDL)',
    # '72_Cosmos Hub(ATOM)',
    # '73_Kelp DAO Restaked ETH(RSETH)',
    # '74_Algorand(ALGO)',
    # '75_sUSDS(SUSDS)',
    # '76_Rocket Pool ETH(RETH)',
    # '77_Render(RENDER)',
    # '78_Gate(GT)',
    # '79_KuCoin(KCS)',
    # '80_Sei(SEI)',
    # '81_Fasttoken(FTN)',
    # '82_Bonk(BONK)',
    # '83_Falcon USD(USDF)',
    # '84_USDtb(USDTB)',
    # '85_Provenance Blockchain(HASH)',
    # '86_Sky(SKY)',
    # '87_Flare(FLR)',
    # '88_Filecoin(FIL)',
    # '89_Official Trump(TRUMP)',
    # '90_Artificial Superintelligence Alliance(FET)',
    # '93_Jupiter(JUP)',
    # '97_Immutable(IMX)',
    # '98_Quant(QNT)',
    # '99_Optimism(OP)',
    # '91_StakeWise Staked ETH(OSETH)',
    # '92_BFUSD(BFUSD)',
    # '94_Liquid Staked ETH(LSETH)',
    # '95_Lombard Staked BTC(LBTC)',
    # '96_Renzo Restaked ETH(EZETH)',
    # '100_Polygon Bridged USDT(Polygon)(USDT)',

    # 您可以在這裡繼續添加更多資產，例如: '7_Ethereum(ETH)'
]

BASE_DATA_DIR = r'lab'
# [!] DeepSeek API Configuration
API_KEY = "YOUR_API_KEY_HERE"
BASE_URL = "https://api.deepseek.com/v1"
API_CALL_DELAY = 0.2

# JSON Sorting Preference
SORT_PREFERENCE = 'ascending'

# [!] Directory Configuration (Placeholders - Update these for your environment)
BASE_DATA_DIR = "./data" 
ASSET_LIST = ["10_Cardano(ADA)", "11_Solana(SOL)"] # Example assets

# --- 2. Phase 1 Function: JSON -> TXT ---

def create_finetuning_dataset(input_json_path, output_txt_path, sort_order='ascending'):
    """
    Reads raw tweet data from JSON, sorts it, formats it, and saves to a TXT file.
    """
    try:
        with open(input_json_path, 'r', encoding='utf-8') as f:
            tweets_data = json.load(f)
        print(f"  -> Successfully read {len(tweets_data)} tweets.")

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
            
        print(f"  -> Sorted and formatted {len(formatted_entries)} valid tweets, saved to: {output_txt_path}")

    except FileNotFoundError:
        print(f"  -> ❌ Error: Input file '{input_json_path}' not found.")
    except json.JSONDecodeError:
        print(f"  -> ❌ Error: Unable to parse '{input_json_path}'.")
    except Exception as e:
        print(f"  -> ❌ Unknown error occurred: {e}")

# --- 3. Phase 2 Function: AI Analysis (Combined Relevance & Sentiment) ---

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
    1. Determine if the tweet is relevant to the 'theme'.
    2. If relevant, perform sentiment analysis (Bullish/Bearish/Consolidation).
    Returns a dictionary containing the analysis results.
    """
    if not full_tweet_block:
        return None

    # --- This is the new prompt combining Relevance Check and Sentiment Analysis ---
    prompt_template = """
You are an expert crypto market sentiment analyst. Your task is to analyze the following social media post (tweet) in two stages, based on the provided theme.

**Theme:** "{theme}"

--- STAGE 1: RELEVANCE CHECK ---
First, determine if the tweet's core content (from `Text:`) is **directly and meaningfully related** to the specified **Theme**.
- 'YES' if it discusses the asset's price, news, technology, or community sentiment.
- 'NO' if it is spam, a different asset.

**If the tweet is NOT relevant, stop here and output ONLY the 'Irrelevant' JSON format.**

--- STAGE 2: SENTIMENT ANALYSIS (Only if RELEVANT) ---
If the tweet IS relevant (from Stage 1), you must strictly follow this 4-step analysis checklist to classify its price sentiment:

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
  "reasoning": "The tweet's core content was not meaningfully related to the specified theme."
}}

--- TWEET TO ANALYZE ---
{full_tweet_block}
"""
    # --- End of Prompt Template ---

    # Format the full prompt
    full_prompt = prompt_template.format(theme=theme, full_tweet_block=full_tweet_block)
    
    try:
        prompt_messages = [
            {"role": "user", "content": full_prompt}
        ]
        
        response = client.chat.completions.create(
            model="deepseek-chat",  # DeepSeek High Performance Model
            messages=prompt_messages,
            max_tokens=300,  # Increased token limit for reasoning
            temperature=0.01,
            response_format={"type": "json_object"}, # Ensure JSON response
        )
        
        raw_output = response.choices[0].message.content.strip()
        time.sleep(API_CALL_DELAY) # Respect rate limits
        
        # --- Parse JSON returned by model ---
        try:
            # Since we used response_format, it should be a direct JSON string
            result = json.loads(raw_output)
            
            # Verify required keys exist
            if "relevant" not in result or "label" not in result:
                print(f"    > ⚠️ Warning: JSON returned by model is missing 'relevant' or 'label' keys.")
                return None
            
            print(f"    > AI Analysis: Relevant={result['relevant']}, Label={result['label']}")
            return result # Return full JSON dictionary

        except json.JSONDecodeError:
            print(f"    > ❌ Error: Failed to parse JSON returned by model. Raw output: {raw_output}")
            return None
        except Exception as e:
            print(f"    > ❌ Unexpected error during JSON parsing: {e}")
            return None

    except Exception as e:
        print(f"    > ❌ Error calling DeepSeek AI API: {e}")
        traceback.print_exc()
        time.sleep(5) # Wait longer if API error occurs
        return None

# --- 4. Core Execution Logic ---
def process_single_asset(asset_name, client):
    """
    Executes the full JSON -> TXT -> Cleaned_Analyzed_TXT pipeline for a single asset.
    """
    print("\n" + "="*30 + f" Processing Asset: {asset_name} " + "="*30)

    # --- Dynamically generate paths and theme ---
    # Extract 'Cardano(ADA)' from '10_Cardano(ADA)'
    clean_asset_name = asset_name.split('_', 1)[1] if '_' in asset_name else asset_name
    # Create Analysis Theme
    theme = f"Cryptocurrency {clean_asset_name}"
    
    raw_json_folder = os.path.join(BASE_DATA_DIR, asset_name)
    intermediate_txt_folder = os.path.join(BASE_DATA_DIR, 'tweet_temp', asset_name)
    final_cleaned_folder = os.path.join(BASE_DATA_DIR, 'tweet_clean', asset_name)

    # --- Preparation: Ensure directories exist ---
    os.makedirs(raw_json_folder, exist_ok=True)
    os.makedirs(intermediate_txt_folder, exist_ok=True)
    os.makedirs(final_cleaned_folder, exist_ok=True)
    print(f"Theme set to: \"{theme}\"")

    # --- Phase 1: JSON -> TXT (Formatting) ---
    print("\n--- Phase 1: JSON -> TXT (Formatting) ---")
    json_files = [f for f in os.listdir(raw_json_folder) if f.endswith('.json')]
    if not json_files:
        print(f"🤷‍♂️ No .json files found in folder '{raw_json_folder}'.")
    else:
        for filename in tqdm(json_files, desc="Converting JSON files"):
            input_path = os.path.join(raw_json_folder, filename)
            output_filename = f"{os.path.splitext(filename)[0]}.txt"
            output_path = os.path.join(intermediate_txt_folder, output_filename)
            create_finetuning_dataset(input_path, output_path, sort_order=SORT_PREFERENCE)
            
    # --- Phase 2: TXT -> Cleaned TXT (AI Filtering & Analysis) ---
    print("\n--- Phase 2: TXT -> Cleaned TXT (AI Filtering & Analysis) ---")
    txt_files = [f for f in os.listdir(intermediate_txt_folder) if f.endswith('.txt')]
    if not txt_files:
        print(f"🤷‍♂️ No .txt files found in '{intermediate_txt_folder}' for cleaning.")
    else:
        for filename in txt_files:
            print(f"\n📄 Cleaning and analyzing file with AI: {filename}")
            input_path = os.path.join(intermediate_txt_folder, filename)
            base_name, extension = os.path.splitext(filename)
            # Final output filename
            output_filename = f"{base_name}_clean{extension}"
            output_path = os.path.join(final_cleaned_folder, output_filename)
            
            all_tweet_blocks = parse_full_tweet_blocks(input_path)
            if not all_tweet_blocks:
                print(" -> No tweets in file, skipping.")
                continue

            relevant_tweets_with_analysis = []
            
            # Use tqdm to show AI classification progress
            for block in tqdm(all_tweet_blocks, desc=f"AI Analyzing {filename}"):
                # Call the new "Two-in-One" analysis function
                analysis_result = analyze_relevance_and_sentiment(client, block, theme)
                
                # Check if API call succeeded and tweet is relevant
                if analysis_result and analysis_result.get("relevant") == True:
                    # --- Tweet is relevant, extract sentiment analysis and build [ANALYZEDATA] block ---
                    label = analysis_result.get("label", "Unknown")
                    keyword = analysis_result.get("key_word_used", "N/A")
                    reasoning = analysis_result.get("reasoning", "N/A")
                    
                    # Format new [ANALYZEDATA] block
                    analysis_block = (
                        f"\n---\n"
                        f"[ANALYZEDATA]\n"
                        f"- label: {label}\n"
                        f"- key_word_used: {keyword}\n"
                        f"- reasoning: {reasoning}\n"
                    )
                    
                    # Insert [ANALYZEDATA] block before [TWEET END]
                    modified_block = block.replace("[TWEET END]", analysis_block + "[TWEET END]")
                    relevant_tweets_with_analysis.append(modified_block)
                
                # elif analysis_result and analysis_result.get("relevant") == False:
                    # --- Tweet is irrelevant, discard (do nothing) ---
                    # print(f"    > Discarded (Irrelevant): ...") # (Uncomment to see detailed logs)
                    pass 
                
                # else:
                    # --- API Call Failed, also discard ---
                    # print(f"    > Discarded (API Error): ...") # (Uncomment to see detailed logs)
                    pass

            # --- Loop finished, save results ---
            if relevant_tweets_with_analysis:
                print(f" -> Found {len(relevant_tweets_with_analysis)}/{len(all_tweet_blocks)} relevant tweets.")
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write("\n\n".join(relevant_tweets_with_analysis))
                print(f" -> 🎉 Cleaned file with analysis saved to: {os.path.basename(output_path)}")
            else:
                print(f" -> 🤷‍♂️ No tweets matching the theme found in file '{filename}'.")

# --- 5. Main Execution ---
if __name__ == "__main__":
    if not API_KEY or "sk-" not in API_KEY: # Simple check
        print("🚨 Please enter your DeepSeek API Key in the configuration section at the top of the code.")
    else:
        print("🚀 Starting fully automated data processing pipeline (Filtering + Sentiment Analysis)...")
        
        # Initialize DeepSeek (OpenAI) Client
        deepseek_client = openai.OpenAI(
            api_key=API_KEY,
            base_url=BASE_URL
        )

        # --- Iterate through Asset List and process each one ---
        for asset in ASSET_LIST:
            process_single_asset(asset, deepseek_client)

        print("\n" + "="*80)
        print("✨✨✨ All assets processed! ✨✨✨")