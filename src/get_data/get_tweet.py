import json
import os 
from datetime import date, timedelta
from apify_client import ApifyClient

client = ApifyClient("apify_api_yourapi")

# ==============================================================================
TASK_LIST = [

    # {
    #     'folder': '31_Shiba Inu(SHIB)',
    #     'query': '"Shiba Inu" OR "$SHIB"'
    # },
    # {
    #     'folder': '32_Coinbase Wrapped BTC',
    #     'query': '"Coinbase Wrapped BTC" OR "$CBBTC"'
    # },
    # {
    #     'folder': '33_Polkadot(DOT)',
    #     'query': '"$DOT" OR Polkadot'
    # },
    # {
    #     'folder': '34_Binance Bridged USDT(BNB Smart Chain)',
    #     'query': '"Binance Bridged USDT" OR "BNB Smart Chain"'
    # },
    # {
    #     'folder': '35_WhiteBIT Coin(WBT)',
    #     'query': '$WBT OR "WhiteBIT Coin" '
    # },
    # {
    #     'folder': '36_World Liberty Financial(WLFI)',
    #     'query': '$WLFI OR "World Liberty Financial"'
    # },
    # {
    #     'folder': '37_Ethena Staked USDe(SUSDE)',
    #     'query': 'SUSDE OR "Ethena Staked USDe"'
    # },
    # {
    #     'folder': '38_Uniswap(UNI)',
    #     'query': '$UNI OR Uniswap'
    # },
    # {
    #     'folder': '39_Mantle(MNT)',
    #     'query': '$MNT OR Mantle'
    # },
    # {
    #     'folder': '40_Monero(XMR)',
    #     'query': '$XMR OR Monero'
    # },
    # {
    #     'folder': '41_MemeCore(M)',
    #     'query': 'MemeCore OR "$M"'
    # },
    # {
    #     'folder': '42_Ethena(ENA)',
    #     'query': '"$ENA" OR Ethena'
    # },
    # {
    #     'folder': '43_Pepe(PEPE)',
    #     'query': '$PEPE'
    # },
    # {
    #     'folder': '44_Aave(AAVE)',
    #     'query': '$AAVE'
    # },
    # {
    #     'folder': '45_Bitget Token(BGB)',
    #     'query': '$BGB OR "Bitget Token"'
    # },
    # {
    #     'folder': '46_Dai(DAI)',
    #     'query': '"$DAI"'
    # },
    # {
    #     'folder': '47_OKB(OKB)',
    #     'query': '$OKB'
    # },
    # {
    #     'folder': '48_Jito Staked SOL(JITOSOL)',
    #     'query': '$JITOSOL OR "Jito Staked SOL"'
    # },
    {
        'folder': '49_NEAR Protocol(NEAR)',
        'query': '$NEAR'
    },
    # {
    #     'folder': '50_Bittensor(TAO)',
    #     'query': 'Bittensor OR $TAO'
    # },
    # {
    #     'folder': '51_Story(IP)',
    #     'query': '"$IP"'
    # },
    # {
    #     'folder': '52_Ondo(ONDO)',
    #     'query': 'ONDO'
    # },
    # {
    #     'folder': '53_Worldcoin(WLD)',
    #     'query': 'Worldcoin OR WLD'
    # },
    # {
    #     'folder': '54_Aptos(APT)',
    #     'query': '"$APT" OR Aptos'
    # },
    # {
    #     'folder': '55_MYX Finance(MYX)',
    #     'query': 'MYX'
    # },
    # {
    #     'folder': '56_Ethereum Classic(ETC)',
    #     'query': '"Ethereum Classic" OR "$ETC"'
    # },
    # {  
    #     'folder': '57_Binance Staked SOL(BNSOL)',
    #     'query': '"Binance Staked SOL" OR "$BNSOL"'
    # },
        {
        'folder': '58_Pi Network(PI)',
        'query': '"Pi Network" OR "$PI"'
    },
    {
        'folder': '59_Pump.fun(PUMP)',
        'query': '$PUMP'
    },
    {
        'folder': '60_USDT0(USDT0)',
        'query': '"USDT0" OR "$USDT0"'
    },
    {
        'folder': '61_Binance-Peg WETH(WETH)',
        'query': '"Binance-Peg WETH" OR "$WETH"'
    },
    {
        'folder': '62_Arbitrum(ARB)',
        'query': '"$ARB"'
    },
    {
        'folder': '63_POL(ex-MATIC)(POL)',
        'query': '"$POL"'
    },
        {
        'folder': '64_USD1(USD1)',
        'query': 'USD1'
    },
    {
        'folder': '65_Internet Computer(ICP)',
        'query': '$ICP'
    },
    {
        'folder': '66_Pudgy Penguins(PENGU)',
        'query': '$PENGU OR "Pudgy Penguins"'
    },
    {
        'folder': '67_Kinetiq Staked HYPE(KHYPE)',
        'query': 'KHYPE OR "Kinetiq Staked HYPE"'
    },
    {
        'folder': '68_Kaspa(KAS)',
        'query': '$KAS OR Kaspa'
    },
    {
        'folder': '69_Jupiter Perpetuals Liquidity Provider Token(JLP)',
        'query': '$JLP OR "Jupiter Perpetuals Liquidity Provider Token"'
    },
        {
        'folder': '70_VeChain(VET)',
        'query': 'VET OR VeChain'
    },
    {
        'folder': '71_BlackRock USD Institutional Digital Liquidity Fund(BUIDL)',
        'query': '$BUIDL OR "BlackRock USD Institutional Digital Liquidity Fund"'
    },
    {
        'folder': '72_Cosmos Hub(ATOM)',
        'query': 'Cosmos Hub(ATOM)'
    },
    {
        'folder': '73_Kelp DAO Restaked ETH(RSETH)',
        'query': '$RSETH OR "Kelp DAO Restaked ETH"'
    },
    {
        'folder': '74_Algorand(ALGO)',
        'query': '$ALGO OR Algorand'
    },
    {
        'folder': '75_sUSDS(SUSDS)',
        'query': 'SUSDS'
    },
        {
        'folder': '76_Rocket Pool ETH(RETH)',
        'query': '"Rocket Pool ETH" OR "$rETH"'
    },
    {
        'folder': '77_Render(RENDER)',
        'query': '$RENDER'
    },
    {
        'folder': '78_Gate(GT)',
        'query': '$GT'
    },
    {
        'folder': '79_KuCoin(KCS)',
        'query': '$KCS OR KuCoin'
    },
    {
        'folder': '80_Sei(SEI)',
        'query': 'SEI'
    },
    {
        'folder': '81_Fasttoken(FTN)',
        'query': 'Fasttoken OR $FTN'
    },
        {
        'folder': '82_Bonk(BONK)',
        'query': 'BONK'
    },
    {
        'folder': '83_Falcon USD(USDF)',
        'query': 'USDF OR "Falcon USD"'
    },
    {
        'folder': '84_USDtb(USDTB)',
        'query': 'USDTB'
    },
    {
        'folder': '85_Provenance Blockchain(HASH)',
        'query': '$HASH OR "Provenance Blockchain"'
    },
    {
        'folder': '86_Sky(SKY)',
        'query': '$SKY'
    },
    {
        'folder': '87_Flare(FLR)',
        'query': '$FLR'
    },
        {
        'folder': '88_Filecoin(FIL)',
        'query': 'Filecoin OR FIL'
    },
    {
        'folder': '89_Official Trump(TRUMP)',
        'query': '$TRUMP'
    },
    {
        'folder': '90_Artificial Superintelligence Alliance(FET)',
        'query': '$FET OR "Artificial Superintelligence Alliance"'
    },
    {
        'folder': '91_StakeWise Staked ETH(OSETH)',
        'query': '"StakeWise Staked ETH" OR $OSETH'
    },
    {
        'folder': '92_BFUSD(BFUSD)',
        'query': 'BFUSD'
    },
    {
        'folder': '93_Jupiter(JUP)',
        'query': '$JUP'
    },
        {
        'folder': '94_Liquid Staked ETH(LSETH)',
        'query': 'LSETH OR "Liquid Staked ETH"'
    },
    {
        'folder': '95_Lombard Staked BTC(LBTC)',
        'query': 'LBTC OR "Lombard Staked BTC"'
    },
    {
        'folder': '96_Renzo Restaked ETH(EZETH)',
        'query': '“Renzo Restaked ETH” OR EZETH'
    },
    {
        'folder': '97_Immutable(IMX)',
        'query': 'Immutable OR $IMX'
    },
    {
        'folder': '98_Quant(QNT)',
        'query': '$QNT'
    },
    {
        'folder': '99_Optimism(OP)',
        'query': '$OP'
    },
        {
        'folder': '100_Polygon Bridged USDT(Polygon)(USDT)',
        'query': '"Polygon Bridged USDT"'
    },
    # {
    #     'folder': '',
    #     'query': ''
    # },
    # {
    #     'folder': '',
    #     'query': ''
    # },
    # {
    #     'folder': '',
    #     'query': ''
    # },
    # {
    #     'folder': '',
    #     'query': ''
    # },
    # {
    #     'folder': '',
    #     'query': ''
    # },
]

# ==============================================================================

start_date = date(2025, 9, 7)
TOTAL_DATA_LIMIT = 2000
base_run_input = {
    "maxItems": 1000,
    "sort": "Top",
    "tweetLanguage": "en",
    "minimumFavorites": 50,
}
# ==============================================================================
def run_scrape_task(folder_name, search_query):
    """
    Executes the full daily scraping process for a single task.
    """
    print("="*80)
    print(f"🚀 Starting new task: [{folder_name}]")
    print(f"🔍 Using keywords: \"{search_query}\"")
    
    # --- Create folder for current task (if it doesn't exist) ---
    os.makedirs(folder_name, exist_ok=True)
    
    # --- Define specific log file path for this task ---
    log_file_path = os.path.join(folder_name, "scrape_log.txt")
    
    # Initialize counter and current date
    total_tweets_collected = 0
    current_date = start_date

    # --- Start daily scraping loop ---
    while total_tweets_collected < TOTAL_DATA_LIMIT:
        # Check if date is in the future
        if current_date > date.today(): # Using >= is safer depending on requirements
            print("="*50)
            print(f"▶️ Current date ({current_date.strftime('%Y-%m-%d')}) has reached or passed today. Stopping scrape for this task.")
            break

        # Calculate and format dates
        start_of_period = current_date
        end_of_period = current_date + timedelta(days=1)
        start_date_str = start_of_period.strftime("%Y-%m-%d")
        end_date_str = end_of_period.strftime("%Y-%m-%d")

        print("="*50)
        print(f"▶️ Preparing to scrape: {start_date_str} to {end_date_str}")

        # Prepare full input for this run
        run_input = base_run_input.copy()
        run_input["searchTerms"] = [search_query] # <-- Use keywords for current task
        run_input["start"] = start_date_str
        run_input["end"] = end_date_str
        
        # # --- Uncomment this section for actual execution ---
        print("⏳ Running Apify Actor, please wait...")
        try:
            # Note: Ensure the Actor ID below is correct for your use case
            run = client.actor("61RPP7dywgiy0JPD0").call(run_input=run_input)
            print("✅ Actor run complete! Fetching results...")
            daily_tweet_data = []
            for item in client.dataset(run["defaultDatasetId"]).iterate_items():
                daily_tweet_data.append(item)
        except Exception as e:
            print(f"❌ Actor run error: {e}")
            daily_tweet_data = [] # Treat as no data found on error
        # # ------------------------------------------------
        
        # # --- (Use mock data for testing/debugging purposes) ---
        # print("⏳ (Mock) Running Apify Actor...")
        # daily_tweet_data = [{'mock_data': f'data for {folder_name} on {start_date_str}'}]
        # print("✅ (Mock) Actor run complete!")
        # ---------------------------------------------
        
        num_found_this_run = len(daily_tweet_data)
        total_tweets_collected += num_found_this_run
        
        print(f"🎉 Retrieved {num_found_this_run} items this run!")
        print(f"Cumulative total: {total_tweets_collected} / {TOTAL_DATA_LIMIT}")

        # (A) Save current data to JSON file named by date
        if num_found_this_run > 0:
            filename_date_part = start_of_period.strftime("%Y%m%d") + "-" + end_of_period.strftime("%Y%m%d")
            # --- Ensure file is saved in the correct task folder ---
            json_file_path = os.path.join(folder_name, f"{filename_date_part}.json")
            
            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(daily_tweet_data, f, ensure_ascii=False, indent=4)
            print(f"💾 Data successfully saved to file: {json_file_path}")
        
        # (B) Record results to log file
        with open(log_file_path, 'a', encoding='utf-8') as log_f:
            log_f.write(f"Date {start_date_str} to {end_date_str}: Retrieved {num_found_this_run} items\n")
        print(f"📄 Log recorded to: {log_file_path}")
        
        # Advance date by one day for the next iteration
        current_date += timedelta(days=1)

    # --- Loop finished, print and record final summary ---
    print("="*50)
    final_summary_line = f"Total items collected: {total_tweets_collected}."
    
    # Determine reason for completion
    if total_tweets_collected >= TOTAL_DATA_LIMIT:
        reason = f"Task complete! Total data limit reached or exceeded {TOTAL_DATA_LIMIT} items."
    else:
        reason = f"Task complete! Reached latest date."

    print(f"🏁 {reason}")
    print(final_summary_line)

    with open(log_file_path, 'a', encoding='utf-8') as log_f:
        log_f.write("="*20 + "\n")
        log_f.write(reason + "\n")
        log_f.write(final_summary_line + "\n")
    print(f"📄 Final summary recorded to: {log_file_path}")
    print(f"✅ Task [{folder_name}] processing finished.")


# ==============================================================================
# --- 4. Main Entry Point ---
# ==============================================================================
if __name__ == "__main__":
    # --- Iterate through task list and execute scrape function ---
    for task in TASK_LIST:
        run_scrape_task(
            folder_name=task['folder'], 
            search_query=task['query']
        )
    
    print("\n\n" + "="*80)
    print("🎉🎉🎉 All tasks processed! Script finished. 🎉🎉🎉")
    print("="*80)