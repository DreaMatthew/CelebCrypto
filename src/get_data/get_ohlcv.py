import requests
import pandas as pd
import time
from datetime import datetime, timedelta


# ========================
INTERVAL = "5m"       
LIMIT = 1000         
SLEEP_TIME = 8        


# ========================
tokens = [
    # ("1_Bitcoin(BTC)", "BTCUSDT", "Binance"),
    # ("2_Ethereum(ETH)", "ETHUSDT", "Binance"),
    # ("3_XRP(XRP)", "XRPUSDT", "Binance"),
    ## ("4_Tether(USDT)", "USDT/USD", "Kraken"),
    # ("5_BNB(BNB)", "BNBUSDT", "Binance"),
    # ("6_Solana(SOL)", "SOLUSDT", "Binance"),
    # ("7_USDC(USDC)", "USDCUSDT", "Binance"),
    # ("8_Dogecoin(DOGE)", "DOGEUSDT", "Binance"),
    # # ("9_Lido Staked Ether(STETH)", "STETH/USD", "Bybit"),
    # ("10_Cardano(ADA)", "ADAUSDT", "Binance"),
    # ("11_TRON(TRX)", "TRXUSDT", "Binance"),
    # # ("12_Wrapped stETH(WSTETH)", "XRP/USD", "Binance"),
    ("13_Chainlink(LINK)", "LINKUSDT", "Binance"),
    # # ("14_Hyperliquid(HYPE)", "HYPE/USDT", "Kucoin"),
    # # ("15_Wrapped Beacon ETH(WBETH)", "XRP/USD", "Binance"),
    # # ("16_Wrapped Bitcoin(WBTC)", "WBTC/USD", "Synthetic"),
    # ("17_Avalanche(AVAX)", "AVAXUSDT", "Binance"),
    # # ("18_Ethena USDe(USDE)", "USDE/USD", "Uniswap"),
    # # ("19_Sui(SUI)", "SUIUSDT", "Binance"),
    # # ("20_Figure Heloc(FIGR_HELOC)", "XRP/USD", "Binance"),
    # ("21_Bitcoin Cash(BCH)", "BCHUSDT", "Binance"),
    # ("22_Stellar(XLM)", "XLMUSDT", "Binance"),
    # # ("23_Wrapped eETH(WEETH)", "XRP/USD", "Binance"),
    # # ("24_WETH(WETH)", "XRP/USD", "Binance"),
    # ("25_Hedera(HBAR)", "HBARUSDT", "Binance"),
    # #("26_LEO Token(LEO)", "LEO/USD", "Bitfinex"),
    # ("27_Litecoin(LTC)", "LTCUSDT", "Binance"),
    # # ("28_Cronos(CRO)", "CRO/USD", "Kucoin"),
    # # ("29_Toncoin(TON)", "TON/USD", "Bybit"),
    # # ("30_USDS(USDS)", "XRP/USD", "Binance"),
    # # ("31_Shiba Inu(SHIB)", "SHIB/USD", "Coinbase pro"),
    # # ("32_Coinbase Wrapped BTC(CBBTC)", "CBBTC/USD", "Uniswap"),#没数据
    # # ("33_Polkadot(DOT)", "XRP/USD", "Binance"),
    # # ("34_Binance Bridged USDT(BNB Smart Chain)", "XRP/USD", "Binance"),
    # # ("35_WhiteBIT Coin(WBT)", "WBT/USDT", "Huobi"),
    # # ("36_World Liberty Financial(WLFI)", "XRP/USD", "Binance"),
    # # ("37_Ethena Staked USDe(SUSDE)", "XRP/USD", "Binance"),
    # ("38_Uniswap(UNI)", "UNIUSDT", "Binance"),
    # # ("39_Mantle(MNT)", "MNT/USD", "Bybit"),
    # # ("40_Monero(XMR)", "XMR/USD", "HitBTC"),
    # # ("41_MemeCore(M)", "XRP/USD", "Binance"),
    # ("42_Ethena(ENA)", "ENAUSDT", "Binance"),
    # ("43_Pepe(PEPE)", "PEPEUSDT", "Binance"),
    # ("44_Aave(AAVE)", "AAVEUSDT", "Binance"),
    # # ("45_Bitget Token(BGB)", "XRP/USD", "Binance"),
    # # ("46_Dai(DAI)", "DAI/USD", "Coinbase pro"),
    # # ("47_OKB(OKB)", "OKB/USD", "OKEx"),
    # # ("48_Jito Staked SOL(JITOSOL)", "XRP/USD", "Binance"),
    # ("49_NEAR Protocol(NEAR)", "NEARUSDT", "Binance"),
    # ("50_Bittensor(TAO)", "TAOUSDT", "Binance"),
    # # ("51_Story(IP)", "XRP/USD", "Binance"),
    # # ("52_Ondo(ONDO)", "ONDO/USD", "Bybit"),
    # ("53_Worldcoin(WLD)", "WLDUSDT", "Binance"),
    # ("54_Aptos(APT)", "APTUSDT", "Binance"),
    # # ("55_MYX Finance(MYX)", "MYX/USD", "Binance"),
    # ("56_Ethereum Classic(ETC)", "ETCUSDT", "Binance"),
    # # ("57_Binance Staked SOL(BNSOL)", "APT/USD", "Binance"),
    # # ("58_Pi Network(PI)", "APT/USD", "Binance"),
    # # ("59_Pump.fun(PUMP)", "APT/USD", "Binance"),
    # # ("60_USDT0(USDT0)", "APT/USD", "Binance"),
    # # ("61_Binance-Peg WETH(WETH)", "APT/USD", "Binance"),
    # ("62_Arbitrum(ARB)", "ARBUSDT", "Binance"),
    # ("63_POL(ex-MATIC)(POL)", "POLUSDT", "Binance"),
    # # ("64_USD1(USD1)", "APT/USD", "Binance"),
    # ("65_Internet Computer(ICP)", "ICPUSDT", "Binance"),
    # ("66_Pudgy Penguins(PENGU)", "PENGUUSDT", "Binance"),
    # # ("67_Kinetiq Staked HYPE(KHYPE)", "APT/USD", "Binance"),
    # # ("68_Kaspa(KAS)", "APT/USD", "Binance"),
    # # ("69_Jupiter Perpetuals Liquidity Provider Token(JLP)", "APT/USD", "Binance"),
    # ("70_VeChain(VET)", "VETUSDT", "Binance"),
    # # ("71_BlackRock USD Institutional Digital Liquidity Fund(BUIDL)", "APT/USD", "Binance"),
    # ("72_Cosmos Hub(ATOM)", "ATOMUSDT", "Binance"),
    # # ("73_Kelp DAO Restaked ETH(RSETH)", "APT/USD", "Binance"),
    # ("74_Algorand(ALGO)", "ALGOUSDT", "Binance"),
    # # ("75_sUSDS(SUSDS)", "APT/USD", "Binance"),
    # # ("76_Rocket Pool ETH(RETH)", "APT/USD", "Binance"),
    # ("77_Render(RENDER)", "RENDERUSDT", "Binance"),
    # # ("78_Gate(GT)", "GT/CAD", "Synthetic"),
    # # ("79_KuCoin(KCS)", "KCS/USD", "Kucoin"),
    # ("80_Sei(SEI)", "SEIUSDT", "Binance"),
    # # ("81_Fasttoken(FTN)", "APT/USD", "Binance"),
    # ("82_Bonk(BONK)", "BONKUSDT", "Binance"),
    # # ("83_Falcon USD(USDF)", "APT/USD", "Binance"),
    # # ("84_USDtb(USDTB)", "APT/USD", "Binance"),
    # # ("85_Provenance Blockchain(HASH)", "APT/USD", "Binance"),
    # # ("86_Sky(SKY)", "SKY/USD", "Synthetic"),
    # # ("87_Flare(FLR)", "APT/USD", "Binance"),
    # ("88_Filecoin(FIL)", "FILUSDT", "Binance"),
    # ("89_Official Trump(TRUMP)", "TRUMPUSDT", "Binance"),
    # # ("90_Artificial Superintelligence Alliance(FET)", "APT/USD", "Binance"),
    # # ("91_StakeWise Staked ETH(OSETH)", "APT/USD", "Binance"),
    # # ("92_BFUSD(BFUSD)", "APT/USD", "Binance"),
    # # ("93_Jupiter(JUP)", "APT/USD", "Binance"),
    # # ("94_Liquid Staked ETH(LSETH)", "APT/USD", "Binance"),
    # # ("95_Lombard Staked BTC(LBTC)", "APT/USD", "Binance"),
    # # ("96_Renzo Restaked ETH(EZETH)", "APT/USD", "Binance"),
    # # ("97_Immutable(IMX)", "IMXM/USD", "Huobi"),
    # ("98_Quant(QNT)", "QNTUSDT", "Binance"),
    # # ("99_Optimism(OP)", "OPM/USD", "Huobi"),
    # # ("100_Polygon Bridged USDT(Polygon)(USDT)", "APT/USD", "Binance"),
    # # ... 
]

BASE_URL = "https://api.binance.com/api/v3/klines"

# ========================
# start_date = datetime.strptime("2025-08-25 00:00:00", "%Y-%m-%d %H:%M:%S")
# end_date   = datetime.strptime("2025-09-25 23:59:59", "%Y-%m-%d %H:%M:%S")

start_date = datetime.strptime("2025-08-24 14:00:00", "%Y-%m-%d %H:%M:%S")
end_date   = datetime.strptime("2025-09-05 23:59:59", "%Y-%m-%d %H:%M:%S")

# ========================
def get_klines(symbol, interval, start_time, end_time):
    all_data = []
    while True:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": int(start_time.timestamp() * 1000),
            "endTime": int(end_time.timestamp() * 1000),
            "limit": LIMIT
        }
        try:
            resp = requests.get(BASE_URL, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"⚠️ error")
            break

        if not data:
            break

        all_data.extend(data)

        last_time = datetime.fromtimestamp(data[-1][0] / 1000)
        if last_time >= end_time or len(data) < LIMIT:
            break
        start_time = last_time + timedelta(milliseconds=1)

        time.sleep(0.2)  

    return all_data


# ========================
for token in tokens:
    name, symbol, _ = token 
    print(f"🔄 getting data of {name} ({symbol}) ...")

    data = get_klines(symbol, INTERVAL, start_date, end_date)

    if not data:
        print(f"❌ {name} ({symbol}) has no data")
        continue

    df = pd.DataFrame(data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
    ])
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
    df = df[["open_time", "open", "high", "low", "close", "volume"]]

    filename = f"{name}_{symbol}_{INTERVAL}.csv"
    df.to_csv(filename, index=False)
    print(f"✅ save {filename} ({len(df)} rows)")

    time.sleep(SLEEP_TIME)  
