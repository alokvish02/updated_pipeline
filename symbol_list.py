import csv
from services.config import redis_client


# BINANCE_SYMBOLS = ['BTCUSDT','ETHUSDT','XRPUSDT','BNBUSDT','SOLUSDT','USDCUSDT','TRXUSDT','DOGEUSDT','ADAUSDT','HYPEUSDT','BCHUSDT','SUIUSDT','LINKUSDT','LEOUSDT','XLMUSDT','AVAXUSDT','TONUSDT','SHIBUSDT']
# BINANCE_SYMBOLS = ["ADAUSDT","AVAXUSDT","BNBUSDT","BTCUSDT","DOGEUSDT","ETHUSDT","FILUSDT","LINKUSDT","LTCUSDT","SUSHIUSDT","TRXUSDT","UNIUSDT","VETUSDT","XLMUSDT","XRPUSDT"]


# BINANCE_SYMBOLS = ["AVAXUSDT","BNBUSDT","BTCUSDT","DOGEUSDT","ETHUSDT","FILUSDT","LINKUSDT","LTCUSDT","SUSHIUSDT","TRXUSDT","UNIUSDT","VETUSDT","XLMUSDT","XRPUSDT"]

BINANCE_SYMBOLS = ["BTCUSDT","ETHUSDT",]

# FYERS_SYMBOLS = ["NSE:ABB-EQ","NSE:ACC-EQ","NSE:APLAPOLLO-EQ","NSE:AUBANK-EQ","NSE:AARTIIND-EQ",
#                  "NSE:ABBOTINDIA-EQ","NSE:ADANIENSOL-EQ","NSE:ADANIENT-EQ","NSE:ADANIGREEN-EQ",
#                  "NSE:ADANIPORTS-EQ","NSE:ATGL-EQ","NSE:ABCAPITAL-EQ","NSE:ABFRL-EQ","NSE:ALKEM-EQ",
#                  "NSE:AMBUJACEM-EQ","NSE:ANGELONE-EQ","NSE:APOLLOHOSP-EQ","NSE:APOLLOTYRE-EQ","NSE:ASHOKLEY-EQ","NSE:ASIANPAINT-EQ","NSE:ASTRAL-EQ","NSE:ATUL-EQ","NSE:AUROPHARMA-EQ","NSE:DMART-EQ","NSE:AXISBANK-EQ","NSE:BSOFT-EQ","NSE:BSE-EQ","NSE:BAJFINANCE-EQ","NSE:BAJAJFINSV-EQ","NSE:BALKRISIND-EQ","NSE:BANDHANBNK-EQ","NSE:BANKBARODA-EQ","NSE:BANKINDIA-EQ","NSE:BATAINDIA-EQ","NSE:BERGEPAINT-EQ","NSE:BEL-EQ","NSE:BHARATFORG-EQ","NSE:BHEL-EQ","NSE:BPCL-EQ","NSE:BHARTIARTL-EQ","NSE:BIOCON-EQ","NSE:BOSCHLTD-EQ","NSE:BRITANNIA-EQ","NSE:CESC-EQ","NSE:CGPOWER-EQ","NSE:CANFINHOME-EQ","NSE:CANBK-EQ","NSE:CDSL-EQ","NSE:CHAMBLFERT-EQ","NSE:CHOLAFIN-EQ","NSE:CIPLA-EQ","NSE:CUB-EQ","NSE:COALINDIA-EQ","NSE:COFORGE-EQ","NSE:COLPAL-EQ","NSE:CAMS-EQ","NSE:CONCOR-EQ","NSE:COROMANDEL-EQ","NSE:CROMPTON-EQ","NSE:CUMMINSIND-EQ","NSE:CYIENT-EQ","NSE:DLF-EQ","NSE:DABUR-EQ","NSE:DALBHARAT-EQ","NSE:DEEPAKNTR-EQ","NSE:DELHIVERY-EQ","NSE:DIVISLAB-EQ","NSE:DIXON-EQ","NSE:LALPATHLAB-EQ","NSE:DRREDDY-EQ","NSE:EICHERMOT-EQ","NSE:ESCORTS-EQ","NSE:EXIDEIND-EQ","NSE:NYKAA-EQ","NSE:GAIL-EQ","NSE:GMRAIRPORT-EQ","NSE:GLENMARK-EQ","NSE:GODREJCP-EQ","NSE:GODREJPROP-EQ","NSE:GRANULES-EQ","NSE:GRASIM-EQ","NSE:GUJGASLTD-EQ","NSE:GNFC-EQ","NSE:HCLTECH-EQ","NSE:HDFCAMC-EQ","NSE:HDFCBANK-EQ","NSE:HDFCLIFE-EQ","NSE:HFCL-EQ","NSE:HAVELLS-EQ","NSE:HEROMOTOCO-EQ","NSE:HINDALCO-EQ","NSE:HAL-EQ","NSE:HINDCOPPER-EQ","NSE:HINDPETRO-EQ","NSE:HINDUNILVR-EQ","NSE:HUDCO-EQ","NSE:ICICIBANK-EQ","NSE:ICICIGI-EQ","NSE:ICICIPRULI-EQ","NSE:IDFCFIRSTB-EQ","NSE:IPCALAB-EQ","NSE:IRB-EQ","NSE:ITC-EQ","NSE:INDIAMART-EQ","NSE:INDIANB-EQ","NSE:IEX-EQ","NSE:IOC-EQ","NSE:IRCTC-EQ","NSE:IRFC-EQ","NSE:IGL-EQ","NSE:INDUSTOWER-EQ","NSE:INDUSINDBK-EQ","NSE:NAUKRI-EQ","NSE:INFY-EQ","NSE:INDIGO-EQ","NSE:JKCEMENT-EQ","NSE:JSWENERGY-EQ","NSE:JSWSTEEL-EQ","NSE:JSL-EQ","NSE:JINDALSTEL-EQ","NSE:JIOFIN-EQ","NSE:JUBLFOOD-EQ","NSE:KEI-EQ","NSE:KPITTECH-EQ","NSE:KALYANKJIL-EQ","NSE:KOTAKBANK-EQ","NSE:LTF-EQ","NSE:LTTS-EQ","NSE:LICHSGFIN-EQ","NSE:LTIM-EQ","NSE:LT-EQ","NSE:LAURUSLABS-EQ","NSE:LICI-EQ","NSE:LUPIN-EQ","NSE:MRF-EQ","NSE:LODHA-EQ","NSE:MGL-EQ","NSE:M&MFIN-EQ","NSE:M&M-EQ","NSE:MANAPPURAM-EQ","NSE:MARICO-EQ","NSE:MARUTI-EQ",


# "NSE:MFSL-EQ","NSE:MAXHEALTH-EQ","NSE:METROPOLIS-EQ","NSE:MPHASIS-EQ","NSE:MCX-EQ","NSE:MUTHOOTFIN-EQ","NSE:NBCC-EQ","NSE:NCC-EQ","NSE:NHPC-EQ","NSE:NMDC-EQ","NSE:NTPC-EQ","NSE:NATIONALUM-EQ","NSE:NAVINFLUOR-EQ","NSE:NESTLEIND-EQ","NSE:OBEROIRLTY-EQ","NSE:ONGC-EQ","NSE:OIL-EQ","NSE:PAYTM-EQ","NSE:OFSS-EQ","NSE:POLICYBZR-EQ","NSE:PIIND-EQ","NSE:PVRINOX-EQ","NSE:PAGEIND-EQ","NSE:PERSISTENT-EQ","NSE:PETRONET-EQ",
# "NSE:PIDILITIND-EQ","NSE:PEL-EQ","NSE:POLYCAB-EQ","NSE:POONAWALLA-EQ","NSE:PFC-EQ","NSE:POWERGRID-EQ","NSE:PRESTIGE-EQ","NSE:PNB-EQ","NSE:RBLBANK-EQ","NSE:RECLTD-EQ","NSE:RELIANCE-EQ","NSE:SBICARD-EQ","NSE:SBILIFE-EQ","NSE:SHREECEM-EQ","NSE:SJVN-EQ","NSE:SRF-EQ","NSE:MOTHERSON-EQ","NSE:SHRIRAMFIN-EQ","NSE:SIEMENS-EQ","NSE:SOLARINDS-EQ","NSE:SONACOMS-EQ","NSE:SBIN-EQ","NSE:SAIL-EQ","NSE:SUNPHARMA-EQ","NSE:SUNTV-EQ","NSE:SUPREMEIND-EQ","NSE:SYNGENE-EQ","NSE:TATACONSUM-EQ","NSE:TVSMOTOR-EQ","NSE:TATACHEM-EQ","NSE:TATACOMM-EQ","NSE:TCS-EQ","NSE:TATAELXSI-EQ","NSE:TATAMOTORS-EQ","NSE:TATAPOWER-EQ","NSE:TATASTEEL-EQ","NSE:TECHM-EQ","NSE:FEDERALBNK-EQ","NSE:INDHOTEL-EQ","NSE:PHOENIXLTD-EQ","NSE:RAMCOCEM-EQ","NSE:TITAN-EQ","NSE:TORNTPHARM-EQ","NSE:TORNTPOWER-EQ","NSE:TRENT-EQ","NSE:TIINDIA-EQ","NSE:UPL-EQ","NSE:ULTRACEMCO-EQ","NSE:UNIONBANK-EQ",
#     "NSE:UBL-EQ",
#     "NSE:UNITDSPR-EQ","NSE:VBL-EQ","NSE:VEDL-EQ","NSE:IDEA-EQ","NSE:VOLTAS-EQ","NSE:WIPRO-EQ","NSE:YESBANK-EQ","NSE:ZYDUSLIFE-EQ"
# ]


FYERS_SYMBOLS = ["NSE:ACC-EQ","NSE:AMBUJACEM-EQ","NSE:BPCL-EQ","NSE:BERGEPAINT-EQ","NSE:ASIANPAINT-EQ","NSE:HINDPETRO-EQ","NSE:HINDALCO-EQ","NSE:HINDCOPPER-EQ"]


SNP_SYMBOLS  = ['A','AA','AAPL','ABBV','ABNB','ABT','ACN','ADBE','ADI','ADM',
  'ADP','ADSK','AEE','AEP','AES','AFL','AIG','AIV','AIZ','AKAM',
  'ALL','AMAT','AMCR','AMD','AMGN','AMP','AMT','AMZN','ANET','ANF',
  'AON','AOS','APA','APD','APH','APO','ARE','ARM','ASML','ATI',
  'ATO','AVB','AVGO','AVY','AXON','AXP','AZN','AZO','BA','BABA',
  'BAC','BALL','BAX','BBY','BDX','BEAM','BEN','BG','BHP','BIIB',
  'BIO','BK','BKNG','BLK','BMY','BN','BP','BRO','BSX','BTI',
  'BTU','BUD','BWA','BX','BXP','C','CA','CAG','CAH','CARR',
  'CAT','CB','CBOE','CCI','CCL','CDNS','CE','CEG','CF','CFG',
  'CHRW','CI','CINF','CL','CLF','CLX','CMA','CMCSA','CME','CMG',
  'CMI','CMS','CNP','CNQ','CNX','COF','COO','COP','COST','CP',
  'CPB','CPT','CRL','CRM','CRWD','CSCO','CSX','CTAS','CTRA','CTSH',
  'CVS','CVX','CZR','D','DD','DE','DELL','DEO','DFS','DGX',
  'DHI','DHR','DIS','DLTR','DNB','DOV','DOW','DPZ','DRI','DTE',
  'DUK','DV','DVA','DVN','EA','EBAY','ECL','ED','EFX','EG',
  'EIX','EL','ELV','EMC','EMN','EMR','ENB','ENPH','EOG','EPAM',
  'EQIX','EQNR','EQR','EQT','ES','ESS','ETN','ETR','EVRG','EW',
  'EXC','EXPD','EXPE','F','FAST','FCX','FDS','FDX','FE','FFIV',
  'FHN','FI','FICO','FIS','FITB','FLR','FLS','FMC','FOSL','FOX',
  'FOXA','FSLR','FTI','GCI','GD','GE','GEN','GEV','GILD','GIS',
  'GL','GLW','GME','GNW','GOOG','GOOGL','GPC','GRMN','GS','GSK',
  'HBAN','HCA','HD','HDB','HIG','HOLX','HON','HPE','HSBC','HST',
  'HWM','IBM','IBN','ICE','IEX','IFF','INCY','INFY','INTC','INTU',
  'INVH','IP','IPG','IRM','ISRG','ITW','J','JBHT','JKHY','JNJ',
  'JPM','KEY','KIM','KKR','KLAC','KMX','KO','KOF','L','LDOS',
  'LH','LIN','LKQ','LLY','LMT','LNT','LOW','LRCX','LUV','LW',
  'LYV','MA','MAA','MAR','MAS','MCD','MCO','MDLZ','MDT','MELI',
  'META','MGM','MKC','MMC','MMM','MO','MOH','MOS','MRK','MS',
  'MSFT','MSI','MTCH','MU','MUFG','NDSN','NEE','NFLX','NI','NKE',
  'NOC','NOW','NTAP','NTRS','NVDA','NVO','NVR','NVS','NWS','NWSA',
  'OMC','ORCL','PANW','PAYC','PBR','PDD','PEP','PFE','PFG','PG',
  'PGR','PH','PHM','PKG','PLD','PLTR','PM','PNC','PNR','PODD',
  'POOL','PPL','PTC','PYPL','QCOM','RACE','REG','REGN','RELX','RF',
  'RIO','RJF','ROL','RTX','RVTY','RY','SAN','SAP','SBUX','SCCO',
  'SCHW','SHEL','SHOP','SHW','SIRI','SJM','SMFG','SNA','SNPS','SNY',
  'SO','SPGI','STE','STLD','STX','SWK','SWKS','SYF','SYK','T',
  'TAP','TD','TDG','TDY','TECH','TER','TFX','TGT','TJX','TM',
  'TMO','TMUS','TRGP','TRI','TRMB','TSLA','TSM','TSN','TT','TTE',
  'TXN','TXT','TYL','UAL','UBER','UBS','UDR','UL','ULTA','UNH',
  'UNP','UPS','V','VRSN','VRTX','VTRS','VZ','WAB','WAT','WDC',
  'WELL','WFC','WM','WMT','WRB','WTW','WYNN','XOM','YUM','ZBRA',
  'ZION','ZTS']

ETF_SYMBOLS  = [
    'SPY', 'IWM', 'MDY', 'QQQ', 'VTV',
    'VUG', 'RSP', 'DIA','XLF', 'XLK',
    'XLE', 'XLV', 'XLI', 'XLY', 'XLP',
    'XLU', 'XLB', 'XLRE','EWJ', 'EWG',
    'EWZ', 'EWC', 'EWA', 'EWT', 'EWY',
    'EWH', 'EWS', 'EWM', 'TLT', 'IEF',
    'SHY', 'LQD', 'HYG', 'TIP', 'EMB',
    'BNDX', 'GLD', 'SLV', 'USO', 'UNG',
    'DBA', 'DBB', 'UUP', 'FXE', 'FXY',
    'FXB', 'VNQ', 'RWX', 'PFF', 'VIG'
]

# print(len(FYERS_SYMBOLS))

def read_nse_symbols_from_csv(redis_key):
    """
    Fetch symbols from a Redis sorted set or set using the provided key.
    Returns a list of decoded strings.
    """
    # print('redis_key', redis_key)
    key_type = redis_client.type(redis_key).decode('utf-8')
    if key_type == 'zset':
        symbols = redis_client.zrange(redis_key, 0, -1)
        return [s.decode('utf-8') for s in symbols]
    elif key_type == 'set':
        symbols = redis_client.smembers(redis_key)
        return [s.decode('utf-8') for s in symbols]
    elif key_type == 'string':
        symbol = redis_client.get(redis_key)
        return [symbol.decode('utf-8')] if symbol else []
    else:
        # print(f"Key {redis_key} has unsupported type: {key_type}")
        return []

# Example usage:
NSE_SYMBOLS = read_nse_symbols_from_csv("spreads:nse_spreads_name")
# print("NSE_SYMBOLS", NSE_SYMBOLS)


def Crypto_symbols_from_csv(redis_key):
    """
    Fetch symbols from a Redis sorted set or set using the provided key.
    Returns a list of decoded strings.
    """
    # print('redis_key', redis_key)
    key_type = redis_client.type(redis_key).decode('utf-8')
    if key_type == 'zset':
        symbols = redis_client.zrange(redis_key, 0, -1)
        return [s.decode('utf-8') for s in symbols]
    elif key_type == 'set':
        symbols = redis_client.smembers(redis_key)
        return [s.decode('utf-8') for s in symbols]
    elif key_type == 'string':
        symbol = redis_client.get(redis_key)
        return [symbol.decode('utf-8')] if symbol else []
    else:
        # print(f"Key {redis_key} has unsupported type: {key_type}")
        return []

# Example usage:
DB_SYMBOLS = Crypto_symbols_from_csv("spreads:binance_spreads_name")