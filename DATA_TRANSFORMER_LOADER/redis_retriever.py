import redis
import json,ast
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, MetaData,select
from sqlalchemy.dialects.postgresql import insert  
#=================== CONFIG ===================================
from configparser import ConfigParser
config_object = ConfigParser()
config_object.read("./DATA_TRANSFORMER_LOADER/collectorConfig.ini")
#----------------------- REDIS CONFIG -------------------------
cred = config_object["REDIS"]
host,port,db = cred['host'],cred['port'],cred['db']
symbol_list = cred['symbols']
symbol_list = ast.literal_eval(symbol_list)
r = redis.StrictRedis(host=host, port=port, db=db)
#---------------------- POSTGRES CONFIG -----------------------
db_cred = config_object["DATABASE"]
db_host,port,db_name ,db_password = db_cred['host'],db_cred['port'],db_cred['db_name'],db_cred['password']

db_url=f"postgresql+psycopg2://{db_name}:{db_password}@{db_host}/{db_name}"
engine = create_engine(db_url)  #"postgresql+psycopg2://postgres:password@localhost/postgres")

from sqlalchemy.orm import declarative_base, Session
#================================================= logger
import logging
import json
from datetime import datetime

class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "datetime": datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S'),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        return json.dumps(log_record)

def get_json_logger(name: str, log_file: str = "app_json.log", level=logging.INFO) -> logging.Logger:
    """
    Creates a logger that outputs logs in JSON format with datetime, module, function, and line number.

    Args:
        name (str): Name of the logger.
        log_file (str): Path to the log file.
        level: Logging level (default is logging.INFO).

    Returns:
        logging.Logger: Configured logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        formatter = JSONFormatter()

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger





logger = get_json_logger("MyJSONLogger")



#======================================================================= sec to minute conversion
def sec_to_minutes(df,interval,reset_index = True):
    if 'date'in list(df.columns):
        df['datetime'] = pd.to_datetime(df['date']+'_'+df['timestamp'].apply(lambda x: x.split('.')[0]),format="%Y-%m-%d_%H:%M:%S")
    df.set_index('datetime',inplace=True)
    minute_df = df.resample(interval).agg({
        'open': 'first',
        'high': 'max',
        'low' : 'min',
        'close' : 'last',
        'symbol': 'first'  
    }).dropna()
    if reset_index:
        return minute_df.reset_index()
    else:
        return minute_df
#==================================================================== buy sell strategy with p/l
def simple_buy_sell_with_pnl(df):
  
    df = df.copy()
    df["ma10"] = df["close"].rolling(window=10).mean()
    df["ma20"] = df["close"].rolling(window=20).mean()
    df["signal"] = 0
    df["position"] = None
    df["entry_price"] = None
    df["pnl"] = 0.0

    df.loc[(df["ma10"] > df["ma20"]) & (df["ma10"].shift(1) <= df["ma20"].shift(1)), "signal"] = 1
    df.loc[(df["ma10"] < df["ma20"]) & (df["ma10"].shift(1) >= df["ma20"].shift(1)), "signal"] = -1

    position = None
    entry_price = 0.0

    for i in range(len(df)):
        signal = df.at[i, "signal"]
        price = df.at[i, "close"]

        if signal == 1:
            if position is None:
                position = "long"
                entry_price = price
                df.at[i, "position"] = "buy"
                df.at[i, "entry_price"] = entry_price

        elif signal == -1:
            if position == "long":
                pnl = price - entry_price
                df.at[i, "position"] = "sell"
                df.at[i, "entry_price"] = entry_price
                df.at[i, "pnl"] = pnl
                position = None
                entry_price = 0.0

        else:
            df.at[i, "position"] = position
            df.at[i, "entry_price"] = entry_price

    return df


def generate_signals(df, short_window=5, long_window=5):
    df['SMA_Short'] = df['close'].rolling(window=short_window).mean()
    df['SMA_Long'] = df['close'].rolling(window=long_window).mean()
    df['Signal'] = 0
    df['Signal'][short_window:] = df['SMA_Short'][short_window:] > df['SMA_Long'][short_window:]
    df['Position'] = df['Signal'].diff()

    return df


def trades_to_db(df_):
    df = simple_buy_sell_with_pnl(df_)
    trades = df[df["position"].isin(["buy", "sell"])].reset_index(drop=True)
    merged = []

    for i in range(0, len(trades)-1, 2):
        buy = trades.iloc[i]
        sell = trades.iloc[i+1]
        merged.append({
            "buy_time": buy["datetime"],
            "buy_price": buy["close"],
            "sell_time": sell["datetime"],
            "sell_price": sell["close"],
            "entry_price": buy["entry_price"],
            "pnl": sell["pnl"],
            "symbol":sell["symbol"]
        })

    return pd.DataFrame(merged)


#==================================================================== bulk db insert
def bulk_insert_db(df,table:str):
    # print(df)
    if not df.empty:
        df = df.reset_index()
        metadata = MetaData()
        metadata.reflect(bind=engine)
        minute_table = metadata.tables[table]
        records = df.to_dict(orient="records")

        stmt = insert(minute_table).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=["datetime"])
        with engine.begin() as conn:
            try:
                conn.execute(stmt)
                logger.info(f"Data Inserted to {table}")
            except Exception as e:
                logger.error(f"UNABLE TO INSERT in  {table}")
        

#====================================================================== select

import psycopg2

# Database connection parameters
db_params = {
    "host": db_host,
    "port": "5432",
    "database": db_name ,
    "user": db_name,
    "password": db_password
}



def getDateRangeData(table,start_date, end_date):
    querry = f'''SELECT datetime, "open", high, low, "close", symbol
                    FROM {table} where datetime between '{start_date} 00:00:00.000' and '{end_date} 00:00:00.000'
                    order by datetime;
                    '''
    try:
        # Connect to PostgreSQL database
        with psycopg2.connect(**db_params) as conn:
            return pd.read_sql(querry,con=conn)
        
    except psycopg2.Error as e:
        print("Database error:", e)

    finally:
        if conn:
            conn.close()






# pd.read_sql('',con=conn)















# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy import Column, Integer, Float, DateTime, String

# Base = declarative_base()

# class df_usdinr_minute_data(Base):
#     __tablename__ = 'df_usdinr_minute_data'

#     id = Column(Integer, primary_key=True)
#     datetime = Column(DateTime,unique=True)
#     open = Column(Float)
#     high = Column(Float)
#     low = Column(Float)
#     close = Column(Float)
#     symbol = Column(String)

# class df_nifty_minute_data(Base):
#     __tablename__ = 'df_nifty_minute_data'

#     id = Column(Integer, primary_key=True)
#     datetime = Column(DateTime,unique=True)
#     open = Column(Float)
#     high = Column(Float)
#     low = Column(Float)
#     close = Column(Float)
#     symbol = Column(String)









































