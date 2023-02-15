import pandas as pd
import sqlite3
import requests
import os
from datetime import datetime



def get_max_timestamp(db_path,table_name,timestamp_col):

    """
    Function to get the max unix timestamp from an existing table in an sqlite3 database.
    """
    
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    query = f"""SELECT {timestamp_col} from {table_name} ORDER BY {timestamp_col} DESC LIMIT 1"""
    
    c.execute(query)

    max_timestamp = c.fetchone() 

    print(f"\n\nEXISTING MAX TIMESTAMP = {max_timestamp}\n\n")

    conn.close()

    if max_timestamp:

        return int(max_timestamp[0])

    else:

        return 946684800 # 2000-01-01 00:00:00 in unix time

def create_table(sqlite3_dbpath,table_name,field_dict):
    
    conn = sqlite3.connect(sqlite3_dbpath)
    c = conn.cursor()
    
    fields = ",".join([f"\n\t{i} {j}" for i,j in field_dict.items()])

    ddl = f"""CREATE TABLE IF NOT EXISTS {table_name} \n({fields}\n)"""
    
    c.execute(ddl)

    conn.close()

def drop_table(sqlite3_dbpath,table_name):
    
    conn = sqlite3.connect(sqlite3_dbpath)
    c = conn.cursor()

    ddl = f"""DROP TABLE IF EXISTS {table_name}"""
    
    c.execute(ddl)

    conn.close()
    
def append_latest_OHLC_data(pair,interval,sqlite3_dbpath):

    """
    For a given trading pair and interval, query the Kraken public api for OHLCVT data since the max unix timestamp
    from the corresponding table in the associated sqlite3 database. 
    The corresponding table is assumed to be named as follows: <pair>_<interval> 

    API returns 720 data points at most as a json response.
    The response is converted into a Pandas dataframe and appends all records with timestamp > pre existing max timestamp
    in the corresponding table.

    Params:

        pair (str) -                    crypto trading pair pf the form: <symbol><base_currency> (ex. XBTUSD).
        interval (int) -                OHLCVT time interval (in units of minutes).
        sqlite3_dbpath (str) -          path to local sqlite3 database file.
    """

    unix_timestamp = get_max_timestamp(db_path = sqlite3_dbpath,
                                        table_name = f"{pair}_{interval}",
                                        timestamp_col = "TIMESTAMP")

    params = {'pair':pair,'since':unix_timestamp,'interval':interval}

    url = 'https://api.kraken.com/0/public/OHLC'

    resp = requests.get(url, params = params)

    try:
        
        data = resp.json()['result']
        jump = data['last']

        fmt_data = [dict(zip(['TIMESTAMP','OPEN','HIGH','LOW','CLOSE','VWAP','VOLUME','COUNT'],candle)) for candle in data[list(data.keys())[0]]]
        
        print(f"\n\n{pair}_{interval}\n\n{len(fmt_data)}:\n\n {data}\n\n")
        
        del data

        df = pd.DataFrame(fmt_data)

        print(df.loc[df['TIMESTAMP'].astype(int) > unix_timestamp].shape)
        print("\n\n","#"*100)

        del fmt_data

        df.insert(loc = 0,
                column = 'PAIR',
                value = pair,
                allow_duplicates = True)

        df.insert(loc = 1,
                column = 'FORMATTED_TIME',
                value = df.TIMESTAMP.apply(lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S')),
                allow_duplicates = False)

        df.insert(loc = len(df.columns),
                column = 'HIST_OR_API',
                value = 1, # 1 to indicate API data
                allow_duplicates = True)
        
        conn = sqlite3.connect(sqlite3_dbpath)

        df.loc[df['TIMESTAMP'].astype(int) > unix_timestamp].to_sql(f'{pair}_{interval}', conn, if_exists='append', index=False)

        conn.close()

        return df

    except Exception as e:

        print(e)
        print(f"{url}::{params}\n\n{resp.json()}")

        return None

def load_historical_ohlcvt(sqlite3_dbpath,table_name,file_path):
    
    conn = sqlite3.connect(sqlite3_dbpath)
    c = conn.cursor()
    
    chunk_generator = pd.read_csv(f"{file_path}",chunksize = 100_000,header = None)

    file = file_path.split("\\")[-1].split("_")

    print(f"\n\nBEGIN LOADING {file[0]} {file[1][:file[1].find('.')]}\n\n")

    while True:
        try:
            chunk = next(chunk_generator)
            chunk.columns = ['TIMESTAMP','OPEN','HIGH','LOW','CLOSE','VOLUME','COUNT']

            chunk.insert(loc = 0,
                    column = 'PAIR',
                    value = file_path.split("\\")[-1].split("_")[0],
                    allow_duplicates = True)

            chunk.insert(loc = 1,
                    column = 'FORMATTED_TIME',
                    value = chunk['TIMESTAMP'].apply(lambda x: datetime.utcfromtimestamp(float(x)).strftime('%Y-%m-%d %H:%M:%S')),
                    allow_duplicates = False)

            chunk.insert(loc = 7,
                    column = 'VWAP',
                    value = "",
                    allow_duplicates = True)

            chunk.insert(loc = len(chunk.columns),
                    column = 'HIST_OR_API',
                    value = 0, # 0 to indicate historical data
                    allow_duplicates = True)

            chunk.to_sql(table_name, conn, if_exists='append', index=False)

            print(f"\nchunk copied to {sqlite3_dbpath}.{table_name}\n")

            del chunk

        except StopIteration:
            print("\n\nNo more chunks!\n\n")
            break

    conn.close()


def init_load(pair,sqlite3_dbpath,historical_file_path):

    """
    Function for initially creating and populating sqlite3 tables with OHLCVT data across multiple trading pairs/intervals
    from a historical csv file (if exists).
    If there is no historical csv file, then tables are initially created and will be empty.

    Tables in the db are created with the following naming convention by default:
        
        <symbol><base_currency>_<interval>

    Historical files are expected to be organized in directories labeled by crypto symbol (i.e. XBT/ ETH/ LTC/ etc...).

    Historical file naming convention is as follows: 

            <symbol><base_currency>_<interval>.csv

            ex.) XBTUSD_5.csv (5 minute OHLCVT data for the XBTUSD trading pair.

    Intervals for historical files are limited to the following (in units of minutes):                                  {1440,720,60,15,5,1}
    Intervals for pulling data via Kraken's public api are limited to the following (in units of minutes):  {21600,10080,1440,240,60,15,5,1} 

    Params:
        pair (str) -                    crypto trading pair pf the form: <symbol><base_currency> (ex. XBTUSD).
        sqlite3_dbpath (str) -          path to local sqlite3 database file.
        historical_file_path (str) -    path to local directory containing historical csv files related to specific symbol. (ex. C:\historical\ETH )
    """

    kraken_public_api_intervals = {21600,10080,1440,240,60,15,5,1}
    kraken_historical_csv_intervals =         {1440,720,60,15,5,1}

    union = kraken_public_api_intervals.union(kraken_historical_csv_intervals)
    just_api = kraken_public_api_intervals - kraken_historical_csv_intervals
    just_hist = kraken_historical_csv_intervals - kraken_public_api_intervals
    overlap = kraken_public_api_intervals.intersection(kraken_historical_csv_intervals)

    print(f"\n\nAll intervals {union}")
    print(f"Intervals that have historical file & can be extracted via API {overlap}")
    print(f"Intervals that ONLY have historical file {just_hist}")
    print(f"Intervals that HAVE NO historical file but CAN be extracted via API {just_api}")

    for interval in union:

        # Given a trading pair, drop (if exists) and recreate table for each interval.

        drop_table(sqlite3_dbpath = sqlite3_dbpath,
                        table_name = f'{pair}_{interval}')

        create_table(sqlite3_dbpath = sqlite3_dbpath,
                        #db_name = sqlite3_dbpath.split("\\")[-1].replace(".db",""),
                        #schema_name = pair,
                        table_name = f'{pair}_{interval}',
                        field_dict = {
                                        'PAIR':             'text',
                                        'FORMATTED_TIME':   'text',
                                        'TIMESTAMP':        'integer',
                                        'OPEN':             'real',
                                        'HIGH':             'real',
                                        'LOW':              'real',
                                        'CLOSE':            'real',
                                        'VWAP':             'real',
                                        'VOLUME':           'real',
                                        'COUNT':            'integer',
                                        'HIST_OR_API':      'integer'
                                        }
                        )

        if interval in kraken_historical_csv_intervals:
            
            try:

                load_historical_ohlcvt(sqlite3_dbpath = sqlite3_dbpath,
                                        table_name = f"{pair}_{interval}",
                                        file_path = rf"{historical_file_path}\{pair}_{interval}.csv")

            except Exception as e:

                print(e)
                print(f"""\n\n{pair}_{interval} may not have an associated historical .csv file associated with it!!!\n\n
                            Double check {historical_file_path}!\n\n""")
                raise

def batch_initial_load(pairs,sqlite3_dbpath,historical_file_path):

    for pair in pairs.items():

        print(f"\n\nStarting initial load for: {pair}\n\n")

        init_load(
                pair = "".join(pair),
                sqlite3_dbpath = sqlite3_dbpath,
                historical_file_path =  rf"{historical_file_path}\{pair[0]}"
            )

def batch_incremental_load(pair,intervals,sqlite3_dbpath):

    """
    Batch incremental load for a given pair across a set of intervals (in units of minutes)
    """

    for interval in intervals:

        append_latest_OHLC_data(pair = pair,interval = interval ,sqlite3_dbpath = sqlite3_dbpath)


def main():

    kraken_public_api_intervals = {21600,10080,1440,240,60,15,5,1}
    pairs = {"XBT":"USD","ETH":"USD"}

    for pair in pairs.items():

        batch_incremental_load(pair = "".join(pair),
                                intervals = kraken_public_api_intervals,
                                sqlite3_dbpath = r"C:\Users\robla\python-projects\donk.tradr.local.testing\kraken_ohlcvt_test.db")

if __name__ == '__main__':

    if not ("kraken_ohlcvt_ETH_BTC_ONLY_test.db" in os.listdir()):

        batch_initial_load(pairs = {"XBT":"USD","ETH":"USD"},
                            sqlite3_dbpath = r".\kraken_ohlcvt_ETH_BTC_ONLY_test.db",
                            historical_file_path =  r"C:\kraken-historical-ohlcvt")
    main()
