# Sample Usage

```
import sqlite3-loader

pairs = {"XBT":"USD","ETH":"USD"}

# initially create tables for all defined pairs and intervals in sqlite3 db and load historical csv data (if exists)
# tables will be created with the naming convention <pair>_<interval_in_minutes> (ex. ETHUSD_60)
# historical csv files have the same naming convention. (ex. ETHUSD_60.csv contains ohlcvt "candlestick" data in 60 minute intervals)
# historical files are assumed to exist in directories labeled by crypto symbol, and contain files across ultiple crypto/base_currency pairs and intervals. 
# Meaning, ETH has its own folder, XBT has its own folder, etc... 
# see actual function definitions in sqlite-loader.py for more details.


batch_initial_load(
                    pairs = pairs,
                    sqlite3_dbpath = r"C:\sqlite_dbs\kraken_ohlcvt.db",
                    historical_file_path =  rf"C:\kraken-historical-ohlcvt"
                )

kraken_public_api_intervals = {21600,10080,1440,240,60,15,5,1}
pairs = {"XBT":"USD","ETH":"USD"}

for pair in pairs.items():

    # incrementally load latest ohlcvt data to sqlite db via kraken public api
    # incremental code block can be run individually as much as needed to keep
    # database somewhat up to date, eventually i'd like to have this set up to 
    # run on a schedule (perhaps with something like Airflow).

    batch_incremental_load(pair = "".join(pair),
                            intervals = kraken_public_api_intervals,
                            sqlite3_dbpath = r"C:\sqlite_dbs\kraken_ohlcvt.db")