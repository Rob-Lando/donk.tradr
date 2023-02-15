# Sample Usage

```
pairs = {"XBT":"USD","ETH":"USD"}

# initially create tables for all defined pairs and intervals in sqlite3 db and load historical csv data (if exists)
# tables will be created with the naming convention <pair>_<interval_in_minutes> (ex. ETHUSD_60)
# historical csv files have the same naming convention. (ex. ETHUSD_60.csv contains ohlcvt "candlestick" data in 60 minute intervals for the ETHUSD trading pair)
# historical files are assumed to exist in directories labeled by crypto symbol, and contain files across ultiple crypto/base_currency pairs and intervals. 
# Meaning, ETH/ has its own folder, XBT/ has its own folder, etc... 
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
    # run on a schedule (perhaps with something like Schtasks on Windows - no need for anything overkill).

    batch_incremental_load(pair = "".join(pair),
                            intervals = kraken_public_api_intervals,
                            sqlite3_dbpath = r"C:\sqlite_dbs\kraken_ohlcvt.db")
```

# Useful Links:

Historical Data Download: https://support.kraken.com/hc/en-us/articles/360047124832-Downloadable-historical-OHLCVT-Open-High-Low-Close-Volume-Trades-data<br><br>
Kraken Public API Documentation: https://docs.kraken.com/rest/#tag/Market-Data/operation/getOHLCData

# Running as a scheduled task on Windows:

  1.) Open your terminal and switch to the directory containing your Python script. Create a .exe file from your Python script by running the following in your terminal:
  
        ```
        pyinstaller <your_script>.py
        ```
      
   This will create a 'dist' directory in the same location as your script, which will contain your newly created executable.
      
  2.) execute this command in windows command prompt as administrator:

    ```
    schtasks /create /tn "<TASK_NAME>" /tr "<PATH_TO_EXECUTABLE>" /sc hourly /mo 1 /ru SYSTEM /f /WakeToRun
    ```
    
   This will schedule your executable to run every hour on the hour, even waking the computer to run if it's connected to ac power and on sleep.
