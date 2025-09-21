import logging
from logging.handlers import RotatingFileHandler
import json
from datetime import date, timedelta
from typing import Dict, Optional
from dotenv import load_dotenv
import os
import time
from dataclasses import dataclass
import requests
import yaml
import duckdb
import pandas as pd
from contextlib import closing
import http.client
import twelvedata

fxdata_api_key = "12DATA_API_KEY"
data_path="DATA_PATH"
database = "DATABASE"
pairs_table = "TABLE_NAME_PAIRS"
rates_table = "TABLE_NAME_RATES"
env_check = "ENV_LOADED_MARKER"

# max no. of pairs per call on free plan
API_LIMIT = 8

@dataclass
class QueryParam:
    """paramaeters for API query"""
    start_date: str
    end_date: str
    interval: str
    timezone: Optional[str]

def get_env_value(name: str) -> str:
    """get an environmental value"""

    if os.getenv(env_check) != "true":
        dotenv_path = os.path.join(os.path.dirname(__file__), 'config', '.env')
        load_dotenv(dotenv_path)

    value = os.getenv(name)
    if value is None:
        error_logger.error(f'{name} environment variable is not set, exited script')
        raise ValueError(f'{name} environment variable is not set')
    return value

def setup_logging():
    """setup logging"""

    log_path = os.path.join(os.path.dirname(__file__), get_env_value(data_path))

    monitor_logger = logging.getLogger('logger')
    monitor_logger.setLevel(logging.INFO)

    monitor_handler = RotatingFileHandler(
        os.path.join(log_path, 'monitor.log'),
        maxBytes=1024*1024,
        backupCount=5,
    )
    monitor_handler.setLevel(logging.INFO)
    monitor_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    monitor_logger.addHandler(monitor_handler)

    # debugging API calls
    """
    http.client.HTTPConnection.debuglevel = 1
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True
    """

    # Error logger with rotation
    error_logger = logging.getLogger('error_logger')
    error_logger.setLevel(logging.ERROR)

    error_handler = RotatingFileHandler(
        os.path.join(log_path, 'error.log'),
        maxBytes=1024*1024,
        backupCount=5,
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    error_logger.addHandler(error_handler)

    return monitor_logger, error_logger

def get_db_path() -> str:
    """helper to get the path to the database"""

    d_path = get_env_value(data_path)
    db_name = get_env_value(database)

    datafile_path = os.path.join(os.path.dirname(__file__), d_path)
    db_path = os.path.join(datafile_path, db_name)

    return db_path

logger, error_logger = setup_logging()

def retrieve_pairs():

    # d_path = get_env_value(data_path)
    # d_base = get_env_value(database)

    # datafile_path = os.path.join(os.path.dirname(__file__), d_path)
    # db_path = os.path.join(datafile_path, d_base)

    db_path = get_db_path()

    select_query = f"""
        select 
            id, 
            pair_name    
        from {os.getenv(pairs_table)}
        where active is true;
        """
    try:
        with closing(duckdb.connect(db_path)) as conn:
            query_result = conn.execute(select_query)
            rows = query_result.fetchall()
    except ConnectionError as e:
        error_logger.error(f"connection error retrieving pairs: error {e}")
    except Exception as e:
        error_logger.error(f"connection error retrieving pairs: error {e}")
        print(f"connection error retrieving pairs: error {e}")
        exit(1)

    result = []

    for i in range(0, len(rows), API_LIMIT):
        element = rows[i:i + API_LIMIT]
        result.append(element)

    return result

def load_rates(param: QueryParam, pairs: list) -> list:

    api_key = get_env_value(fxdata_api_key)
    td = twelvedata.TDClient(apikey=api_key)

    ts = td.time_series(
        symbol = pairs,
        interval=param.interval,
        timezone=param.timezone,
        start_date=param.start_date,
        end_date=param.end_date,
    )
    # print(param, pairs)

    try:
        data = ts.as_json()
        # logger.info(f"from load_rates, {data}")
    except Exception as e:
        error_logger.error(f"time series error retrieving rates: error {e}")
        print(f"time series error retrieving rates: error {e}")
        exit(1)

    rows = []

    for pair, entries in data.items():
        for entry in entries:
            rows.append((pair, entry['datetime'], float(entry['close'])))
    # print(f"{len(rows)} rates")
    return rows

def parse_rates(pairs: list, rates: list) -> list:

    db_data = []
    map_id = {pair: id for id, pair in pairs}

    for pair, date, value in rates:
        if pair in map_id:
            db_data.append((map_id[pair], date, value))

    return db_data

def insert_rates(rates: list) -> int:
    """add rates to the db"""

    db_path = get_db_path()

    df = pd.DataFrame(rates, columns=['pair_id', 'date', 'value'])
    count = len(df)


    with closing(duckdb.connect(db_path)) as conn:
        conn.execute("begin transaction")
        try:
            conn.execute(f"INSERT INTO {get_env_value(rates_table)} (pair_id, date, value) SELECT * FROM df")
            conn.execute("commit")
            return count
        except ConnectionError as e:
            error_logger.error(f"connection error inserting rates: error {e}")
            conn.execute('rollback')
            raise
        except Exception as e:
            error_logger.error(f"connection error inserting rates: error {e}")
            print(f"connection error inserting rate: error {e}")
            conn.execute('roolback')
            raise

def run():
    """temp for dev"""

    pairs = retrieve_pairs()

    pair_names = []
    for item in pairs:
        tmp = [pr[1] for pr in item]
        pair_names.append(tmp)

    rate_rows = []

    open_date = date.today() - timedelta(days=2)
    close_date = date.today() - timedelta(days=1)

    query_param = QueryParam(
        open_date.strftime('%Y-%m-%d'),
        close_date.strftime('%Y-%m-%d'),
        "1day",
        "UTC"
    )

    for n, p_names in enumerate(pair_names):
        rows = load_rates(query_param, p_names)
        logger.info(f"retrieved {len(rows)} records from API")
        if len(rows) > 0:
            db_data = parse_rates(pairs[n], rows)
            rate_rows += db_data
        if p_names != pair_names[-1]:
            time.sleep(62)

    count_inserts = 0

    if len(rate_rows) > 0:
        count_inserts = insert_rates(rate_rows)

    logger.info(f"inserted {count_inserts} forex daily closes into rates table, from {open_date + timedelta(days=1)} to {close_date}")


run()