#!/usr/bin/env python3
"""
Forex data fetcher and database updater.
Retrieves daily forex rates from TwelveData API and stores in DuckDB.
Production-ready daemon with comprehensive error handling and logging.
"""

import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator, List, Tuple, Optional
import sys

import duckdb
import pandas as pd
import twelvedata
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler


@dataclass(frozen=True)
class Config:
    """Application configuration."""
    api_key: str
    data_path: Path
    database_path: Path
    pairs_table: str
    rates_table: str
    api_limit: int = 8
    rate_limit_delay: int = 62
    max_retries: int = 3


@dataclass(frozen=True)
class QueryParams:
    """API query parameters."""
    start_date: str
    end_date: str
    interval: str = "1day"
    timezone: str = "UTC"


class ConfigError(Exception):
    """Configuration related errors."""


class DataError(Exception):
    """Data processing related errors."""


class APIError(Exception):
    """API request related errors."""


class ForexUpdater:
    """Handles forex data retrieval and database updates."""

    def __init__(self, config: Config):
        self.config = config
        self.logger, self.error_logger = self._setup_logging()
        self._client = twelvedata.TDClient(apikey=config.api_key)

    def _setup_logging(self) -> Tuple[logging.Logger, logging.Logger]:
        """Configure production logging with rotation."""
        # Main logger
        logger = logging.getLogger('forex_monitor')
        logger.setLevel(logging.INFO)

        # Error logger
        error_logger = logging.getLogger('forex_error')
        error_logger.setLevel(logging.ERROR)

        # Clear existing handlers
        logger.handlers.clear()
        error_logger.handlers.clear()

        # File handlers with rotation
        log_path = self.config.data_path

        monitor_handler = RotatingFileHandler(
            log_path / 'monitor.log',
            maxBytes=1024*1024,
            backupCount=5,
        )
        monitor_handler.setLevel(logging.INFO)
        monitor_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        logger.addHandler(monitor_handler)

        error_handler = RotatingFileHandler(
            log_path / 'error.log',
            maxBytes=1024*1024,
            backupCount=5,
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        error_logger.addHandler(error_handler)

        # Console handler for immediate feedback
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(
            logging.Formatter('%(levelname)s: %(message)s')
        )
        logger.addHandler(console_handler)

        return logger, error_logger

    @contextmanager
    def _db_connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Database connection context manager with proper error handling."""
        conn = None
        try:
            conn = duckdb.connect(str(self.config.database_path))
            yield conn
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            self.error_logger.error(f"Database connection error: {e}")
            raise DataError(f"Database error: {e}") from e
        finally:
            if conn:
                conn.close()

    def get_active_pairs(self) -> List[List[Tuple[int, str]]]:
        """Retrieve active currency pairs, chunked by API limit."""
        query = f"""
            SELECT id, pair_name 
            FROM {self.config.pairs_table} 
            WHERE active = true
            ORDER BY id
        """

        try:
            with self._db_connection() as conn:
                rows = conn.execute(query).fetchall()
        except Exception as e:
            self.error_logger.error(f"Failed to retrieve active pairs: {e}")
            raise

        if not rows:
            raise DataError("No active currency pairs found")

        self.logger.info(f"Retrieved {len(rows)} active currency pairs")

        # Chunk pairs to respect API limits
        return [rows[i:i + self.config.api_limit]
                for i in range(0, len(rows), self.config.api_limit)]

    def _make_api_request(self, params: QueryParams, pairs: List[str]) -> dict:
        """Make API request with retry logic."""
        for attempt in range(self.config.max_retries):
            try:
                ts = self._client.time_series(
                    symbol=pairs,
                    interval=params.interval,
                    timezone=params.timezone,
                    start_date=params.start_date,
                    end_date=params.end_date,
                )
                return ts.as_json()

            except Exception as e:
                if attempt == self.config.max_retries - 1:
                    self.error_logger.error(f"API request failed after {self.config.max_retries} attempts: {e}")
                    raise APIError(f"API request failed: {e}") from e

                self.logger.warning(f"API request attempt {attempt + 1} failed, retrying in {self.config.rate_limit_delay}s: {e}")
                time.sleep(self.config.rate_limit_delay)

    def _ensure_weekday(self, date_to_check: date) -> date:
        """return the next weekday, or the given date if it is a weekday"""
        if not isinstance(date_to_check, date):
            raise TypeError(f'not a date: {date_to_check}')
        var = 0 if date_to_check.weekday() < 5 else 7 - date_to_check.weekday()
        return date_to_check + timedelta(days=var)

    def _get_last_upate(self) -> date:
        """Retrieve last date db was updated."""
        query = f"""
            SELECT max(date)
            FROM {self.config.rates_table} 
        """

        try:
            with self._db_connection() as conn:
                result = conn.execute(query).fetchone()
        except Exception as e:
            self.error_logger.error(f"Failed to retrieve last date: {e}")
            raise

        if not result:
            raise DataError("No last date found")

        self.logger.info(f"Retrieved {result[0]} as last updated")

        year, month, day = map(int, str(result[0]).split('-'))

        return  date(year, month, day)
    def fetch_rates(self, params: QueryParams, pairs: List[str]) -> List[Tuple[str, str, float]]:
        """Fetch forex rates from API with validation."""
        data = self._make_api_request(params, pairs)

        if not isinstance(data, dict):
            raise APIError("Invalid API response format")

        rates = []
        for pair, entries in data.items():
            if not isinstance(entries, (list, tuple)):
                self.logger.warning(f"Invalid entries format for pair {pair}")
                continue

            for entry in entries:
                try:
                    if not isinstance(entry, dict) or 'datetime' not in entry or 'close' not in entry:
                        self.logger.warning(f"Invalid entry format for {pair}: {entry}")
                        continue

                    rates.append((
                        str(pair),
                        str(entry['datetime']),
                        float(entry['close'])
                    ))
                except (KeyError, ValueError, TypeError) as e:
                    self.logger.warning(f"Invalid data for {pair}: {e}")
                    continue

        return rates

    def process_rates(self,
                     pair_chunks: List[Tuple[int, str]],
                     rates: List[Tuple[str, str, float]]) -> List[Tuple[int, str, float]]:
        """Convert API rates to database format."""
        pair_map = {pair: pair_id for pair_id, pair in pair_chunks}

        processed = [(pair_map[pair], date_str, value)
                    for pair, date_str, value in rates
                    if pair in pair_map]

        if len(processed) != len(rates):
            missing = len(rates) - len(processed)
            self.logger.warning(f"Dropped {missing} rates due to unknown pairs")

        return processed

    def insert_rates(self, rates: List[Tuple[int, str, float]]) -> int:
        """Insert rates into database with duplicate handling."""
        if not rates:
            return 0

        df = pd.DataFrame(rates, columns=['pair_id', 'date', 'value'])

        try:
            with self._db_connection() as conn:
                conn.execute("BEGIN TRANSACTION")
                try:
                    conn.execute(
                        f"INSERT INTO {self.config.rates_table} (pair_id, date, value) "
                        "SELECT * FROM df"
                    )
                    conn.execute("COMMIT")
                    return len(df)
                except Exception as e:
                    conn.execute("ROLLBACK")
                    # Check if it's a duplicate key error (unique constraint)
                    if 'unique' in str(e).lower() or 'constraint' in str(e).lower():
                        self.logger.info(f"Skipped {len(df)} duplicate rates")
                        return 0
                    raise
        except Exception as e:
            self.error_logger.error(f"Failed to insert rates: {e}")
            raise DataError(f"Insert failed: {e}") from e

    def update_daily_rates(self, days_back: int = 2) -> None:
        """Main method to update forex rates."""
        begin_date = self._get_last_upate()
        end_date = date.today() - timedelta(days=1)

        start_date = self._ensure_weekday(begin_date)
        if (start_date.weekday()>= 4):
            self.logger.info(f"last updated {start_date}: wait for Tuesday NZT before next update")
            sys.exit(0)

        params = QueryParams(
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d')
        )

        print(params)
        sys.exit(0)

        self.logger.info(f"Starting forex update for {params.start_date} to {params.end_date}")

        try:
            pair_chunks = self.get_active_pairs()
        except Exception as e:
            self.error_logger.error(f"Failed to retrieve pairs: {e}")
            raise

        all_rates = []
        successful_chunks = 0

        for i, chunk in enumerate(pair_chunks):
            pair_names = [pair for _, pair in chunk]

            try:
                rates = self.fetch_rates(params, pair_names)
                processed_rates = self.process_rates(chunk, rates)
                all_rates.extend(processed_rates)
                successful_chunks += 1

                self.logger.info(f"Processed chunk {i+1}/{len(pair_chunks)}: {len(rates)} rates retrieved")

                # Rate limiting between API calls
                if i < len(pair_chunks) - 1:
                    time.sleep(self.config.rate_limit_delay)

            except (APIError, DataError) as e:
                self.error_logger.error(f"Failed to process chunk {i+1}: {e}")
                continue
            except Exception as e:
                self.error_logger.error(f"Unexpected error processing chunk {i+1}: {e}")
                continue

        if successful_chunks == 0:
            raise DataError("No chunks processed successfully")

        try:
            inserted_count = self.insert_rates(all_rates)
            self.logger.info(
                f"Successfully inserted {inserted_count} rates from {successful_chunks}/{len(pair_chunks)} chunks "
                f"for period {start_date} to {end_date}"
            )
        except Exception as e:
            self.error_logger.error(f"Failed to insert rates: {e}")
            raise


def load_config() -> Config:
    """Load and validate configuration."""
    # Load environment variables
    env_path = Path(__file__).parent / 'config' / '.env'
    if env_path.exists():
        load_dotenv(env_path)

    # Required environment variables mapping
    required_vars = {
        'TWELVEDATA_API_KEY': 'api_key',
        'DATA_PATH': 'data_path',
        'DATABASE': 'database',
        'PAIRS_TABLE': 'pairs_table',
        'RATES_TABLE': 'rates_table'
    }

    config_dict = {}
    missing_vars = []

    for env_var, config_key in required_vars.items():
        value = os.getenv(env_var)
        if not value:
            missing_vars.append(env_var)
        else:
            config_dict[config_key] = value

    if missing_vars:
        raise ConfigError(f"Missing required environment variables: {', '.join(missing_vars)}")

    # Convert and validate paths
    base_path = Path(__file__).parent
    data_path = base_path / config_dict['data_path']
    database_path = data_path / config_dict['database']

    if not data_path.exists():
        raise ConfigError(f"Data directory does not exist: {data_path}")
    if not database_path.exists():
        raise ConfigError(f"Database file does not exist: {database_path}")

    # Optional configuration with defaults
    api_limit = int(os.getenv('API_LIMIT', '8'))
    rate_limit_delay = int(os.getenv('RATE_LIMIT_DELAY', '62'))
    max_retries = int(os.getenv('MAX_RETRIES', '3'))

    return Config(
        api_key=config_dict['api_key'],
        data_path=data_path,
        database_path=database_path,
        pairs_table=config_dict['pairs_table'],
        rates_table=config_dict['rates_table'],
        api_limit=api_limit,
        rate_limit_delay=rate_limit_delay,
        max_retries=max_retries
    )


def main() -> None:
    """Entry point with comprehensive error handling."""
    try:
        config = load_config()
        updater = ForexUpdater(config)
        updater.update_daily_rates()

    except ConfigError as e:
        logging.error(f"Configuration error: {e}")
        sys.exit(1)
    except DataError as e:
        logging.error(f"Data processing error: {e}")
        sys.exit(1)
    except APIError as e:
        logging.error(f"API error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
