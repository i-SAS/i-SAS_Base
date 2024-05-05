import hashlib
import logging
import os
import pickle
from datetime import datetime
from pathlib import Path
from typing import Any

import _pickle
import influxdb_client
import numpy as np
import pandas as pd
import sqlalchemy
import yaml
from influxdb_client.client.write_api import SYNCHRONOUS

logger = logging.getLogger(__name__)


class DataStorageHandler:
    INFLUXDB_URL: str = 'http://influxdb:8086'
    POSTGRES_HOST: str = 'postgres:5432'
    DATA_STRUCTURE_CFG_PATH: Path = Path(__file__).parent / 'cfg/data_structure.yml'

    def __init__(
            self,
            datadrive: str | Path,
            ) -> None:
        """Initialize DataManager.

        Args:
            datadrive: Path to datadrive, used in 'file' mode.
        """
        self.datadrive = Path(datadrive)
        with self.DATA_STRUCTURE_CFG_PATH.open('r') as f:
            self.data_structure_cfg = yaml.safe_load(f)

    def _load_table_data(
            self,
            table_name: str | Path,
            data_system: str = 'postgres',
            ) -> pd.DataFrame | None:
        """Load table data.

        Args:
            table_name: A Name of table.
            data_system: A data sytem.

        Returns:
            Table data loaded.
        """
        self._check_data_system(data_system, ('postgres', 'file'))
        _load_table_data = {
            'postgres': self._load_postgres_data,
            'file': self._load_table_data_from_file,
            }
        df = _load_table_data[data_system](table_name)
        if df is None:
            return df
        table_name = Path(table_name).stem
        if table_name in self.data_structure_cfg:
            drop_columns = list(set(df.columns) - set(self.data_structure_cfg[table_name].keys()))
            df = df.drop(drop_columns, axis=1)
            df = df.astype(self.data_structure_cfg[table_name]).fillna(pd.NA)
        df = df.set_index(df.columns[0])
        return df

    def _load_postgres_data(
            self,
            table_name: str,
            ) -> pd.DataFrame | None:
        """Load postgres data.

        Args:
            table_name: A Name of table.

        Returns:
            Table data loaded.
        """
        database = f'postgresql://{os.environ["POSTGRES_USER"]}:{os.environ["POSTGRES_PASSWORD"]}' \
                   f'@{self.POSTGRES_HOST}/{os.environ["POSTGRES_DATABASE"]}'
        sql = f'SELECT * FROM \"{table_name}\"'
        logger.debug(f'[DataManager] Load postgres data: {database=}, {sql=}')
        engine = sqlalchemy.create_engine(database, connect_args={'client_encoding': 'utf8'}, echo=False)
        try:
            df = pd.read_sql(sql, engine)
        except sqlalchemy.exc.ProgrammingError:  # the table does not exist
            return
        return df

    def _load_table_data_from_file(
            self,
            path: Path,
            ) -> pd.DataFrame | None:
        """Load table data from file.

        Args:
            path: A path to the table data.

        Returns:
            Table data loaded.
        """
        logger.debug(f'[DataManager] Load table data from file: {path=}')
        if not path.exists():
            logger.info(f'[DataManager] File not found: {path=}')
            return
        return pd.read_csv(str(path), header=0, index_col=None)

    def _save_table_data(
            self,
            table_name: str | Path,
            df: pd.DataFrame | None,
            data_system: str = 'postgres',
            mode: str | None = 'sync',
            ) -> None:
        """Save table data.

        Args:
            table_name: A name of table.
            df: Data to export.
            data_system: A data sytem.
            mode: A mode of export. 'append', 'replace', or 'sync'.
        """
        self._check_data_system(data_system, ('postgres', 'file'))
        if df is None or not len(df):
            return
        index_name = df.index.name
        df = df.reset_index()
        _table_name = Path(table_name).stem
        if _table_name in self.data_structure_cfg:
            drop_columns = list(set(df.columns) - set(self.data_structure_cfg[_table_name].keys()))
            df = df.drop(drop_columns, axis=1)
            data_types = {k: v for k, v in self.data_structure_cfg[_table_name].items() if k in df.columns}
            df = df.astype(data_types).fillna(pd.NA)
        _save_table_data = {
            'postgres': self._save_postgres_data,
            'file': self._save_table_data_to_file,
            }
        _save_table_data[data_system](table_name, df, index_name, mode)

    def _save_postgres_data(
            self,
            table_name: str,
            df: pd.DataFrame,
            index_name: str | None,
            mode: str,
            ) -> None:
        """Save postgres data.

        Args:
            table_name: A name of table.
            df: Data to export.
            index_name: A name of index.
            mode: A mode of saving. 'sync' or 'append'.
        """
        if mode not in ('sync', 'append'):
            ValueError(f'Invalid mode: {mode}')
        database = f'postgresql://{os.environ["POSTGRES_USER"]}:{os.environ["POSTGRES_PASSWORD"]}' \
                   f'@{self.POSTGRES_HOST}/{os.environ["POSTGRES_DATABASE"]}'
        logger.debug(f'[DataManager] Save postgres data: {database=}, {table_name=}, {df=}')
        engine = sqlalchemy.create_engine(database, connect_args={'client_encoding': 'utf8'}, echo=False)
        if mode == 'sync':
            # Create tmp table
            data_hash = hashlib.md5(str(df).encode()).hexdigest()
            table_name_tmp = f'{table_name}_{data_hash}'
            sql_create_tmp = f'CREATE TABLE \"{table_name_tmp}\" AS SELECT * FROM \"{table_name}\" WHERE false;'
            with engine.begin() as connection:
                connection.execute(sqlalchemy.text(sql_create_tmp))
            # Insert data to tmp table
            df.to_sql(table_name_tmp, engine, index=False, if_exists='append')
            # Move data from tmp table to target table
            columns = df.columns.to_numpy().tolist()
            columns_str = ', '.join([f'\"{column}\"' for column in columns])
            update_columns = [column for column in columns if column != index_name]
            if len(update_columns):
                update_columns_str = ', '.join([f'\"{column}\" = EXCLUDED.\"{column}\"' for column in update_columns])
                sql_update = f'INSERT INTO \"{table_name}\" ({columns_str})\
                    SELECT {columns_str} FROM \"{table_name_tmp}\"\
                    ON CONFLICT ({index_name}) DO UPDATE SET {update_columns_str};'
            else:
                sql_update = f'INSERT INTO \"{table_name}\" ({columns_str})\
                    SELECT {columns_str} FROM \"{table_name_tmp}\"\
                    ON CONFLICT ({index_name}) DO NOTHING;'
            with engine.begin() as connection:
                connection.execute(sqlalchemy.text(sql_update))
            # Drop tmp table
            sql_drop = f'DROP TABLE \"{table_name_tmp}\";'
            with engine.begin() as connection:
                connection.execute(sqlalchemy.text(sql_drop))
        else:
            df.to_sql(table_name, engine, index=False, if_exists=mode)

    def _save_table_data_to_file(
            self,
            path: Path,
            df: pd.DataFrame,
            _,
            mode: str,
            ) -> None:
        """Save table data to file.

        Args:
            path: A path to the table.
            df: Data to export.
            mode: A mode of saving.
        """
        if mode not in ('replace', ):
            ValueError(f'Invalid mode: {mode}')
        logger.debug(f'[DataManager] Save table data to file: {path=}, {df=}')
        path.parent.mkdir(exist_ok=True, parents=True)
        df.to_csv(str(path), header=True, index=False, mode='w')

    def _load_time_series_data(
            self,
            data_name: str | Path,
            tag_keys: list[str],
            data_system: str = 'influx',
            first_datetime: datetime | None = None,
            last_datetime: datetime | None = None,
            ) -> pd.DataFrame | None:
        """Load time-series data.

        Args:
            data_name: A name of data.
            tag_keys: A list of key names of tags.
            data_system: A data sytem.
            first_datetime: The data is imported after this datetime. The default, None
            last_datetime: The data is imported before this datetime. The default, None

        Returns:
            Time-series data and tags loaded.
        """
        self._check_data_system(data_system, ('influx', 'file', 'streaming'))
        _load_time_series_data = {
            'influx': self._load_influx_data,
            'file': self._load_time_series_data_from_file,
            'streaming': self._load_time_series_data_streaming,
            }
        df = _load_time_series_data[data_system](
            data_name,
            first_datetime,
            last_datetime,
            )
        if df is None:
            return None
        tag_idx = df.columns.str.match('|'.join(tag_keys), na=False)
        df.loc[:, ~tag_idx].columns = df.loc[:, ~tag_idx].columns.astype(int)
        df.loc[:, ~tag_idx] = df.loc[:, ~tag_idx].astype(float)
        return df

    def _load_influx_data(
            self,
            data_name: str,
            first_datetime: datetime | None,
            last_datetime: datetime | None,
            ) -> pd.DataFrame:
        """Load influx data.

        Args:
            data_name: A name of time-series data.
            first_datetime: The data is imported after this datetime. The default, None
            last_datetime: The data is imported before this datetime. The default, None

        Returns:
            Time-series data and tags loaded.
        """
        start_timestamp = int(datetime.fromtimestamp(0).timestamp()) if first_datetime is None \
            else int(first_datetime.timestamp())
        stop_timestamp = 'now()' if last_datetime is None \
            else int(last_datetime.timestamp())
        client = influxdb_client.InfluxDBClient(
            url=self.INFLUXDB_URL,
            token=os.environ['INFLUXDB_TOKEN'],
            org=os.environ['INFLUXDB_ORG'],
            )
        query_api = client.query_api()
        query = f'from(bucket:"{os.environ["INFLUXDB_BUCKET"]}")\
            |> range(start: {start_timestamp}, stop: {stop_timestamp})\
            |> filter(fn: (r) => r._measurement == "{data_name}")\
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\
            |> drop(columns: ["_start", "_stop", "_measurement"])'
        logger.debug(f'[DataManager] Load influx data: {query=}')
        df = query_api.query_data_frame(query)
        if len(df) != 0:
            df['time'] = self._convert_datetime(df['_time'])
            df = df.set_index('time')
            df = df.drop(['result', 'table', '_time'], axis=1)
        df = df.sort_index(axis=0)
        df = df.sort_index(axis=1)
        return df

    def _load_time_series_data_from_file(
            self,
            path: Path,
            first_datetime: datetime | None,
            last_datetime: datetime | None,
            ) -> pd.DataFrame | None:
        """Import time-series data from file.

        Args:
            path: A path to the time-series data.
            first_datetime: The data is imported after this datetime. The default, None
            last_datetime: The data is imported before this datetime. The default, None

        Returns:
            time-series data and tags loaded.
        """
        logger.debug(f'[DataManager] Load time-series data from file: {path=}')
        if not path.exists():
            logger.info(f'[DataManager] File not found: {path=}')
            return None
        if first_datetime is not None or last_datetime is not None:
            _datetime = self._convert_datetime(pd.read_csv(path, usecols=[0])['time'])
            skiprows = []
            if first_datetime is not None:
                skiprows.extend((np.where(_datetime < first_datetime)[0] + 1).tolist())
            if last_datetime is not None:
                skiprows.extend((np.where(last_datetime <= _datetime)[0] + 1).tolist())
        else:
            skiprows = None
        df = pd.read_csv(str(path), header=0, index_col=0, skiprows=skiprows)
        df.index = self._convert_datetime(df.index)
        return df

    def _load_time_series_data_streaming(
            self,
            path: str | Path,
            *_: Any,
            ) -> pd.DataFrame | None:
        """Load time-series data streaming.

        Args:
            path: A path to the time-series data.

        Returns:
            Time-series data and tags loaded.
        """
        logger.debug(f'[DataManager] Load time-series data streaming: {path=}')
        if not path.exists():
            logger.info(f'[DataManager] File not found: {path=}')
            return None
        while True:
            try:
                with path.open('rb') as f:
                    df = pickle.load(f)
                break
            except EOFError:
                pass
            except _pickle.UnpicklingError:
                pass
        return df

    def _save_time_series_data(
            self,
            data_name: str | Path,
            df: pd.DataFrame,
            tag_keys: list[str],
            data_system: str = 'influx',
            ) -> None:
        """Save time-series data.

        Args:
            data_name: A name of data.
            df: Data to be exported.
            tag_keys: A list of key names of tags.
            data_system: A data sytem.
        """
        self._check_data_system(data_system, ('influx', 'file', 'streaming'))
        if df is None or not len(df):
            return
        _save_time_series_data = {
            'influx': self._save_influx_data,
            'file': self._save_time_series_data_to_file,
            'streaming': self._save_time_series_data_streaming,
            }
        _save_time_series_data[data_system](data_name, df, tag_keys)

    def _save_influx_data(
            self,
            data_name: str,
            df: pd.DataFrame,
            tag_keys: list[str],
            ) -> None:
        """Save influx data.

        Args:
            data_name: A name of data.
            df: Data to be exported.
            tag_keys: A list of key names of tags.
        """
        logger.debug(f'[DataManager] Save influx data: {data_name=}, {df=}')
        client = influxdb_client.InfluxDBClient(
            url=self.INFLUXDB_URL,
            token=os.environ['INFLUXDB_TOKEN'],
            org=os.environ['INFLUXDB_ORG'],
            )
        write_api = client.write_api(write_options=SYNCHRONOUS)
        df.columns = df.columns.astype(str)
        write_api.write(
            os.environ['INFLUXDB_BUCKET'],
            record=df,
            data_frame_measurement_name=data_name,
            data_frame_tag_columns=tag_keys,
            )

    def _save_time_series_data_to_file(
            self,
            path: Path,
            df: pd.DataFrame,
            _,
            ) -> None:
        """Save time-series data to file.

        Args:
            path: A path to the time-series data.
            df: Data to be exported.
        """
        logger.debug(f'[DataManager] Save time-series data to file: {path=}, {df=}')
        path.parent.mkdir(exist_ok=True, parents=True)
        header = not path.exists()
        df.to_csv(str(path), header=header, index=True, mode='a')

    def _save_time_series_data_streaming(
            self,
            path: str | Path,
            df: pd.DataFrame,
            _,
            ) -> None:
        """Save time-series data streaming.

        Args:
            data_name: A name of data.
            df: Data to be exported.
        """
        logger.debug(f'[DataManager] Save time-series data streaming: {path=}, {df=}')
        path.parent.mkdir(parents=True, exist_ok=True)
        df.index = self._convert_datetime(df.index)
        with path.open('wb') as f:
            pickle.dump(df, f)

    @staticmethod
    def _convert_datetime(
            s: pd.Series | pd.core.indexes.range.RangeIndex,
            ) -> pd.Series | pd.core.indexes.range.RangeIndex:
        """Convert pandas series to datatime.

        Args:
            s: A series or index to be converted to datetime

        Returns:
            A series or index converted to datetime
        """
        s = pd.to_datetime(s, unit='ns')
        if hasattr(s, 'tz') and s.tz is not None:
            return s.tz.tz_convert(None)
        elif hasattr(s, 'dt') and s.dt.tz is not None:
            return s.dt.tz_convert(None)
        return s

    def _check_data_system(
            self,
            data_system: str,
            candidates: tuple[str],
            ) -> None:
        """Check if data system is valid.

        Args:
            data_system: A specified name of data system.
            candidates: Candidates of data system.
        """
        if data_system not in candidates:
            raise ValueError(f'data_system {data_system} must be one of the followings: {candidates}')
