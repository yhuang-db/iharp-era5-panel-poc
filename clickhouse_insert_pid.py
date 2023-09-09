import pickle

import duckdb
import pandas as pd
import xarray as xr
from clickhouse_driver import Client

file_variable_map = pickle.load(open('data/pickle/file_variable_map.pkl', 'rb'))
df_pid = pd.read_csv('data/vector/pid.csv')


def reset_table(client):
    drop_pop = 'DROP TABLE IF EXISTS pop_pid'
    create_pop = '''
    CREATE TABLE pop_pid
    (
        time DateTime('UTC'),
        pid UInt32,
        latitude Float32,
        longitude Float32,
        u10 Float64,
        v10 Float64,
        d2m Float64,
        t2m Float64,
        msl Float64,
        mwd Float64,
        mwp Float64,
        sst Float64,
        swh Float64,
        sp Float64,
        tp Float64
    )
    ENGINE = MergeTree
    ORDER BY (time, pid)
    '''
    client.execute(drop_pop)
    client.execute(create_pop)


def insert_for_date(client, year, month, day):
    ds_list = []
    variable_list = []
    for file, variable in file_variable_map.items():
        variable_list.append(variable)
        ds = xr.open_dataset(f'data/download/{file}_{year}-{month}-{day}.nc')
        ds_list.append(ds)
    ds_all = xr.merge(ds_list)
    df = ds_all.to_dataframe().reset_index()
    sql = '''
    select t1.pid, t2.*
    from df_pid t1, df t2
    where t1.longitude = t2.longitude
      and t1.latitude = t2.latitude
    order by t2.time, t1.pid
    '''
    pid_df = duckdb.query(sql).to_df()
    pid_df = pid_df[['time', 'pid', 'longitude', 'latitude'] + variable_list]
    client.insert_dataframe(f'INSERT INTO pop_pid VALUES', pid_df)


if __name__ == '__main__':
    client = Client('localhost', password='huan1531', settings={'use_numpy': True})
    reset_table(client)
    start_date = '2022-05-01'
    end_date = '2022-05-07'
    for day in pd.date_range(start=start_date, end=end_date):
        date = day.strftime('%Y-%m-%d')
        year = date[:4]
        month = date[5:7]
        day = date[8:]
        print(f'Inserting {date}')
        insert_for_date(client, year, month, day)
    print(client.query_dataframe('SELECT * FROM pop_pid LIMIT 5'))